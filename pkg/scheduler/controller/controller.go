package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/GBA-BI/tes-scheduler/pkg/log"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"

	"github.com/GBA-BI/tes-scheduler/pkg/consts"
	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/cache"
	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/crontab"
	schemodels "github.com/GBA-BI/tes-scheduler/pkg/scheduler/models"
	"github.com/GBA-BI/tes-scheduler/pkg/utils"
)

// Controller is some extra logic controlling task and cluster
type Controller struct {
	cache *cache.Cache

	clusterRescheduleTimeout time.Duration
}

// Init ...
func Init(opts *Options, cache *cache.Cache) error {
	c := &Controller{
		cache:                    cache,
		clusterRescheduleTimeout: opts.ClusterRescheduleTimeout,
	}
	if err := crontab.RegisterCron(opts.Period, func() {
		ctx := context.Background()
		if err := c.rescheduleTasks(ctx); err != nil {
			log.CtxErrorw(ctx, "reschedule cluster failed", "err", err)
		}
	}); err != nil {
		return err
	}
	if err := crontab.RegisterCron(opts.Period, func() {
		ctx := context.Background()
		if err := c.markTasksFailedNotMeetLimits(ctx); err != nil {
			log.CtxErrorw(ctx, "mark tasks failed not meet limits failed", "err", err)
		}
	}); err != nil {
		return err
	}
	return nil
}

// rescheduleTasks reschedule task of deleted cluster or long-not-lived cluster
func (c *Controller) rescheduleTasks(ctx context.Context) error {
	clusters := c.cache.ClusterCache.ListClusters()
	taskClusterIDs := c.cache.TaskCache.ListTaskClusterIDs()

	existClustersMap := make(map[string]struct{}, len(clusters))
	shouldRescheduleClusters := make([]string, 0)
	for _, cluster := range clusters {
		existClustersMap[cluster.ID] = struct{}{}
		if time.Since(cluster.HeartbeatTimestamp) > c.clusterRescheduleTimeout {
			shouldRescheduleClusters = append(shouldRescheduleClusters, cluster.ID)
		}
	}
	for _, clusterID := range taskClusterIDs {
		if _, ok := existClustersMap[clusterID]; !ok {
			shouldRescheduleClusters = append(shouldRescheduleClusters, clusterID)
		}
	}

	var errs []error
	for _, clusterID := range shouldRescheduleClusters {
		tasks := c.cache.TaskCache.ListTasks(clusterID)
		for _, task := range tasks {
			if err := c.rescheduleTask(ctx, task); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return utilerrors.NewAggregate(errs)
}

func (c *Controller) rescheduleTask(ctx context.Context, task *schemodels.TaskInfo) error {
	if task.State == consts.TaskCanceling {
		if err := c.cache.TaskCache.UpdateTask(ctx, task.ID, utils.Point(consts.TaskCanceled), nil, nil); err != nil {
			return err
		}
		log.CtxInfow(ctx, "directly cancel task need to be rescheduled", "task", task.ID)
		return nil
	}
	if err := c.cache.TaskCache.UpdateTask(ctx, task.ID, utils.Point(consts.TaskQueued), utils.Point(""), nil); err != nil {
		return err
	}
	log.CtxInfow(ctx, "reschedule task", "task", task.ID)
	return nil
}

// markTasksFailedNotMeetLimits marks tasks that do not meet cluster limits as failed
func (c *Controller) markTasksFailedNotMeetLimits(ctx context.Context) error {
	tasks := c.cache.TaskCache.ListTasks("")
	clusters := c.cache.ClusterCache.ListClusters()

	// special treatment for no cluster, not mark failed
	if len(clusters) == 0 {
		return nil
	}

	var errs []error
	for _, task := range tasks {
		if task.State != consts.TaskQueued {
			continue
		}
		msg := taskMeetLimits(task, clusters)
		if msg == "" {
			continue
		}
		if err := c.cache.TaskCache.UpdateTask(ctx, task.ID, utils.Point(consts.TaskSystemError), nil, utils.Point(fmt.Sprintf("no cluster limits match task resources: %s", msg))); err != nil {
			errs = append(errs, err)
		}
	}
	return utilerrors.NewAggregate(errs)
}

func taskMeetLimits(task *schemodels.TaskInfo, clusters []*schemodels.ClusterInfo) string {
	var errs []error
	for _, cluster := range clusters {
		if err := utils.ResourcesMeetLimits(task.Resources, cluster.Limits); err == nil {
			return ""
		} else {
			errs = append(errs, fmt.Errorf("cluster[%s]: %w", cluster.ID, err))
		}
	}
	return utilerrors.NewAggregate(errs).Error()
}
