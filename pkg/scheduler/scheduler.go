package scheduler

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/GBA-BI/tes-scheduler/pkg/log"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"

	"github.com/GBA-BI/tes-scheduler/pkg/consts"
	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/cache"
	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/controller"
	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/crontab"
	schemodels "github.com/GBA-BI/tes-scheduler/pkg/scheduler/models"
	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/plugin"
	"github.com/GBA-BI/tes-scheduler/pkg/utils"
	"github.com/GBA-BI/tes-scheduler/pkg/vetesclient"
)

// Scheduler ...
type Scheduler struct {
	cache                  *cache.Cache
	plugins                pluginsGroup
	clusterNotReadyTimeout time.Duration
}

type pluginsGroup struct {
	sort          plugin.SortPlugin // only one
	globalFilters []plugin.GlobalFilterPlugin
	filters       []plugin.FilterPlugin
	scores        []plugin.ScorePlugin
}

// NewScheduler ...
func NewScheduler(opts *Options, vetesClient vetesclient.Client) (*Scheduler, error) {
	cache, err := cache.NewCache(vetesClient, opts.Cache)
	if err != nil {
		return nil, err
	}

	scheduler := &Scheduler{
		cache:                  cache,
		clusterNotReadyTimeout: opts.ClusterNotReadyTimeout,
	}
	plugins, err := initPluginsGroup(opts, cache)
	if err != nil {
		return nil, err
	}
	scheduler.plugins = plugins

	if err = controller.Init(opts.Controller, cache); err != nil {
		return nil, err
	}

	if err = crontab.RegisterCron(opts.SchedulePeriod, scheduler.scheduleTasks); err != nil {
		return nil, err
	}

	return scheduler, nil
}

func initPluginsGroup(opts *Options, cache *cache.Cache) (pluginsGroup, error) {
	plugins := pluginsGroup{}
	for _, pluginName := range opts.Plugins {
		factory, ok := registry[pluginName]
		if !ok {
			return pluginsGroup{}, fmt.Errorf("invalid plugin name: %s", pluginName)
		}
		p, err := factory(extractPluginConfig(opts, pluginName), cache)
		if err != nil {
			return pluginsGroup{}, fmt.Errorf("failed to init plugin %s: %w", pluginName, err)
		}
		if sort, ok := p.(plugin.SortPlugin); ok {
			plugins.sort = sort // use last one
		}
		if globalFilter, ok := p.(plugin.GlobalFilterPlugin); ok {
			plugins.globalFilters = append(plugins.globalFilters, globalFilter)
		}
		if filter, ok := p.(plugin.FilterPlugin); ok {
			plugins.filters = append(plugins.filters, filter)
		}
		if score, ok := p.(plugin.ScorePlugin); ok {
			plugins.scores = append(plugins.scores, score)
		}
	}
	return plugins, nil
}

// Run ...
func (s *Scheduler) Run(ctx context.Context) {
	crontab.Start()
	defer func() {
		stopCtx := crontab.Stop()
		<-stopCtx.Done() // wait for all cronjob finish
	}()

	<-ctx.Done()
}

func (s *Scheduler) scheduleTasks() {
	tasks := s.cache.TaskCache.ListTasks("")
	toScheduleTasks := make([]*schemodels.TaskInfo, 0, len(tasks))
	for _, task := range tasks {
		if task.State == consts.TaskCanceling {
			s.cancelUnscheduledTask(task)
			continue
		}
		if task.State != consts.TaskQueued {
			continue
		}
		toScheduleTasks = append(toScheduleTasks, task)
	}
	if len(toScheduleTasks) == 0 {
		return
	}

	clusters := s.cache.ClusterCache.ListClusters()
	readyClusters := make([]*schemodels.ClusterInfo, 0, len(clusters))
	for _, cluster := range clusters {
		if time.Since(cluster.HeartbeatTimestamp) <= s.clusterNotReadyTimeout {
			readyClusters = append(readyClusters, cluster)
		}
	}
	if len(readyClusters) == 0 {
		return
	}

	sort.Slice(toScheduleTasks, func(i, j int) bool {
		return s.plugins.sort.Less(toScheduleTasks[i], toScheduleTasks[j])
	})
	for _, task := range toScheduleTasks {
		s.scheduleTask(task, readyClusters)
	}
}

func (s *Scheduler) cancelUnscheduledTask(task *schemodels.TaskInfo) {
	ctx := context.Background()
	if err := s.cache.TaskCache.UpdateTask(ctx, task.ID, utils.Point(consts.TaskCanceled), nil, nil); err != nil {
		log.CtxErrorw(ctx, "failed to cancel unscheduled task", "task", task.ID, "err", err)
	}
	log.CtxInfow(ctx, "directly cancel unscheduled task", "task", task.ID)
}

func (s *Scheduler) scheduleTask(task *schemodels.TaskInfo, clusters []*schemodels.ClusterInfo) {
	ctx := context.Background()
	cycleState := make(map[string]interface{})

	for _, globalFilter := range s.plugins.globalFilters {
		if err := globalFilter.GlobalFilter(ctx, task, cycleState); err != nil {
			s.recordUnscheduledReason(ctx, task.ID, map[string][]error{globalFilter.Name(): {err}})
			return
		}
	}

	availableClusters, pluginNameWithErrors := s.filterAvailableClusters(task, clusters, ctx, cycleState)
	if len(availableClusters) == 0 {
		s.recordUnscheduledReason(ctx, task.ID, pluginNameWithErrors)
		return
	}

	clusterWithScores := s.getClusterWithScores(task, availableClusters, ctx, cycleState)
	scheduleClusterID := s.getMaxScoreClusterID(clusterWithScores)

	if err := s.cache.TaskCache.UpdateTask(ctx, task.ID, nil, utils.Point(scheduleClusterID), nil); err != nil {
		s.recordUnscheduledReason(ctx, task.ID, map[string][]error{"finalUpdate": {err}})
	}
	s.recordScheduleResult(ctx, task.ID, scheduleClusterID)
}

func (s *Scheduler) filterAvailableClusters(task *schemodels.TaskInfo, clusters []*schemodels.ClusterInfo, ctx context.Context, cycleState map[string]interface{}) ([]*schemodels.ClusterInfo, map[string][]error) {
	var availableClusters []*schemodels.ClusterInfo
	pluginNameWithErrors := make(map[string][]error)
	for _, cluster := range clusters {
		clusterAvailable := true
		for _, filter := range s.plugins.filters {
			if err := filter.Filter(ctx, task, cluster, cycleState); err != nil {
				pluginNameWithErrors[filter.Name()] = append(pluginNameWithErrors[filter.Name()], fmt.Errorf("cluster[%s]: %w", cluster.ID, err))
				clusterAvailable = false
				break
			}
		}
		if clusterAvailable {
			availableClusters = append(availableClusters, cluster)
		}
	}
	return availableClusters, pluginNameWithErrors
}

func (s *Scheduler) getClusterWithScores(task *schemodels.TaskInfo, availableClusters []*schemodels.ClusterInfo, ctx context.Context, cycleState map[string]interface{}) []clusterWithScore {
	clusterWithScores := make([]clusterWithScore, 0, len(availableClusters))
	for _, cluster := range availableClusters {
		var valueSum int64 = 0
		for _, score := range s.plugins.scores {
			scoreValue := score.Score(ctx, task, cluster, cycleState)
			if scoreValue < plugin.MinScore {
				scoreValue = plugin.MinScore
			}
			if scoreValue > plugin.MaxScore {
				scoreValue = plugin.MaxScore
			}
			valueSum += scoreValue
		}
		if len(s.plugins.scores) == 0 {
			clusterWithScores = append(clusterWithScores, clusterWithScore{
				clusterID: cluster.ID,
				score:     plugin.MaxScore,
			})
		} else {
			clusterWithScores = append(clusterWithScores, clusterWithScore{
				clusterID: cluster.ID,
				score:     valueSum / int64(len(s.plugins.scores)),
			})
		}
	}
	return clusterWithScores
}

func (s *Scheduler) getMaxScoreClusterID(clusterWithScores []clusterWithScore) string {
	var maxItems []clusterWithScore // clusterID with same score
	for _, item := range clusterWithScores {
		if len(maxItems) == 0 {
			maxItems = []clusterWithScore{item}
		}
		if item.score > maxItems[0].score {
			maxItems = []clusterWithScore{item}
		}
		if item.score == maxItems[0].score {
			maxItems = append(maxItems, item)
		}
	}
	maxItem := maxItems[rand.Intn(len(maxItems))]
	return maxItem.clusterID
}

type clusterWithScore struct {
	clusterID string
	score     int64
}

func (s *Scheduler) recordUnscheduledReason(ctx context.Context, taskID string, pluginNameWithErrors map[string][]error) {
	keysAndValues := make([]interface{}, 0, len(pluginNameWithErrors)*2)
	for name, errs := range pluginNameWithErrors {
		keysAndValues = append(keysAndValues, name)
		keysAndValues = append(keysAndValues, utilerrors.NewAggregate(errs).Error())
	}
	keysAndValues = append(keysAndValues, "task", taskID)
	log.CtxInfow(ctx, "failed to schedule task", keysAndValues...)
}

func (s *Scheduler) recordScheduleResult(ctx context.Context, taskID, clusterID string) {
	log.CtxInfow(ctx, "successfully schedule task", "task", taskID, "cluster", clusterID)
}
