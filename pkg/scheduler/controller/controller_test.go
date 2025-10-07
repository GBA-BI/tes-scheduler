package controller

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/onsi/gomega"

	"github.com/GBA-BI/tes-scheduler/pkg/consts"
	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/cache"
	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/cache/fake"
	schemodels "github.com/GBA-BI/tes-scheduler/pkg/scheduler/models"
	"github.com/GBA-BI/tes-scheduler/pkg/utils"
)

func TestRescheduleTask(t *testing.T) {
	g := gomega.NewWithT(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	now := time.Now()

	fakeClusterCache := fake.NewFakeClusterCache(ctrl)
	fakeClusterCache.EXPECT().ListClusters().
		Return([]*schemodels.ClusterInfo{
			{ID: "cluster-normal", HeartbeatTimestamp: now},
			{ID: "cluster-too-old", HeartbeatTimestamp: now.Add(-time.Hour)},
		})
	fakeTaskCache := fake.NewFakeTaskCache(ctrl)
	fakeTaskCache.EXPECT().ListTaskClusterIDs().
		Return([]string{"cluster-normal", "cluster-too-old", "cluster-deleted"})
	fakeTaskCache.EXPECT().ListTasks("cluster-too-old").
		Return([]*schemodels.TaskInfo{
			{ID: "task-01", ClusterID: "cluster-too-old", State: consts.TaskRunning},
			{ID: "task-02", ClusterID: "cluster-too-old", State: consts.TaskCanceling},
		})
	fakeTaskCache.EXPECT().ListTasks("cluster-deleted").
		Return([]*schemodels.TaskInfo{
			{ID: "task-03", ClusterID: "cluster-deleted", State: consts.TaskQueued},
			{ID: "task-04", ClusterID: "cluster-deleted", State: consts.TaskCanceling},
		})
	fakeTaskCache.EXPECT().
		UpdateTask(gomock.Any(), "task-01", utils.Point(consts.TaskQueued), utils.Point(""), nil).
		Return(nil)
	fakeTaskCache.EXPECT().
		UpdateTask(gomock.Any(), "task-02", utils.Point(consts.TaskCanceled), nil, nil).
		Return(nil)
	fakeTaskCache.EXPECT().
		UpdateTask(gomock.Any(), "task-03", utils.Point(consts.TaskQueued), utils.Point(""), nil).
		Return(nil)
	fakeTaskCache.EXPECT().
		UpdateTask(gomock.Any(), "task-04", utils.Point(consts.TaskCanceled), nil, nil).
		Return(nil)

	c := &Controller{
		cache: &cache.Cache{
			TaskCache:    fakeTaskCache,
			ClusterCache: fakeClusterCache,
		},
		clusterRescheduleTimeout: time.Minute * 20,
	}
	err := c.rescheduleTasks(context.Background())
	g.Expect(err).NotTo(gomega.HaveOccurred())
}

func TestMarkTasksFailedNotMeetLimits(t *testing.T) {
	g := gomega.NewWithT(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tests := []struct {
		name     string
		clusters []*schemodels.ClusterInfo
		task     *schemodels.TaskInfo
		mark     bool
	}{
		{
			name: "mark",
			clusters: []*schemodels.ClusterInfo{
				{Limits: &schemodels.Limits{CPUCores: utils.Point(1)}},
				{Limits: &schemodels.Limits{RamGB: utils.Point[float64](2)}},
			},
			task: &schemodels.TaskInfo{
				ID:        "task-0000",
				State:     consts.TaskQueued,
				ClusterID: "",
				Resources: &schemodels.Resources{CPUCores: 2, RamGB: 4},
			},
			mark: true,
		},
		{
			name:     "no mark: no cluster",
			clusters: []*schemodels.ClusterInfo{},
			task: &schemodels.TaskInfo{
				ID:        "task-0000",
				State:     consts.TaskQueued,
				ClusterID: "",
				Resources: &schemodels.Resources{CPUCores: 2, RamGB: 4},
			},
			mark: false,
		},
		{
			name: "no mark: meet any cluster limit",
			clusters: []*schemodels.ClusterInfo{
				{Limits: &schemodels.Limits{CPUCores: utils.Point(3)}},
				{Limits: &schemodels.Limits{RamGB: utils.Point[float64](2)}},
			},
			task: &schemodels.TaskInfo{
				ID:        "task-0000",
				State:     consts.TaskQueued,
				ClusterID: "",
				Resources: &schemodels.Resources{CPUCores: 2, RamGB: 4},
			},
			mark: false,
		},
		{
			name: "no mark: task not queued",
			clusters: []*schemodels.ClusterInfo{
				{Limits: &schemodels.Limits{CPUCores: utils.Point(1)}},
				{Limits: &schemodels.Limits{RamGB: utils.Point[float64](2)}},
			},
			task: &schemodels.TaskInfo{
				ID:        "task-0000",
				State:     consts.TaskCanceling,
				ClusterID: "",
				Resources: &schemodels.Resources{CPUCores: 2, RamGB: 4},
			},
			mark: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fakeClusterCache := fake.NewFakeClusterCache(ctrl)
			fakeClusterCache.EXPECT().ListClusters().Return(test.clusters)
			fakeTaskCache := fake.NewFakeTaskCache(ctrl)
			fakeTaskCache.EXPECT().ListTasks("").Return([]*schemodels.TaskInfo{test.task})
			if test.mark {
				fakeTaskCache.EXPECT().UpdateTask(gomock.Any(), test.task.ID, utils.Point(consts.TaskSystemError), nil, gomock.Any())
			}
			c := &Controller{cache: &cache.Cache{
				ClusterCache: fakeClusterCache,
				TaskCache:    fakeTaskCache,
			}}
			err := c.markTasksFailedNotMeetLimits(context.Background())
			g.Expect(err).NotTo(gomega.HaveOccurred())
		})
	}
}
