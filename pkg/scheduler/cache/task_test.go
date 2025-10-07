package cache

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/onsi/gomega"

	"github.com/GBA-BI/tes-scheduler/pkg/consts"
	schemodels "github.com/GBA-BI/tes-scheduler/pkg/scheduler/models"
	"github.com/GBA-BI/tes-scheduler/pkg/utils"
	vetesclientfake "github.com/GBA-BI/tes-scheduler/pkg/vetesclient/fake"
	clientmodels "github.com/GBA-BI/tes-scheduler/pkg/vetesclient/models"
)

func TestInitCache(t *testing.T) {
	g := gomega.NewWithT(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	now := time.Now().UTC().Truncate(time.Second)

	fakeVeTESClient := vetesclientfake.NewFakeClient(ctrl)
	fakeVeTESClient.EXPECT().
		ListTasks(gomock.Any(), &clientmodels.ListTasksRequest{
			State:    nonFinishedStates,
			View:     consts.BasicView,
			PageSize: consts.DefaultPageSize,
		}).
		Return(&clientmodels.ListTasksResponse{
			Tasks: []*clientmodels.Task{{
				ID:    "task-xxxx",
				State: consts.TaskQueued,
				Resources: &clientmodels.Resources{
					CPUCores: 1,
					RamGB:    2,
					DiskGB:   10,
					GPU:      &clientmodels.GPUResource{Type: "gpu-01", Count: 1},
				},
				CreationTime: now.Format(time.RFC3339),
				BioosInfo: &clientmodels.BioosInfo{
					AccountID:    "account-01",
					UserID:       "user-01",
					SubmissionID: "submission-01",
					RunID:        "run-01",
				},
				PriorityValue: 100,
				ClusterID:     "",
			}},
			NextPageToken: "",
		}, nil)

	i := &taskCacheImpl{
		vetesClient: fakeVeTESClient,
		data: &data{
			tasks:          make(map[string]*schemodels.TaskInfo),
			clusterIndexer: make(map[string]map[string]struct{}),
		},
	}

	err := i.initCache(context.TODO())
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(i.data.tasks).To(gomega.BeEquivalentTo(map[string]*schemodels.TaskInfo{
		"task-xxxx": {
			ID:           "task-xxxx",
			State:        consts.TaskQueued,
			ClusterID:    "",
			CreationTime: now,
			Resources: &schemodels.Resources{
				CPUCores: 1,
				RamGB:    2,
				DiskGB:   10,
				GPU:      &schemodels.GPUResource{Type: "gpu-01", Count: 1},
			},
			BioosInfo: &schemodels.BioosInfo{
				AccountID:    "account-01",
				UserID:       "user-01",
				SubmissionID: "submission-01",
				RunID:        "run-01",
			},
			PriorityValue: 100,
		},
	}))
	g.Expect(i.data.clusterIndexer).To(gomega.BeEquivalentTo(map[string]map[string]struct{}{
		"": {"task-xxxx": {}},
	}))
}

func TestSyncTasks(t *testing.T) {
	g := gomega.NewWithT(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	now := time.Now().UTC().Truncate(time.Second)

	fakeVeTESClient := vetesclientfake.NewFakeClient(ctrl)
	fakeVeTESClient.EXPECT().
		ListTasks(gomock.Any(), &clientmodels.ListTasksRequest{
			State:    nonFinishedStates,
			View:     consts.MinimalView,
			PageSize: consts.MaximumPageSize,
		}).
		Return(&clientmodels.ListTasksResponse{
			Tasks: []*clientmodels.Task{{
				ID:    "task-no-change",
				State: consts.TaskQueued,
			}, {
				ID:    "task-change",
				State: consts.TaskCanceling,
			}, {
				ID:    "task-new",
				State: consts.TaskQueued,
			}},
			NextPageToken: "",
		}, nil)
	fakeVeTESClient.EXPECT().
		GetTask(gomock.Any(), &clientmodels.GetTaskRequest{
			ID:   "task-new",
			View: consts.BasicView,
		}).
		Return(&clientmodels.GetTaskResponse{Task: &clientmodels.Task{
			ID:    "task-new",
			State: consts.TaskQueued,
			Resources: &clientmodels.Resources{
				CPUCores: 1,
				RamGB:    2,
				DiskGB:   3,
			},
			CreationTime: now.Format(time.RFC3339),
			BioosInfo: &clientmodels.BioosInfo{
				AccountID:    "account-01",
				UserID:       "user-01",
				SubmissionID: "submission-01",
				RunID:        "run-02",
			},
			PriorityValue: 1000,
			ClusterID:     "",
		}}, nil)

	i := &taskCacheImpl{
		vetesClient: fakeVeTESClient,
		data: &data{
			tasks: map[string]*schemodels.TaskInfo{
				"task-no-change": {
					ID:            "task-no-change",
					State:         consts.TaskQueued,
					ClusterID:     "cluster-01",
					CreationTime:  now,
					Resources:     &schemodels.Resources{CPUCores: 1},
					BioosInfo:     &schemodels.BioosInfo{AccountID: "account-01"},
					PriorityValue: 100,
				},
				"task-change": {
					ID:            "task-change",
					State:         consts.TaskRunning,
					ClusterID:     "cluster-01",
					CreationTime:  now,
					Resources:     &schemodels.Resources{RamGB: 2},
					BioosInfo:     &schemodels.BioosInfo{SubmissionID: "submission-01"},
					PriorityValue: -100,
				},
				"task-not-exist": {
					ID:            "task-not-exist",
					State:         consts.TaskRunning,
					ClusterID:     "cluster-02",
					CreationTime:  now,
					Resources:     &schemodels.Resources{DiskGB: 5},
					BioosInfo:     &schemodels.BioosInfo{AccountID: "account-01"},
					PriorityValue: 10,
				},
			},
			clusterIndexer: map[string]map[string]struct{}{
				"cluster-01": {"task-no-change": {}, "task-change": {}},
				"cluster-02": {"task-not-exist": {}},
			},
		},
	}

	err := i.syncTasks(context.Background())
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(i.data.tasks).To(gomega.BeEquivalentTo(map[string]*schemodels.TaskInfo{
		"task-no-change": {
			ID:            "task-no-change",
			State:         consts.TaskQueued,
			ClusterID:     "cluster-01",
			CreationTime:  now,
			Resources:     &schemodels.Resources{CPUCores: 1},
			BioosInfo:     &schemodels.BioosInfo{AccountID: "account-01"},
			PriorityValue: 100,
		},
		"task-change": {
			ID:            "task-change",
			State:         consts.TaskCanceling,
			ClusterID:     "cluster-01",
			CreationTime:  now,
			Resources:     &schemodels.Resources{RamGB: 2},
			BioosInfo:     &schemodels.BioosInfo{SubmissionID: "submission-01"},
			PriorityValue: -100,
		},
		"task-new": {
			ID:           "task-new",
			State:        consts.TaskQueued,
			ClusterID:    "",
			CreationTime: now,
			Resources: &schemodels.Resources{
				CPUCores: 1,
				RamGB:    2,
				DiskGB:   3,
			},
			BioosInfo: &schemodels.BioosInfo{
				AccountID:    "account-01",
				UserID:       "user-01",
				SubmissionID: "submission-01",
				RunID:        "run-02",
			},
			PriorityValue: 1000,
		},
	}))
	g.Expect(i.data.clusterIndexer).To(gomega.BeEquivalentTo(map[string]map[string]struct{}{
		"cluster-01": {"task-no-change": {}, "task-change": {}},
		"":           {"task-new": {}},
	}))
}

func TestListTasks(t *testing.T) {
	g := gomega.NewWithT(t)

	i := &taskCacheImpl{data: &data{
		tasks: map[string]*schemodels.TaskInfo{
			"task-01": {
				ID:        "task-01",
				State:     consts.TaskRunning,
				ClusterID: "cluster-01",
			},
			"task-02": {
				ID:        "task-02",
				State:     consts.TaskRunning,
				ClusterID: "cluster-02",
			},
		},
		clusterIndexer: map[string]map[string]struct{}{
			"cluster-01": {"task-01": {}},
			"cluster-02": {"task-02": {}},
		},
	}}

	resp := i.ListTasks("cluster-02")
	g.Expect(resp).To(gomega.BeEquivalentTo([]*schemodels.TaskInfo{{
		ID:        "task-02",
		State:     consts.TaskRunning,
		ClusterID: "cluster-02",
	}}))
}

func TestListScheduledTasks(t *testing.T) {
	g := gomega.NewWithT(t)

	i := &taskCacheImpl{data: &data{
		tasks: map[string]*schemodels.TaskInfo{
			"task-01": {
				ID:        "task-01",
				State:     consts.TaskRunning,
				ClusterID: "cluster-01",
			},
			"task-02": {
				ID:        "task-02",
				State:     consts.TaskRunning,
				ClusterID: "cluster-02",
			},
			"task-03": {
				ID:        "task-03",
				State:     consts.TaskQueued,
				ClusterID: "",
			},
		},
		clusterIndexer: map[string]map[string]struct{}{
			"cluster-01": {"task-01": {}},
			"cluster-02": {"task-02": {}},
			"":           {"task-03": {}},
		},
	}}

	resp := i.ListScheduledTasks()
	sort.Slice(resp, func(i, j int) bool {
		return resp[i].ID < resp[j].ID
	})
	g.Expect(resp).To(gomega.BeEquivalentTo([]*schemodels.TaskInfo{{
		ID:        "task-01",
		State:     consts.TaskRunning,
		ClusterID: "cluster-01",
	}, {
		ID:        "task-02",
		State:     consts.TaskRunning,
		ClusterID: "cluster-02",
	}}))
}

func TestListTaskClusterIDs(t *testing.T) {
	g := gomega.NewWithT(t)

	i := &taskCacheImpl{data: &data{
		tasks: map[string]*schemodels.TaskInfo{
			"task-01": {
				ID:        "task-01",
				State:     consts.TaskRunning,
				ClusterID: "cluster-01",
			},
			"task-02": {
				ID:        "task-02",
				State:     consts.TaskRunning,
				ClusterID: "cluster-02",
			},
			"task-03": {
				ID:        "task-03",
				State:     consts.TaskQueued,
				ClusterID: "",
			},
		},
		clusterIndexer: map[string]map[string]struct{}{
			"cluster-01": {"task-01": {}},
			"cluster-02": {"task-02": {}},
			"":           {"task-03": {}},
		},
	}}

	resp := i.ListTaskClusterIDs()
	sort.Strings(resp)
	g.Expect(resp).To(gomega.BeEquivalentTo([]string{"cluster-01", "cluster-02"}))
}

func TestUpdateTaskChangeStateAndClusterID(t *testing.T) {
	g := gomega.NewWithT(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fakeVeTESClient := vetesclientfake.NewFakeClient(ctrl)
	fakeVeTESClient.EXPECT().
		UpdateTask(gomock.Any(), &clientmodels.UpdateTaskRequest{
			ID:        "task-0001",
			State:     utils.Point(consts.TaskQueued),
			ClusterID: utils.Point(""),
		}).Return(&clientmodels.UpdateTaskResponse{}, nil)

	i := &taskCacheImpl{
		vetesClient: fakeVeTESClient,
		data: &data{
			tasks: map[string]*schemodels.TaskInfo{
				"task-0001": {
					ID:            "task-0001",
					State:         consts.TaskRunning,
					ClusterID:     "cluster-01",
					PriorityValue: 100,
				},
			},
			clusterIndexer: map[string]map[string]struct{}{
				"cluster-01": {"task-0001": {}},
			},
		},
	}
	err := i.UpdateTask(context.Background(), "task-0001", utils.Point(consts.TaskQueued), utils.Point(""), nil)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(i.data.tasks).To(gomega.BeEquivalentTo(map[string]*schemodels.TaskInfo{
		"task-0001": {
			ID:            "task-0001",
			State:         consts.TaskQueued,
			ClusterID:     "",
			PriorityValue: 100,
		},
	}))
	g.Expect(i.data.clusterIndexer).To(gomega.BeEquivalentTo(map[string]map[string]struct{}{
		"": {"task-0001": {}},
	}))
}

func TestUpdateTaskChangeStateFinishedWithMessage(t *testing.T) {
	g := gomega.NewWithT(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fakeVeTESClient := vetesclientfake.NewFakeClient(ctrl)
	fakeVeTESClient.EXPECT().
		UpdateTask(gomock.Any(), &clientmodels.UpdateTaskRequest{
			ID:    "task-0001",
			State: utils.Point(consts.TaskCanceled),
			Logs: []*clientmodels.TaskLog{{
				ClusterID:  schedulerName,
				SystemLogs: []string{"message"},
			}},
		}).Return(&clientmodels.UpdateTaskResponse{}, nil)

	i := &taskCacheImpl{
		vetesClient: fakeVeTESClient,
		data: &data{
			tasks: map[string]*schemodels.TaskInfo{
				"task-0001": {
					ID:        "task-0001",
					State:     consts.TaskCanceling,
					ClusterID: "",
				},
			},
			clusterIndexer: map[string]map[string]struct{}{
				"": {"task-0001": {}},
			},
		},
	}
	err := i.UpdateTask(context.Background(), "task-0001", utils.Point(consts.TaskCanceled), nil, utils.Point("message"))
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(i.data.tasks).To(gomega.BeEquivalentTo(map[string]*schemodels.TaskInfo{}))
	g.Expect(i.data.clusterIndexer).To(gomega.BeEquivalentTo(map[string]map[string]struct{}{}))
}

func TestUpdateTaskChangeClusterID(t *testing.T) {
	g := gomega.NewWithT(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fakeVeTESClient := vetesclientfake.NewFakeClient(ctrl)
	fakeVeTESClient.EXPECT().
		UpdateTask(gomock.Any(), &clientmodels.UpdateTaskRequest{
			ID:        "task-0001",
			ClusterID: utils.Point("cluster-02"),
		}).Return(&clientmodels.UpdateTaskResponse{}, nil)

	i := taskCacheImpl{
		vetesClient: fakeVeTESClient,
		data: &data{
			tasks: map[string]*schemodels.TaskInfo{
				"task-0001": {
					ID:            "task-0001",
					State:         consts.TaskQueued,
					ClusterID:     "",
					PriorityValue: 10,
				},
			},
			clusterIndexer: map[string]map[string]struct{}{
				"": {"task-0001": {}},
			},
		},
	}
	err := i.UpdateTask(context.Background(), "task-0001", nil, utils.Point("cluster-02"), nil)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(i.data.tasks).To(gomega.BeEquivalentTo(map[string]*schemodels.TaskInfo{
		"task-0001": {
			ID:            "task-0001",
			State:         consts.TaskQueued,
			ClusterID:     "cluster-02",
			PriorityValue: 10,
		},
	}))
	g.Expect(i.data.clusterIndexer).To(gomega.BeEquivalentTo(map[string]map[string]struct{}{
		"cluster-02": {"task-0001": {}},
	}))
}
