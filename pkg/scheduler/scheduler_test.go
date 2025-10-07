package scheduler

import (
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/onsi/gomega"

	"github.com/GBA-BI/tes-scheduler/pkg/consts"
	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/cache"
	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/cache/fake"
	schemodels "github.com/GBA-BI/tes-scheduler/pkg/scheduler/models"
	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/plugin"
	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/plugin/clustercapacity"
	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/plugin/clusterlimit"
	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/plugin/prioritysort"
	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/plugin/resourcequota"
	"github.com/GBA-BI/tes-scheduler/pkg/utils"
)

func TestScheduleTasks(t *testing.T) {
	g := gomega.NewWithT(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	now := time.Now()

	fakeClusterCache := fake.NewFakeClusterCache(ctrl)
	fakeClusterCache.EXPECT().ListClusters().Return([]*schemodels.ClusterInfo{
		{ID: "cluster-ready", HeartbeatTimestamp: now},
		{ID: "cluster-not-ready", HeartbeatTimestamp: now.Add(-time.Hour)},
	})
	fakeTaskCache := fake.NewFakeTaskCache(ctrl)
	fakeTaskCache.EXPECT().ListTasks("").Return([]*schemodels.TaskInfo{
		{ID: "task-01", State: consts.TaskQueued},
		{ID: "task-02", State: consts.TaskQueued},
		{ID: "task-canceling", State: consts.TaskCanceling},
	})
	fakeTaskCache.EXPECT().UpdateTask(gomock.Any(), "task-canceling", utils.Point(consts.TaskCanceled), nil, nil).Return(nil)
	task2Call := fakeTaskCache.EXPECT().UpdateTask(gomock.Any(), "task-02", nil, utils.Point("cluster-ready"), nil).Return(nil)
	fakeTaskCache.EXPECT().UpdateTask(gomock.Any(), "task-01", nil, utils.Point("cluster-ready"), nil).Return(nil).After(task2Call)

	fakeSort := plugin.NewFakeSortPlugin(ctrl)
	// task-01 < task-02
	fakeSort.EXPECT().Less(
		&schemodels.TaskInfo{ID: "task-01", State: consts.TaskQueued},
		&schemodels.TaskInfo{ID: "task-02", State: consts.TaskQueued}).
		Return(false).AnyTimes()
	fakeSort.EXPECT().Less(
		&schemodels.TaskInfo{ID: "task-02", State: consts.TaskQueued},
		&schemodels.TaskInfo{ID: "task-01", State: consts.TaskQueued}).
		Return(true).AnyTimes()

	s := &Scheduler{
		cache: &cache.Cache{
			ClusterCache: fakeClusterCache,
			TaskCache:    fakeTaskCache,
		},
		plugins: pluginsGroup{
			sort: fakeSort,
		},
		clusterNotReadyTimeout: time.Minute * 5,
	}
	g.Expect(func() { s.scheduleTasks() }).NotTo(gomega.Panic())
}

func TestSchedulerTask(t *testing.T) {
	g := gomega.NewWithT(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fakeGlobalFilterPass := plugin.NewFakeGlobalFilterPlugin(ctrl)
	fakeGlobalFilterPass.EXPECT().Name().Return("fakeGlobalFilterPass").AnyTimes()
	fakeGlobalFilterPass.EXPECT().GlobalFilter(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	fakeGlobalFilterFail := plugin.NewFakeGlobalFilterPlugin(ctrl)
	fakeGlobalFilterFail.EXPECT().Name().Return("fakeGlobalFilterFail").AnyTimes()
	fakeGlobalFilterFail.EXPECT().GlobalFilter(gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("xxx")).AnyTimes()

	fakeFilter := plugin.NewFakeFilterPlugin(ctrl)
	fakeFilter.EXPECT().Name().Return("fakeFilter").AnyTimes()
	// only pass cluster-01/cluster-02/cluster-03
	fakeFilter.EXPECT().Filter(gomock.Any(), gomock.Any(), &schemodels.ClusterInfo{ID: "cluster-01"}, gomock.Any()).Return(nil).AnyTimes()
	fakeFilter.EXPECT().Filter(gomock.Any(), gomock.Any(), &schemodels.ClusterInfo{ID: "cluster-02"}, gomock.Any()).Return(nil).AnyTimes()
	fakeFilter.EXPECT().Filter(gomock.Any(), gomock.Any(), &schemodels.ClusterInfo{ID: "cluster-03"}, gomock.Any()).Return(nil).AnyTimes()
	fakeFilter.EXPECT().Filter(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("xxx")).AnyTimes()
	fakeFilterFail := plugin.NewFakeFilterPlugin(ctrl)
	fakeFilterFail.EXPECT().Name().Return("fakeFilterFail").AnyTimes()
	fakeFilterFail.EXPECT().Filter(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("xxx")).AnyTimes()

	fakeScore := plugin.NewFakeScorePlugin(ctrl)
	fakeScore.EXPECT().Name().Return("fakeScore").AnyTimes()
	// cluster-04 > cluster-01 = cluster-02 > cluster-03
	fakeScore.EXPECT().Score(gomock.Any(), gomock.Any(), &schemodels.ClusterInfo{ID: "cluster-01"}, gomock.Any()).Return(int64(10)).AnyTimes()
	fakeScore.EXPECT().Score(gomock.Any(), gomock.Any(), &schemodels.ClusterInfo{ID: "cluster-02"}, gomock.Any()).Return(int64(10)).AnyTimes()
	fakeScore.EXPECT().Score(gomock.Any(), gomock.Any(), &schemodels.ClusterInfo{ID: "cluster-03"}, gomock.Any()).Return(int64(8)).AnyTimes()
	fakeScore.EXPECT().Score(gomock.Any(), gomock.Any(), &schemodels.ClusterInfo{ID: "cluster-04"}, gomock.Any()).Return(int64(20)).AnyTimes()
	fakeScoreAnother := plugin.NewFakeScorePlugin(ctrl)
	fakeScoreAnother.EXPECT().Name().Return("fakeScoreAnother").AnyTimes()
	// cluster-02 < cluster-03
	fakeScoreAnother.EXPECT().Score(gomock.Any(), gomock.Any(), &schemodels.ClusterInfo{ID: "cluster-02"}, gomock.Any()).Return(int64(8)).AnyTimes()
	fakeScoreAnother.EXPECT().Score(gomock.Any(), gomock.Any(), &schemodels.ClusterInfo{ID: "cluster-03"}, gomock.Any()).Return(int64(10)).AnyTimes()

	tests := []struct {
		name          string
		task          *schemodels.TaskInfo
		clusters      []*schemodels.ClusterInfo
		globalFilters []plugin.GlobalFilterPlugin
		filters       []plugin.FilterPlugin
		scores        []plugin.ScorePlugin
		expClusterIDs []string
	}{
		{
			name:          "global filter fail",
			task:          &schemodels.TaskInfo{ID: "task-0000"},
			clusters:      []*schemodels.ClusterInfo{{ID: "cluster-01"}, {ID: "cluster-02"}},
			globalFilters: []plugin.GlobalFilterPlugin{fakeGlobalFilterPass, fakeGlobalFilterFail},
		},
		{
			name:          "filter fail",
			task:          &schemodels.TaskInfo{ID: "task-0000"},
			clusters:      []*schemodels.ClusterInfo{{ID: "cluster-01"}, {ID: "cluster-02"}},
			globalFilters: []plugin.GlobalFilterPlugin{fakeGlobalFilterPass},
			filters:       []plugin.FilterPlugin{fakeFilter, fakeFilterFail},
		},
		{
			name:          "filter pass, no score, random",
			task:          &schemodels.TaskInfo{ID: "task-0000"},
			clusters:      []*schemodels.ClusterInfo{{ID: "cluster-01"}, {ID: "cluster-02"}, {ID: "cluster-03"}, {ID: "cluster-04"}},
			globalFilters: []plugin.GlobalFilterPlugin{fakeGlobalFilterPass},
			filters:       []plugin.FilterPlugin{fakeFilter},
			expClusterIDs: []string{"cluster-01", "cluster-02", "cluster-03"},
		},
		{
			name:          "filter pass, score only one",
			task:          &schemodels.TaskInfo{ID: "task-0000"},
			clusters:      []*schemodels.ClusterInfo{{ID: "cluster-01"}, {ID: "cluster-03"}, {ID: "cluster-04"}},
			filters:       []plugin.FilterPlugin{fakeFilter},
			scores:        []plugin.ScorePlugin{fakeScore},
			expClusterIDs: []string{"cluster-01"},
		},
		{
			name:          "filter pass, score random",
			task:          &schemodels.TaskInfo{ID: "task-0000"},
			clusters:      []*schemodels.ClusterInfo{{ID: "cluster-01"}, {ID: "cluster-02"}, {ID: "cluster-03"}, {ID: "cluster-04"}},
			filters:       []plugin.FilterPlugin{fakeFilter},
			scores:        []plugin.ScorePlugin{fakeScore},
			expClusterIDs: []string{"cluster-01", "cluster-02"},
		},
		{
			name:          "no filter, score only one",
			task:          &schemodels.TaskInfo{ID: "task-0000"},
			clusters:      []*schemodels.ClusterInfo{{ID: "cluster-01"}, {ID: "cluster-02"}, {ID: "cluster-03"}, {ID: "cluster-04"}},
			scores:        []plugin.ScorePlugin{fakeScore},
			expClusterIDs: []string{"cluster-04"},
		},
		{
			name:          "no filter, average score, random",
			task:          &schemodels.TaskInfo{ID: "task-0000"},
			clusters:      []*schemodels.ClusterInfo{{ID: "cluster-02"}, {ID: "cluster-03"}},
			scores:        []plugin.ScorePlugin{fakeScore, fakeScoreAnother},
			expClusterIDs: []string{"cluster-02", "cluster-03"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fakeTaskCache := fake.NewFakeTaskCache(ctrl)
			switch len(test.expClusterIDs) {
			case 0:
				fakeTaskCache.EXPECT().UpdateTask(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
			case 1:
				fakeTaskCache.EXPECT().UpdateTask(gomock.Any(), test.task.ID, nil, utils.Point(test.expClusterIDs[0]), nil)
			default:
				// random
				for _, clusterID := range test.expClusterIDs {
					fakeTaskCache.EXPECT().UpdateTask(gomock.Any(), test.task.ID, nil, utils.Point(clusterID), nil).AnyTimes()
				}
			}
			s := &Scheduler{
				cache: &cache.Cache{TaskCache: fakeTaskCache},
				plugins: pluginsGroup{
					globalFilters: test.globalFilters,
					filters:       test.filters,
					scores:        test.scores,
				},
			}
			g.Expect(func() { s.scheduleTask(test.task, test.clusters) }).NotTo(gomega.Panic())
		})
	}
}

func TestInitPluginsGroup(t *testing.T) {
	g := gomega.NewWithT(t)
	opts := &Options{Plugins: []string{
		clustercapacity.Name,
		clusterlimit.Name,
		prioritysort.Name,
		resourcequota.Name,
	}}
	cache := &cache.Cache{}
	plugins, err := initPluginsGroup(opts, cache)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(plugins.sort.Name()).To(gomega.Equal(prioritysort.Name))
	g.Expect(plugins.globalFilters[0].Name()).To(gomega.Equal(resourcequota.Name))
	g.Expect(plugins.filters[0].Name()).To(gomega.Equal(clustercapacity.Name))
	g.Expect(plugins.filters[1].Name()).To(gomega.Equal(clusterlimit.Name))
	g.Expect(plugins.scores[0].Name()).To(gomega.Equal(clustercapacity.Name))
}
