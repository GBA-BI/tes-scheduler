package resourcequota

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/onsi/gomega"

	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/cache"
	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/cache/fake"
	schemodels "github.com/GBA-BI/tes-scheduler/pkg/scheduler/models"
	"github.com/GBA-BI/tes-scheduler/pkg/utils"
)

func TestGlobalFilter(t *testing.T) {
	g := gomega.NewWithT(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tests := []struct {
		name                 string
		task                 *schemodels.TaskInfo
		globalResourceQuota  *schemodels.ResourceQuota
		accountResourceQuota *schemodels.ResourceQuota
		userResourceQuota    *schemodels.ResourceQuota
		scheduledTasks       []*schemodels.TaskInfo
		expErr               bool
	}{
		{
			name:   "no quota",
			task:   &schemodels.TaskInfo{ID: "task-0000"},
			expErr: false,
		},
		{
			name:                "global quota enough",
			task:                &schemodels.TaskInfo{ID: "task-0000"},
			globalResourceQuota: &schemodels.ResourceQuota{Count: utils.Point(5)},
			scheduledTasks:      []*schemodels.TaskInfo{{ID: "task-exist-01"}, {ID: "task-exist-02"}},
			expErr:              false,
		},
		{
			name:                "global quota not enough",
			task:                &schemodels.TaskInfo{ID: "task-0000"},
			globalResourceQuota: &schemodels.ResourceQuota{Count: utils.Point(2)},
			scheduledTasks:      []*schemodels.TaskInfo{{ID: "task-exist-01"}, {ID: "task-exist-02"}},
			expErr:              true,
		},
		{
			name:                 "account quota enough",
			task:                 &schemodels.TaskInfo{ID: "task-0000", BioosInfo: &schemodels.BioosInfo{AccountID: "account-01", UserID: "user-01"}},
			accountResourceQuota: &schemodels.ResourceQuota{Count: utils.Point(2)},
			scheduledTasks: []*schemodels.TaskInfo{
				{ID: "task-exist-01", BioosInfo: &schemodels.BioosInfo{AccountID: "account-01", UserID: "user-02"}},
				{ID: "task-exist-02", BioosInfo: &schemodels.BioosInfo{AccountID: "account-02", UserID: "user-03"}},
			},
			expErr: false,
		},
		{
			name:                 "account quota not enough",
			task:                 &schemodels.TaskInfo{ID: "task-0000", BioosInfo: &schemodels.BioosInfo{AccountID: "account-01", UserID: "user-01"}},
			accountResourceQuota: &schemodels.ResourceQuota{Count: utils.Point(2)},
			scheduledTasks: []*schemodels.TaskInfo{
				{ID: "task-exist-01", BioosInfo: &schemodels.BioosInfo{AccountID: "account-01", UserID: "user-02"}},
				{ID: "task-exist-02", BioosInfo: &schemodels.BioosInfo{AccountID: "account-01", UserID: "user-03"}},
			},
			expErr: true,
		},
		{
			name:              "user quota enough",
			task:              &schemodels.TaskInfo{ID: "task-0000", BioosInfo: &schemodels.BioosInfo{AccountID: "account-01", UserID: "user-01"}},
			userResourceQuota: &schemodels.ResourceQuota{Count: utils.Point(2)},
			scheduledTasks: []*schemodels.TaskInfo{
				{ID: "task-exist-01", BioosInfo: &schemodels.BioosInfo{AccountID: "account-01", UserID: "user-01"}},
				{ID: "task-exist-02", BioosInfo: &schemodels.BioosInfo{AccountID: "account-01", UserID: "user-02"}},
			},
			expErr: false,
		},
		{
			name:              "user quota not enough",
			task:              &schemodels.TaskInfo{ID: "task-0000", BioosInfo: &schemodels.BioosInfo{AccountID: "account-01", UserID: "user-01"}},
			userResourceQuota: &schemodels.ResourceQuota{Count: utils.Point(2)},
			scheduledTasks: []*schemodels.TaskInfo{
				{ID: "task-exist-01", BioosInfo: &schemodels.BioosInfo{AccountID: "account-01", UserID: "user-01"}},
				{ID: "task-exist-02", BioosInfo: &schemodels.BioosInfo{AccountID: "account-01", UserID: "user-01"}},
			},
			expErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fakeQuotaCache := fake.NewFakeQuotaCache(ctrl)
			fakeQuotaCache.EXPECT().GetGlobalQuota(gomock.Any()).Return(test.globalResourceQuota, nil).AnyTimes()
			fakeQuotaCache.EXPECT().GetAccountQuota(gomock.Any(), gomock.Any()).Return(test.accountResourceQuota, nil).AnyTimes()
			fakeQuotaCache.EXPECT().GetUserQuota(gomock.Any(), gomock.Any(), gomock.Any()).Return(test.userResourceQuota, nil).AnyTimes()
			fakeTaskCache := fake.NewFakeTaskCache(ctrl)
			fakeTaskCache.EXPECT().ListScheduledTasks().Return(test.scheduledTasks)
			i := &impl{cache: &cache.Cache{QuotaCache: fakeQuotaCache, TaskCache: fakeTaskCache}}
			err := i.GlobalFilter(context.Background(), test.task, make(map[string]interface{}))
			g.Expect(err != nil).To(gomega.Equal(test.expErr))
		})
	}
}

func TestCheckQuota(t *testing.T) {
	g := gomega.NewWithT(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tests := []struct {
		name      string
		quota     *schemodels.ResourceQuota
		task      *schemodels.TaskInfo
		scheduled []*schemodels.TaskInfo // match
		expErr    bool
	}{
		{
			name: "no quota",
			task: &schemodels.TaskInfo{
				ID: "task-0000",
			},
			quota:     nil,
			scheduled: nil,
			expErr:    false,
		},
		{
			name: "no task resource, count enough",
			task: &schemodels.TaskInfo{
				ID: "task-0000",
			},
			quota: &schemodels.ResourceQuota{
				Count: utils.Point(10),
			},
			scheduled: []*schemodels.TaskInfo{{
				ID: "task-exist",
			}},
			expErr: false,
		},
		{
			name: "no task resource, count not enough",
			task: &schemodels.TaskInfo{
				ID: "task-0000",
			},
			quota: &schemodels.ResourceQuota{
				Count: utils.Point(1),
			},
			scheduled: []*schemodels.TaskInfo{{
				ID: "task-exist",
			}},
			expErr: true,
		},
		{
			name: "task with cpu/ram/disk, enough",
			task: &schemodels.TaskInfo{
				ID: "task-0000",
				Resources: &schemodels.Resources{
					CPUCores: 1,
					RamGB:    2,
					DiskGB:   10,
				},
			},
			quota: &schemodels.ResourceQuota{
				CPUCores: utils.Point(10),
				RamGB:    utils.Point[float64](20),
				DiskGB:   utils.Point[float64](100),
			},
			scheduled: []*schemodels.TaskInfo{{
				ID: "task-exist-01",
				Resources: &schemodels.Resources{
					CPUCores: 1,
					RamGB:    2,
					DiskGB:   10,
				},
			}, {
				ID: "task-exist-02",
				Resources: &schemodels.Resources{
					CPUCores: 1,
					RamGB:    2,
					DiskGB:   10,
				},
			}},
			expErr: false,
		},
		{
			name: "task with cpu/ram/disk, cpu not enough",
			task: &schemodels.TaskInfo{
				ID: "task-0000",
				Resources: &schemodels.Resources{
					CPUCores: 10,
					RamGB:    10,
					DiskGB:   10,
				},
			},
			quota: &schemodels.ResourceQuota{
				CPUCores: utils.Point(10),
				RamGB:    utils.Point[float64](20),
				DiskGB:   utils.Point[float64](100),
			},
			scheduled: []*schemodels.TaskInfo{{
				ID: "task-exist-01",
				Resources: &schemodels.Resources{
					CPUCores: 1,
					RamGB:    2,
					DiskGB:   10,
				},
			}, {
				ID: "task-exist-02",
				Resources: &schemodels.Resources{
					CPUCores: 1,
					RamGB:    2,
					DiskGB:   10,
				},
			}},
			expErr: true,
		},
		{
			name: "task with cpu/ram/disk, ram not enough",
			task: &schemodels.TaskInfo{
				ID: "task-0000",
				Resources: &schemodels.Resources{
					CPUCores: 5,
					RamGB:    18,
					DiskGB:   10,
				},
			},
			quota: &schemodels.ResourceQuota{
				CPUCores: utils.Point(10),
				RamGB:    utils.Point[float64](20),
				DiskGB:   utils.Point[float64](100),
			},
			scheduled: []*schemodels.TaskInfo{{
				ID: "task-exist-01",
				Resources: &schemodels.Resources{
					CPUCores: 1,
					RamGB:    2,
					DiskGB:   10,
				},
			}, {
				ID: "task-exist-02",
				Resources: &schemodels.Resources{
					CPUCores: 1,
					RamGB:    2,
					DiskGB:   10,
				},
			}},
			expErr: true,
		},
		{
			name: "task with cpu/ram/disk, disk not enough",
			task: &schemodels.TaskInfo{
				ID: "task-0000",
				Resources: &schemodels.Resources{
					CPUCores: 1,
					RamGB:    2,
					DiskGB:   90,
				},
			},
			quota: &schemodels.ResourceQuota{
				CPUCores: utils.Point(10),
				RamGB:    utils.Point[float64](20),
				DiskGB:   utils.Point[float64](100),
			},
			scheduled: []*schemodels.TaskInfo{{
				ID: "task-exist-01",
				Resources: &schemodels.Resources{
					CPUCores: 1,
					RamGB:    2,
					DiskGB:   10,
				},
			}, {
				ID: "task-exist-02",
				Resources: &schemodels.Resources{
					CPUCores: 1,
					RamGB:    2,
					DiskGB:   10,
				},
			}},
			expErr: true,
		},
		{
			name: "task with gpu, nil GPUQuota means no limit",
			task: &schemodels.TaskInfo{
				ID: "task-0000",
				Resources: &schemodels.Resources{
					GPU: &schemodels.GPUResource{Count: 10},
				},
			},
			quota: &schemodels.ResourceQuota{
				GPUQuota: nil,
			},
			expErr: false,
		},
		{
			name: "task with gpu, empty GPUQuota means no gpu",
			task: &schemodels.TaskInfo{
				ID: "task-0000",
				Resources: &schemodels.Resources{
					GPU: &schemodels.GPUResource{Count: 10},
				},
			},
			quota: &schemodels.ResourceQuota{
				GPUQuota: &schemodels.GPUQuota{},
			},
			expErr: true,
		},
		{
			name: "task with only gpu count, enough",
			task: &schemodels.TaskInfo{
				ID: "task-0000",
				Resources: &schemodels.Resources{
					GPU: &schemodels.GPUResource{Count: 10},
				},
			},
			quota: &schemodels.ResourceQuota{
				GPUQuota: &schemodels.GPUQuota{
					GPU: map[string]float64{"type1": 6, "type2": 6},
				},
			},
			scheduled: []*schemodels.TaskInfo{{
				ID: "task-exist-01",
				Resources: &schemodels.Resources{
					GPU: &schemodels.GPUResource{Type: "type1", Count: 1},
				},
			}, {
				ID: "task-exist-02",
				Resources: &schemodels.Resources{
					GPU: &schemodels.GPUResource{Count: 1},
				},
			}},
			expErr: false,
		},
		{
			name: "task with only gpu count, not enough",
			task: &schemodels.TaskInfo{
				ID: "task-0000",
				Resources: &schemodels.Resources{
					GPU: &schemodels.GPUResource{Count: 11},
				},
			},
			quota: &schemodels.ResourceQuota{
				GPUQuota: &schemodels.GPUQuota{
					GPU: map[string]float64{"type1": 6, "type2": 6},
				},
			},
			scheduled: []*schemodels.TaskInfo{{
				ID: "task-exist-01",
				Resources: &schemodels.Resources{
					GPU: &schemodels.GPUResource{Type: "type1", Count: 1},
				},
			}, {
				ID: "task-exist-02",
				Resources: &schemodels.Resources{
					GPU: &schemodels.GPUResource{Count: 1},
				},
			}},
			expErr: true,
		},
		{
			name: "task with gpu count and type, enough",
			task: &schemodels.TaskInfo{
				ID: "task-0000",
				Resources: &schemodels.Resources{
					GPU: &schemodels.GPUResource{Count: 1, Type: "type1"},
				},
			},
			quota: &schemodels.ResourceQuota{
				GPUQuota: &schemodels.GPUQuota{
					GPU: map[string]float64{"type1": 5, "type2": 5},
				},
			},
			scheduled: []*schemodels.TaskInfo{{
				ID: "task-exist-01",
				Resources: &schemodels.Resources{
					GPU: &schemodels.GPUResource{Type: "type1", Count: 1},
				},
			}, {
				ID: "task-exist-02",
				Resources: &schemodels.Resources{
					GPU: &schemodels.GPUResource{Count: 1},
				},
			}},
			expErr: false,
		},
		{
			name: "task with gpu count and type, total count not enough",
			task: &schemodels.TaskInfo{
				ID: "task-0000",
				Resources: &schemodels.Resources{
					GPU: &schemodels.GPUResource{Count: 1, Type: "type1"},
				},
			},
			quota: &schemodels.ResourceQuota{
				GPUQuota: &schemodels.GPUQuota{
					GPU: map[string]float64{"type1": 5, "type2": 5},
				},
			},
			scheduled: []*schemodels.TaskInfo{{
				ID: "task-exist-01",
				Resources: &schemodels.Resources{
					GPU: &schemodels.GPUResource{Type: "type1", Count: 1},
				},
			}, {
				ID: "task-exist-02",
				Resources: &schemodels.Resources{
					GPU: &schemodels.GPUResource{Count: 9},
				},
			}},
			expErr: false,
		},
		{
			name: "task with gpu count and type, type count not enough",
			task: &schemodels.TaskInfo{
				ID: "task-0000",
				Resources: &schemodels.Resources{
					GPU: &schemodels.GPUResource{Count: 1, Type: "type1"},
				},
			},
			quota: &schemodels.ResourceQuota{
				GPUQuota: &schemodels.GPUQuota{
					GPU: map[string]float64{"type1": 5, "type2": 5},
				},
			},
			scheduled: []*schemodels.TaskInfo{{
				ID: "task-exist-01",
				Resources: &schemodels.Resources{
					GPU: &schemodels.GPUResource{Type: "type1", Count: 5},
				},
			}, {
				ID: "task-exist-02",
				Resources: &schemodels.Resources{
					GPU: &schemodels.GPUResource{Count: 1},
				},
			}},
			expErr: true,
		},
		{
			name: "task with gpu count and type, no match type",
			task: &schemodels.TaskInfo{
				ID: "task-0000",
				Resources: &schemodels.Resources{
					GPU: &schemodels.GPUResource{Count: 1, Type: "type3"},
				},
			},
			quota: &schemodels.ResourceQuota{
				GPUQuota: &schemodels.GPUQuota{
					GPU: map[string]float64{"type1": 5, "type2": 5},
				},
			},
			scheduled: []*schemodels.TaskInfo{{
				ID: "task-exist-01",
				Resources: &schemodels.Resources{
					GPU: &schemodels.GPUResource{Type: "type1", Count: 1},
				},
			}, {
				ID: "task-exist-02",
				Resources: &schemodels.Resources{
					GPU: &schemodels.GPUResource{Count: 1},
				},
			}},
			expErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := checkQuota(test.quota, test.task, test.scheduled, func(scheduledTask *schemodels.TaskInfo) bool {
				return true
			})
			g.Expect(err != nil).To(gomega.Equal(test.expErr))
		})
	}
}
