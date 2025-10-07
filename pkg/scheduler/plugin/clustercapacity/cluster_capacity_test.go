package clustercapacity

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/onsi/gomega"

	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/cache"
	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/cache/fake"
	schemodels "github.com/GBA-BI/tes-scheduler/pkg/scheduler/models"
	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/plugin"
	"github.com/GBA-BI/tes-scheduler/pkg/utils"
)

func TestFilter(t *testing.T) {
	g := gomega.NewWithT(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tests := []struct {
		name          string
		task          *schemodels.TaskInfo
		cluster       *schemodels.ClusterInfo
		scheduled     []*schemodels.TaskInfo
		expErr        bool
		expCycleState map[string]interface{}
	}{
		{
			name: "no cluster capacity",
			task: &schemodels.TaskInfo{
				ID: "task-0000",
			},
			cluster: &schemodels.ClusterInfo{
				ID: "cluster-01",
			},
			scheduled:     nil,
			expErr:        false,
			expCycleState: map[string]interface{}{},
		},
		{
			name: "no task resource, count enough",
			task: &schemodels.TaskInfo{
				ID: "task-0000",
			},
			cluster: &schemodels.ClusterInfo{
				ID: "cluster-01",
				Capacity: &schemodels.Capacity{
					Count: utils.Point(10),
				},
			},
			scheduled: []*schemodels.TaskInfo{{
				ID:        "task-exist",
				ClusterID: "cluster-01",
			}},
			expErr: false,
			expCycleState: map[string]interface{}{
				totalCountKey:    1,
				totalCPUCoreKey:  0,
				totalRamGBKey:    float64(0),
				totalDiskGBKey:   float64(0),
				totalGPUCountKey: float64(0),
				totalGPUKey:      map[string]float64{},
			},
		},
		{
			name: "no task resource, count not enough",
			task: &schemodels.TaskInfo{
				ID: "task-0000",
			},
			cluster: &schemodels.ClusterInfo{
				ID: "cluster-01",
				Capacity: &schemodels.Capacity{
					Count: utils.Point(1),
				},
			},
			scheduled: []*schemodels.TaskInfo{{
				ID:        "task-exist",
				ClusterID: "cluster-01",
			}},
			expErr:        true,
			expCycleState: map[string]interface{}{},
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
			cluster: &schemodels.ClusterInfo{
				ID: "cluster-01",
				Capacity: &schemodels.Capacity{
					CPUCores: utils.Point(10),
					RamGB:    utils.Point[float64](20),
					DiskGB:   utils.Point[float64](100),
				},
			},
			scheduled: []*schemodels.TaskInfo{{
				ID:        "task-exist-01",
				ClusterID: "cluster-01",
				Resources: &schemodels.Resources{
					CPUCores: 1,
					RamGB:    2,
					DiskGB:   10,
				},
			}, {
				ID:        "task-exist-02",
				ClusterID: "cluster-01",
				Resources: &schemodels.Resources{
					CPUCores: 1,
					RamGB:    2,
					DiskGB:   10,
				},
			}},
			expErr: false,
			expCycleState: map[string]interface{}{
				totalCountKey:    2,
				totalCPUCoreKey:  2,
				totalRamGBKey:    float64(4),
				totalDiskGBKey:   float64(20),
				totalGPUCountKey: float64(0),
				totalGPUKey:      map[string]float64{},
			},
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
			cluster: &schemodels.ClusterInfo{
				ID: "cluster-01",
				Capacity: &schemodels.Capacity{
					CPUCores: utils.Point(10),
					RamGB:    utils.Point[float64](20),
					DiskGB:   utils.Point[float64](100),
				},
			},
			scheduled: []*schemodels.TaskInfo{{
				ID:        "task-exist-01",
				ClusterID: "cluster-01",
				Resources: &schemodels.Resources{
					CPUCores: 1,
					RamGB:    2,
					DiskGB:   10,
				},
			}, {
				ID:        "task-exist-02",
				ClusterID: "cluster-01",
				Resources: &schemodels.Resources{
					CPUCores: 1,
					RamGB:    2,
					DiskGB:   10,
				},
			}},
			expErr:        true,
			expCycleState: map[string]interface{}{},
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
			cluster: &schemodels.ClusterInfo{
				ID: "cluster-01",
				Capacity: &schemodels.Capacity{
					CPUCores: utils.Point(10),
					RamGB:    utils.Point[float64](20),
					DiskGB:   utils.Point[float64](100),
				},
			},
			scheduled: []*schemodels.TaskInfo{{
				ID:        "task-exist-01",
				ClusterID: "cluster-01",
				Resources: &schemodels.Resources{
					CPUCores: 1,
					RamGB:    2,
					DiskGB:   10,
				},
			}, {
				ID:        "task-exist-02",
				ClusterID: "cluster-01",
				Resources: &schemodels.Resources{
					CPUCores: 1,
					RamGB:    2,
					DiskGB:   10,
				},
			}},
			expErr:        true,
			expCycleState: map[string]interface{}{},
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
			cluster: &schemodels.ClusterInfo{
				ID: "cluster-01",
				Capacity: &schemodels.Capacity{
					CPUCores: utils.Point(10),
					RamGB:    utils.Point[float64](20),
					DiskGB:   utils.Point[float64](100),
				},
			},
			scheduled: []*schemodels.TaskInfo{{
				ID:        "task-exist-01",
				ClusterID: "cluster-01",
				Resources: &schemodels.Resources{
					CPUCores: 1,
					RamGB:    2,
					DiskGB:   10,
				},
			}, {
				ID:        "task-exist-02",
				ClusterID: "cluster-01",
				Resources: &schemodels.Resources{
					CPUCores: 1,
					RamGB:    2,
					DiskGB:   10,
				},
			}},
			expErr:        true,
			expCycleState: map[string]interface{}{},
		},
		{
			name: "task with gpu, nil cluster GPUCapacity means no limit",
			task: &schemodels.TaskInfo{
				ID: "task-0000",
				Resources: &schemodels.Resources{
					GPU: &schemodels.GPUResource{Count: 10},
				},
			},
			cluster: &schemodels.ClusterInfo{
				ID: "cluster-01",
				Capacity: &schemodels.Capacity{
					GPUCapacity: nil,
				},
			},
			expErr: false,
			expCycleState: map[string]interface{}{
				totalCountKey:    0,
				totalCPUCoreKey:  0,
				totalRamGBKey:    float64(0),
				totalDiskGBKey:   float64(0),
				totalGPUCountKey: float64(0),
				totalGPUKey:      map[string]float64{},
			},
		},
		{
			name: "task with gpu, empty cluster GPUCapacity means no gpu",
			task: &schemodels.TaskInfo{
				ID: "task-0000",
				Resources: &schemodels.Resources{
					GPU: &schemodels.GPUResource{Count: 10},
				},
			},
			cluster: &schemodels.ClusterInfo{
				ID: "cluster-01",
				Capacity: &schemodels.Capacity{
					GPUCapacity: &schemodels.GPUCapacity{},
				},
			},
			expErr:        true,
			expCycleState: map[string]interface{}{},
		},
		{
			name: "task with only gpu count, enough",
			task: &schemodels.TaskInfo{
				ID: "task-0000",
				Resources: &schemodels.Resources{
					GPU: &schemodels.GPUResource{Count: 10},
				},
			},
			cluster: &schemodels.ClusterInfo{
				ID: "cluster-01",
				Capacity: &schemodels.Capacity{
					GPUCapacity: &schemodels.GPUCapacity{
						GPU: map[string]float64{"type1": 6, "type2": 6},
					},
				},
			},
			scheduled: []*schemodels.TaskInfo{{
				ID:        "task-exist-01",
				ClusterID: "cluster-01",
				Resources: &schemodels.Resources{
					GPU: &schemodels.GPUResource{Type: "type1", Count: 1},
				},
			}, {
				ID:        "task-exist-02",
				ClusterID: "cluster-01",
				Resources: &schemodels.Resources{
					GPU: &schemodels.GPUResource{Count: 1},
				},
			}},
			expErr: false,
			expCycleState: map[string]interface{}{
				totalCountKey:    2,
				totalCPUCoreKey:  0,
				totalRamGBKey:    float64(0),
				totalDiskGBKey:   float64(0),
				totalGPUCountKey: float64(2),
				totalGPUKey: map[string]float64{
					"type1": 1,
				},
			},
		},
		{
			name: "task with only gpu count, not enough",
			task: &schemodels.TaskInfo{
				ID: "task-0000",
				Resources: &schemodels.Resources{
					GPU: &schemodels.GPUResource{Count: 11},
				},
			},
			cluster: &schemodels.ClusterInfo{
				ID: "cluster-01",
				Capacity: &schemodels.Capacity{
					GPUCapacity: &schemodels.GPUCapacity{
						GPU: map[string]float64{"type1": 6, "type2": 6},
					},
				},
			},
			scheduled: []*schemodels.TaskInfo{{
				ID:        "task-exist-01",
				ClusterID: "cluster-01",
				Resources: &schemodels.Resources{
					GPU: &schemodels.GPUResource{Type: "type1", Count: 1},
				},
			}, {
				ID:        "task-exist-02",
				ClusterID: "cluster-01",
				Resources: &schemodels.Resources{
					GPU: &schemodels.GPUResource{Count: 1},
				},
			}},
			expErr:        true,
			expCycleState: map[string]interface{}{},
		},
		{
			name: "task with gpu count and type, enough",
			task: &schemodels.TaskInfo{
				ID: "task-0000",
				Resources: &schemodels.Resources{
					GPU: &schemodels.GPUResource{Count: 1, Type: "type1"},
				},
			},
			cluster: &schemodels.ClusterInfo{
				ID: "cluster-01",
				Capacity: &schemodels.Capacity{
					GPUCapacity: &schemodels.GPUCapacity{
						GPU: map[string]float64{"type1": 5, "type2": 5},
					},
				},
			},
			scheduled: []*schemodels.TaskInfo{{
				ID:        "task-exist-01",
				ClusterID: "cluster-01",
				Resources: &schemodels.Resources{
					GPU: &schemodels.GPUResource{Type: "type1", Count: 1},
				},
			}, {
				ID:        "task-exist-02",
				ClusterID: "cluster-01",
				Resources: &schemodels.Resources{
					GPU: &schemodels.GPUResource{Count: 1},
				},
			}},
			expErr: false,
			expCycleState: map[string]interface{}{
				totalCountKey:    2,
				totalCPUCoreKey:  0,
				totalRamGBKey:    float64(0),
				totalDiskGBKey:   float64(0),
				totalGPUCountKey: float64(2),
				totalGPUKey: map[string]float64{
					"type1": 1,
				},
			},
		},
		{
			name: "task with gpu count and type, total count not enough",
			task: &schemodels.TaskInfo{
				ID: "task-0000",
				Resources: &schemodels.Resources{
					GPU: &schemodels.GPUResource{Count: 1, Type: "type1"},
				},
			},
			cluster: &schemodels.ClusterInfo{
				ID: "cluster-01",
				Capacity: &schemodels.Capacity{
					GPUCapacity: &schemodels.GPUCapacity{
						GPU: map[string]float64{"type1": 5, "type2": 5},
					},
				},
			},
			scheduled: []*schemodels.TaskInfo{{
				ID:        "task-exist-01",
				ClusterID: "cluster-01",
				Resources: &schemodels.Resources{
					GPU: &schemodels.GPUResource{Type: "type1", Count: 1},
				},
			}, {
				ID:        "task-exist-02",
				ClusterID: "cluster-01",
				Resources: &schemodels.Resources{
					GPU: &schemodels.GPUResource{Count: 9},
				},
			}},
			expErr:        true,
			expCycleState: map[string]interface{}{},
		},
		{
			name: "task with gpu count and type, type count not enough",
			task: &schemodels.TaskInfo{
				ID: "task-0000",
				Resources: &schemodels.Resources{
					GPU: &schemodels.GPUResource{Count: 1, Type: "type1"},
				},
			},
			cluster: &schemodels.ClusterInfo{
				ID: "cluster-01",
				Capacity: &schemodels.Capacity{
					GPUCapacity: &schemodels.GPUCapacity{
						GPU: map[string]float64{"type1": 5, "type2": 5},
					},
				},
			},
			scheduled: []*schemodels.TaskInfo{{
				ID:        "task-exist-01",
				ClusterID: "cluster-01",
				Resources: &schemodels.Resources{
					GPU: &schemodels.GPUResource{Type: "type1", Count: 5},
				},
			}, {
				ID:        "task-exist-02",
				ClusterID: "cluster-01",
				Resources: &schemodels.Resources{
					GPU: &schemodels.GPUResource{Count: 1},
				},
			}},
			expErr:        true,
			expCycleState: map[string]interface{}{},
		},
		{
			name: "task with gpu count and type, no match type",
			task: &schemodels.TaskInfo{
				ID: "task-0000",
				Resources: &schemodels.Resources{
					GPU: &schemodels.GPUResource{Count: 1, Type: "type3"},
				},
			},
			cluster: &schemodels.ClusterInfo{
				ID: "cluster-01",
				Capacity: &schemodels.Capacity{
					GPUCapacity: &schemodels.GPUCapacity{
						GPU: map[string]float64{"type1": 5, "type2": 5},
					},
				},
			},
			scheduled: []*schemodels.TaskInfo{{
				ID:        "task-exist-01",
				ClusterID: "cluster-01",
				Resources: &schemodels.Resources{
					GPU: &schemodels.GPUResource{Type: "type1", Count: 1},
				},
			}, {
				ID:        "task-exist-02",
				ClusterID: "cluster-01",
				Resources: &schemodels.Resources{
					GPU: &schemodels.GPUResource{Count: 1},
				},
			}},
			expErr:        true,
			expCycleState: map[string]interface{}{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fakeTaskCache := fake.NewFakeTaskCache(ctrl)
			fakeTaskCache.EXPECT().ListTasks(test.cluster.ID).Return(test.scheduled).AnyTimes() // 0 or 1 time
			i := &impl{cache: &cache.Cache{TaskCache: fakeTaskCache}}
			cycleState := make(map[string]interface{})
			g.Expect(i.Filter(context.Background(), test.task, test.cluster, cycleState) != nil).To(gomega.Equal(test.expErr))
			g.Expect(cycleState).To(gomega.BeEquivalentTo(test.expCycleState))
		})
	}
}

func TestScore(t *testing.T) {
	g := gomega.NewWithT(t)

	tests := []struct {
		name       string
		task       *schemodels.TaskInfo
		cluster    *schemodels.ClusterInfo
		cycleState map[string]interface{}
		expScore   int64
	}{
		{
			name: "no cluster capacity",
			task: &schemodels.TaskInfo{
				ID: "task-0000",
			},
			cluster: &schemodels.ClusterInfo{
				ID: "cluster-01",
			},
			cycleState: map[string]interface{}{},
			expScore:   plugin.MaxScore,
		},
		{
			name: "zero total item",
			task: &schemodels.TaskInfo{
				ID: "task-0000",
			},
			cluster: &schemodels.ClusterInfo{
				ID:       "clusterID",
				Capacity: &schemodels.Capacity{},
			},
			cycleState: map[string]interface{}{},
			expScore:   plugin.MaxScore,
		},
		{
			name: "no task resource, only count",
			task: &schemodels.TaskInfo{
				ID: "task-0000",
			},
			cluster: &schemodels.ClusterInfo{
				ID: "cluster-01",
				Capacity: &schemodels.Capacity{
					Count: utils.Point(10),
				},
			},
			cycleState: map[string]interface{}{
				totalCountKey:    1,
				totalCPUCoreKey:  0,
				totalRamGBKey:    float64(0),
				totalDiskGBKey:   float64(0),
				totalGPUCountKey: float64(0),
				totalGPUKey:      map[string]float64{},
			},
			expScore: 80, // (10-2)/10*100
		},
		{
			name: "task with cpu/ram/disk",
			task: &schemodels.TaskInfo{
				ID: "task-0000",
				Resources: &schemodels.Resources{
					CPUCores: 1,
					RamGB:    2,
					DiskGB:   10,
				},
			},
			cluster: &schemodels.ClusterInfo{
				ID: "cluster-01",
				Capacity: &schemodels.Capacity{
					CPUCores: utils.Point(10),
					RamGB:    utils.Point[float64](20),
					DiskGB:   utils.Point[float64](100),
				},
			},
			cycleState: map[string]interface{}{
				totalCountKey:    2,
				totalCPUCoreKey:  2,
				totalRamGBKey:    float64(4),
				totalDiskGBKey:   float64(20),
				totalGPUCountKey: float64(0),
				totalGPUKey:      map[string]float64{},
			},
			expScore: 70, // ((10-3)/10*100 + (20-6)/20*100 + (100-30)/100*100) / 3
		},
		{
			name: "task with only gpu count",
			task: &schemodels.TaskInfo{
				ID: "task-0000",
				Resources: &schemodels.Resources{
					GPU: &schemodels.GPUResource{Count: 10},
				},
			},
			cluster: &schemodels.ClusterInfo{
				ID: "cluster-01",
				Capacity: &schemodels.Capacity{
					GPUCapacity: &schemodels.GPUCapacity{
						GPU: map[string]float64{"type1": 6, "type2": 6},
					},
				},
			},
			cycleState: map[string]interface{}{
				totalCountKey:    2,
				totalCPUCoreKey:  0,
				totalRamGBKey:    float64(0),
				totalDiskGBKey:   float64(0),
				totalGPUCountKey: float64(2),
				totalGPUKey: map[string]float64{
					"type1": 1,
				},
			},
			expScore: 0, // (12-12)/12 * 100
		},
		{
			name: "task with gpu count and type",
			task: &schemodels.TaskInfo{
				ID: "task-0000",
				Resources: &schemodels.Resources{
					GPU: &schemodels.GPUResource{Count: 1, Type: "type1"},
				},
			},
			cluster: &schemodels.ClusterInfo{
				ID: "cluster-01",
				Capacity: &schemodels.Capacity{
					GPUCapacity: &schemodels.GPUCapacity{
						GPU: map[string]float64{"type1": 5, "type2": 5},
					},
				},
			},
			cycleState: map[string]interface{}{
				totalCountKey:    2,
				totalCPUCoreKey:  0,
				totalRamGBKey:    float64(0),
				totalDiskGBKey:   float64(0),
				totalGPUCountKey: float64(2),
				totalGPUKey: map[string]float64{
					"type1": 1,
				},
			},
			expScore: 60, // (5-2)/5 * 100
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			i := &impl{}
			g.Expect(i.Score(context.Background(), test.task, test.cluster, test.cycleState)).To(gomega.Equal(test.expScore))
		})
	}
}
