package clusterlimit

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/onsi/gomega"

	schemodels "github.com/GBA-BI/tes-scheduler/pkg/scheduler/models"
	"github.com/GBA-BI/tes-scheduler/pkg/utils"
)

func TestFilter(t *testing.T) {
	g := gomega.NewWithT(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tests := []struct {
		name    string
		task    *schemodels.TaskInfo
		cluster *schemodels.ClusterInfo
		expErr  bool
	}{
		{
			name: "no task resources",
			task: &schemodels.TaskInfo{Resources: nil},
			cluster: &schemodels.ClusterInfo{Limits: &schemodels.Limits{
				CPUCores: utils.Point(0),
				RamGB:    utils.Point[float64](0),
				GPULimit: &schemodels.GPULimit{},
			}},
			expErr: false,
		},
		{
			name: "no cluster limits",
			task: &schemodels.TaskInfo{Resources: &schemodels.Resources{
				CPUCores: 1,
				RamGB:    2,
				DiskGB:   5,
				GPU:      &schemodels.GPUResource{Type: "typ1", Count: 1},
			}},
			cluster: &schemodels.ClusterInfo{Limits: nil},
			expErr:  false,
		},
		{
			name: "task with cpu and ram, cpu not meet limit",
			task: &schemodels.TaskInfo{Resources: &schemodels.Resources{
				CPUCores: 4,
				RamGB:    2,
				DiskGB:   5,
			}},
			cluster: &schemodels.ClusterInfo{Limits: &schemodels.Limits{
				CPUCores: utils.Point(2),
				RamGB:    utils.Point[float64](5),
			}},
			expErr: true,
		},
		{
			name: "task with cpu and ram, ram not meet limit",
			task: &schemodels.TaskInfo{Resources: &schemodels.Resources{
				CPUCores: 1,
				RamGB:    10,
				DiskGB:   5,
			}},
			cluster: &schemodels.ClusterInfo{Limits: &schemodels.Limits{
				CPUCores: utils.Point(2),
				RamGB:    utils.Point[float64](5),
			}},
			expErr: true,
		},
		{
			name: "task with gpu, nil GPULimit means no limit",
			task: &schemodels.TaskInfo{Resources: &schemodels.Resources{
				GPU: &schemodels.GPUResource{Count: 2},
			}},
			cluster: &schemodels.ClusterInfo{Limits: &schemodels.Limits{
				GPULimit: nil,
			}},
			expErr: false,
		},
		{
			name: "task with gpu, empty GPULimit means no gpu",
			task: &schemodels.TaskInfo{Resources: &schemodels.Resources{
				GPU: &schemodels.GPUResource{Count: 2},
			}},
			cluster: &schemodels.ClusterInfo{Limits: &schemodels.Limits{
				GPULimit: &schemodels.GPULimit{},
			}},
			expErr: true,
		},
		{
			name: "task with only gpu count, enough",
			task: &schemodels.TaskInfo{Resources: &schemodels.Resources{
				GPU: &schemodels.GPUResource{Count: 2},
			}},
			cluster: &schemodels.ClusterInfo{Limits: &schemodels.Limits{
				GPULimit: &schemodels.GPULimit{
					GPU: map[string]float64{"type1": 3, "type2": 1},
				},
			}},
			expErr: false,
		},
		{
			name: "task with only gpu count, not meet limit",
			task: &schemodels.TaskInfo{Resources: &schemodels.Resources{
				GPU: &schemodels.GPUResource{Count: 3},
			}},
			cluster: &schemodels.ClusterInfo{Limits: &schemodels.Limits{
				GPULimit: &schemodels.GPULimit{
					GPU: map[string]float64{"type1": 2, "type2": 2},
				},
			}},
			expErr: true,
		},
		{
			name: "task with gpu count and type, enough",
			task: &schemodels.TaskInfo{Resources: &schemodels.Resources{
				GPU: &schemodels.GPUResource{Count: 2, Type: "type1"},
			}},
			cluster: &schemodels.ClusterInfo{Limits: &schemodels.Limits{
				GPULimit: &schemodels.GPULimit{
					GPU: map[string]float64{"type1": 3, "type2": 1},
				},
			}},
			expErr: false,
		},
		{
			name: "task with gpu count and type, count not meet limit",
			task: &schemodels.TaskInfo{Resources: &schemodels.Resources{
				GPU: &schemodels.GPUResource{Count: 2, Type: "type2"},
			}},
			cluster: &schemodels.ClusterInfo{Limits: &schemodels.Limits{
				GPULimit: &schemodels.GPULimit{
					GPU: map[string]float64{"type1": 3, "type2": 1},
				},
			}},
			expErr: true,
		},
		{
			name: "task with gpu count and type, no match type",
			task: &schemodels.TaskInfo{Resources: &schemodels.Resources{
				GPU: &schemodels.GPUResource{Count: 1, Type: "type3"},
			}},
			cluster: &schemodels.ClusterInfo{Limits: &schemodels.Limits{
				GPULimit: &schemodels.GPULimit{
					GPU: map[string]float64{"type1": 3, "type2": 1},
				},
			}},
			expErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			i := &impl{}
			g.Expect(i.Filter(context.Background(), test.task, test.cluster, map[string]interface{}{}) != nil).To(gomega.Equal(test.expErr))
		})
	}
}
