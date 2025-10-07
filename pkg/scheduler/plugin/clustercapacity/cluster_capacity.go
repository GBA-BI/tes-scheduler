package clustercapacity

import (
	"context"
	"fmt"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"

	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/cache"
	schemodels "github.com/GBA-BI/tes-scheduler/pkg/scheduler/models"
	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/plugin"
)

// Name is the plugin name
const Name = "ClusterCapacity"

type impl struct {
	cache *cache.Cache
}

var _ plugin.FilterPlugin = (*impl)(nil)
var _ plugin.ScorePlugin = (*impl)(nil)

// New ...
func New(_ interface{}, cache *cache.Cache) (plugin.Plugin, error) {
	return &impl{cache: cache}, nil
}

// Name ...
func (i *impl) Name() string {
	return Name
}

// Filter ...
func (i *impl) Filter(_ context.Context, task *schemodels.TaskInfo, cluster *schemodels.ClusterInfo, cycleState map[string]interface{}) error {
	if cluster.Capacity == nil {
		return nil
	}

	var totalCount = 0
	var totalCPUCores = 0
	var totalRamGB float64 = 0
	var totalDiskGB float64 = 0
	var totalGPUCount float64 = 0
	totalGPU := make(map[string]float64)
	scheduled := i.cache.TaskCache.ListTasks(cluster.ID)
	for _, item := range scheduled {
		totalCount++
		if item.Resources == nil {
			continue
		}
		totalCPUCores += item.Resources.CPUCores
		totalRamGB += item.Resources.RamGB
		totalDiskGB += item.Resources.DiskGB
		if item.Resources.GPU == nil {
			continue
		}
		totalGPUCount += item.Resources.GPU.Count
		if item.Resources.GPU.Type != "" {
			totalGPU[item.Resources.GPU.Type] += item.Resources.GPU.Count
		}
	}

	var errs []error
	if cluster.Capacity.Count != nil && *cluster.Capacity.Count < totalCount+1 {
		errs = append(errs, fmt.Errorf("count should no more than %d, occupied %d", *cluster.Capacity.Count, totalCount))
	}
	if task.Resources != nil {
		if cluster.Capacity.CPUCores != nil && task.Resources.CPUCores > 0 && *cluster.Capacity.CPUCores < totalCPUCores+task.Resources.CPUCores {
			errs = append(errs, fmt.Errorf("CPUCores should no more than %d, occupied %d, claimed %d", *cluster.Capacity.CPUCores, totalCPUCores, task.Resources.CPUCores))
		}
		if cluster.Capacity.RamGB != nil && task.Resources.RamGB > 0 && *cluster.Capacity.RamGB < totalRamGB+task.Resources.RamGB {
			errs = append(errs, fmt.Errorf("RamGB should no more than %.2f, occupied %.2f, claimed %.2f", *cluster.Capacity.RamGB, totalRamGB, task.Resources.RamGB))
		}
		if cluster.Capacity.DiskGB != nil && task.Resources.DiskGB > 0 && *cluster.Capacity.DiskGB < totalDiskGB+task.Resources.DiskGB {
			errs = append(errs, fmt.Errorf("DiskGB should no more than %.2f, occupied %.2f, claimed %.2f", *cluster.Capacity.DiskGB, totalDiskGB, task.Resources.DiskGB))
		}
		if cluster.Capacity.GPUCapacity != nil && task.Resources.GPU != nil {
			// no matter task with gpuType or not, we must check total gpu count, because maybe there are
			// running tasks without gpuType using this type of GPU
			var sumGPUCountCapacity float64 = 0
			for _, gpuCount := range cluster.Capacity.GPUCapacity.GPU {
				sumGPUCountCapacity += gpuCount
			}
			if sumGPUCountCapacity < totalGPUCount+task.Resources.GPU.Count {
				errs = append(errs, fmt.Errorf("GPUCount should no more than %.2f, occupied %.2f, claimed %.2f", sumGPUCountCapacity, totalGPUCount, task.Resources.GPU.Count))
			}
			if task.Resources.GPU.Type != "" {
				gpuType := task.Resources.GPU.Type
				gpuCountCapacity, ok := cluster.Capacity.GPUCapacity.GPU[gpuType]
				if !ok {
					errs = append(errs, fmt.Errorf("no match GPUType: %s", gpuType))
				} else if gpuCountCapacity < totalGPU[gpuType]+task.Resources.GPU.Count {
					errs = append(errs, fmt.Errorf("GPUCount should no more than %.2f, occupied %.2f, claimed %.2f", gpuCountCapacity, totalGPU[gpuType], task.Resources.GPU.Count))
				}
			}
		}
	}
	if len(errs) > 0 {
		return utilerrors.NewAggregate(errs)
	}

	cycleState[totalCountKey] = totalCount
	cycleState[totalCPUCoreKey] = totalCPUCores
	cycleState[totalRamGBKey] = totalRamGB
	cycleState[totalDiskGBKey] = totalDiskGB
	cycleState[totalGPUCountKey] = totalGPUCount
	cycleState[totalGPUKey] = totalGPU
	return nil
}

// Score ...
func (i *impl) Score(_ context.Context, task *schemodels.TaskInfo, cluster *schemodels.ClusterInfo, cycleState map[string]interface{}) int64 {
	if cluster.Capacity == nil {
		return plugin.MaxScore
	}

	var totalScore int64 = 0
	var totalItem int64 = 0

	if cluster.Capacity.Count != nil {
		totalScore += leastRequestedScore(float64(1+cycleState[totalCountKey].(int)), float64(*cluster.Capacity.Count))
		totalItem++
	}
	if task.Resources != nil {
		if cluster.Capacity.CPUCores != nil && task.Resources.CPUCores > 0 {
			totalScore += leastRequestedScore(float64(task.Resources.CPUCores+cycleState[totalCPUCoreKey].(int)), float64(*cluster.Capacity.CPUCores))
			totalItem++
		}
		if cluster.Capacity.RamGB != nil && task.Resources.RamGB > 0 {
			totalScore += leastRequestedScore(task.Resources.RamGB+cycleState[totalRamGBKey].(float64), *cluster.Capacity.RamGB)
			totalItem++
		}
		if cluster.Capacity.DiskGB != nil && task.Resources.DiskGB > 0 {
			totalScore += leastRequestedScore(task.Resources.DiskGB+cycleState[totalDiskGBKey].(float64), *cluster.Capacity.DiskGB)
			totalItem++
		}
		if cluster.Capacity.GPUCapacity != nil && task.Resources.GPU != nil {
			if task.Resources.GPU.Type == "" {
				var sumGPUCountCapacity float64 = 0
				for _, gpuCount := range cluster.Capacity.GPUCapacity.GPU {
					sumGPUCountCapacity += gpuCount
				}
				totalScore += leastRequestedScore(task.Resources.GPU.Count+cycleState[totalGPUCountKey].(float64), sumGPUCountCapacity)
			} else {
				totalScore += leastRequestedScore(task.Resources.GPU.Count+cycleState[totalGPUKey].(map[string]float64)[task.Resources.GPU.Type], cluster.Capacity.GPUCapacity.GPU[task.Resources.GPU.Type])
			}
			totalItem++
		}
	}

	if totalItem == 0 {
		return plugin.MaxScore
	}
	return totalScore / totalItem
}

func leastRequestedScore(requested, capacity float64) int64 {
	if capacity == 0 {
		return plugin.MinScore
	}
	if requested > capacity {
		return plugin.MinScore
	}
	return int64((capacity - requested) / capacity * float64(plugin.MaxScore))
}

const (
	totalCountKey    = "totalCount"
	totalCPUCoreKey  = "totalCPUCore"
	totalRamGBKey    = "totalRamGB"
	totalDiskGBKey   = "totalDiskGB"
	totalGPUCountKey = "totalGPUCount"
	totalGPUKey      = "totalGPU"
)
