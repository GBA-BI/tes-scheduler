package resourcequota

import (
	"context"
	"fmt"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"

	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/cache"
	schemodels "github.com/GBA-BI/tes-scheduler/pkg/scheduler/models"
	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/plugin"
)

// Name is the plugin name
const Name = "ResourceQuota"

type impl struct {
	cache *cache.Cache
}

var _ plugin.GlobalFilterPlugin = (*impl)(nil)

// New ...
func New(_ interface{}, cache *cache.Cache) (plugin.Plugin, error) {
	return &impl{cache: cache}, nil
}

// Name ...
func (i *impl) Name() string {
	return Name
}

// GlobalFilter ...
func (i *impl) GlobalFilter(ctx context.Context, task *schemodels.TaskInfo, _ map[string]interface{}) error {
	scheduledTasks := i.cache.TaskCache.ListScheduledTasks()

	globalQuota, err := i.cache.QuotaCache.GetGlobalQuota(ctx)
	if err != nil {
		return err
	}
	if globalQuota != nil {
		if err = checkQuota(globalQuota, task, scheduledTasks, func(scheduledTask *schemodels.TaskInfo) bool {
			return true
		}); err != nil {
			return fmt.Errorf("global quota: %w", err)
		}
	}

	if task.BioosInfo == nil {
		return nil
	}

	if task.BioosInfo.AccountID != "" {
		accountQuota, err := i.cache.QuotaCache.GetAccountQuota(ctx, task.BioosInfo.AccountID)
		if err != nil {
			return err
		}
		if accountQuota != nil {
			if err = checkQuota(accountQuota, task, scheduledTasks, func(scheduledTask *schemodels.TaskInfo) bool {
				return scheduledTask.BioosInfo != nil && scheduledTask.BioosInfo.AccountID == task.BioosInfo.AccountID
			}); err != nil {
				return fmt.Errorf("account[%s] quota: %w", task.BioosInfo.AccountID, err)
			}
		}
	}

	if task.BioosInfo.AccountID != "" && task.BioosInfo.UserID != "" {
		userQuota, err := i.cache.QuotaCache.GetUserQuota(ctx, task.BioosInfo.AccountID, task.BioosInfo.UserID)
		if err != nil {
			return err
		}
		if userQuota != nil {
			if err = checkQuota(userQuota, task, scheduledTasks, func(scheduledTask *schemodels.TaskInfo) bool {
				return scheduledTask.BioosInfo != nil && scheduledTask.BioosInfo.AccountID == task.BioosInfo.AccountID && scheduledTask.BioosInfo.UserID == task.BioosInfo.UserID
			}); err != nil {
				return fmt.Errorf("user[%s/%s] quota: %w", task.BioosInfo.AccountID, task.BioosInfo.UserID, err)
			}
		}
	}
	return nil
}

func checkQuota(quota *schemodels.ResourceQuota, task *schemodels.TaskInfo, scheduledTasks []*schemodels.TaskInfo, filter func(scheduledTask *schemodels.TaskInfo) bool) error {
	if quota == nil {
		return nil
	}

	var totalCount = 0
	var totalCPUCores = 0
	var totalRamGB float64 = 0
	var totalDiskGB float64 = 0
	var totalGPUCount float64 = 0
	totalGPU := make(map[string]float64)

	for _, scheduled := range scheduledTasks {
		if !filter(scheduled) {
			continue
		}
		totalCount++
		if scheduled.Resources == nil {
			continue
		}
		totalCPUCores += scheduled.Resources.CPUCores
		totalRamGB += scheduled.Resources.RamGB
		totalDiskGB += scheduled.Resources.DiskGB
		if scheduled.Resources.GPU == nil {
			continue
		}
		totalGPUCount += scheduled.Resources.GPU.Count
		totalGPU[scheduled.Resources.GPU.Type] += scheduled.Resources.GPU.Count
	}

	var errs []error
	if quota.Count != nil && *quota.Count < totalCount+1 {
		errs = append(errs, fmt.Errorf("count should no more than %d, occupied %d", *quota.Count, totalCount))
	}
	if task.Resources != nil {
		if quota.CPUCores != nil && task.Resources.CPUCores > 0 && *quota.CPUCores < totalCPUCores+task.Resources.CPUCores {
			errs = append(errs, fmt.Errorf("CPUCores should no more than %d, occupied %d, claimed %d", *quota.CPUCores, totalCPUCores, task.Resources.CPUCores))
		}
		if quota.RamGB != nil && task.Resources.RamGB > 0 && *quota.RamGB < totalRamGB+task.Resources.RamGB {
			errs = append(errs, fmt.Errorf("RamGB should no more than %.2f, occupied %.2f, claimed %.2f", *quota.RamGB, totalRamGB, task.Resources.RamGB))
		}
		if quota.DiskGB != nil && task.Resources.DiskGB > 0 && *quota.DiskGB < totalDiskGB+task.Resources.DiskGB {
			errs = append(errs, fmt.Errorf("DiskGB should no more than %.2f, occupied %.2f, claimed %.2f", *quota.DiskGB, totalDiskGB, task.Resources.DiskGB))
		}
		if quota.GPUQuota != nil && task.Resources.GPU != nil {
			if task.Resources.GPU.Type != "" {
				// only check this type of GPU count quota
				gpuType := task.Resources.GPU.Type
				gpuCountQuota, ok := quota.GPUQuota.GPU[gpuType]
				if !ok {
					errs = append(errs, fmt.Errorf("no match GPUType: %s", gpuType))
				} else if gpuCountQuota < totalGPU[gpuType]+task.Resources.GPU.Count {
					errs = append(errs, fmt.Errorf("GPUCount no more no more than %.2f, occupied %.2f, claimed %.2f", gpuCountQuota, totalGPU[gpuType], task.Resources.GPU.Count))
				}
			} else {
				// check total GPU count quota
				var sumGPUCountQuota float64 = 0
				for _, gpuCount := range quota.GPUQuota.GPU {
					sumGPUCountQuota += gpuCount
				}
				if sumGPUCountQuota < totalGPUCount+task.Resources.GPU.Count {
					errs = append(errs, fmt.Errorf("GPUCount should no more than %.2f, occupied %.2f, claimed %.2f", sumGPUCountQuota, totalGPUCount, task.Resources.GPU.Count))
				}
			}
		}
	}
	return utilerrors.NewAggregate(errs)
}
