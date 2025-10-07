package utils

import (
	"fmt"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"

	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/models"
)

// ResourcesMeetLimits ...
func ResourcesMeetLimits(resources *models.Resources, limits *models.Limits) error {
	if resources == nil || limits == nil {
		return nil
	}

	var errs []error

	if limits.CPUCores != nil && resources.CPUCores > *limits.CPUCores {
		errs = append(errs, fmt.Errorf("CPUCore should no more than %d", *limits.CPUCores))
	}
	if limits.RamGB != nil && resources.RamGB > *limits.RamGB {
		errs = append(errs, fmt.Errorf("RamGB should no more than %.2f", *limits.RamGB))
	}

	if resources.GPU != nil && limits.GPULimit != nil {
		if resources.GPU.Type == "" {
			existProperGPUType := false
			for _, gpuCount := range limits.GPULimit.GPU {
				if resources.GPU.Count <= gpuCount {
					existProperGPUType = true
					break
				}
			}
			if !existProperGPUType {
				errs = append(errs, fmt.Errorf("GPUCount should less than %+v", limits.GPULimit.GPU))
			}
		} else {
			gpuType := resources.GPU.Type
			gpuCount, ok := limits.GPULimit.GPU[gpuType]
			if !ok {
				errs = append(errs, fmt.Errorf("no match GPUType %s", gpuType))
			} else if resources.GPU.Count > gpuCount {
				errs = append(errs, fmt.Errorf("GPUCount of GPUType %s should no more than %.2f", gpuType, gpuCount))
			}
		}
	}
	return utilerrors.NewAggregate(errs)
}
