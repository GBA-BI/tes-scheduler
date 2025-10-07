package models

// ResourceQuota ...
type ResourceQuota struct {
	Count    *int      `json:"count,omitempty"`
	CPUCores *int      `json:"cpu_cores,omitempty"`
	RamGB    *float64  `json:"ram_gb,omitempty"` // nolint
	DiskGB   *float64  `json:"disk_gb,omitempty"`
	GPUQuota *GPUQuota `json:"gpu_quota,omitempty"`
}

// GPUQuota ...
type GPUQuota struct {
	GPU map[string]float64 `json:"gpu,omitempty"`
}
