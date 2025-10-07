package models

// GetQuotaRequest ...
type GetQuotaRequest struct {
	Global    bool   `query:"global"`
	AccountID string `query:"account_id"`
	UserID    string `query:"user_id"`
}

// GetQuotaResponse ...
type GetQuotaResponse struct {
	Global        bool           `json:"global"`
	Default       bool           `json:"default"`
	AccountID     string         `json:"account_id,omitempty"`
	UserID        string         `json:"user_id,omitempty"`
	ResourceQuota *ResourceQuota `json:"resource_quota,omitempty"`
}

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
