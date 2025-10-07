package models

import "time"

// TaskInfo ...
type TaskInfo struct {
	ID            string
	State         string
	ClusterID     string
	CreationTime  time.Time
	Resources     *Resources
	BioosInfo     *BioosInfo
	PriorityValue int
}

// Resources ...
type Resources struct {
	CPUCores int
	RamGB    float64 // nolint
	DiskGB   float64
	GPU      *GPUResource
}

// GPUResource ...
type GPUResource struct {
	Count float64
	Type  string
}

// BioosInfo ...
type BioosInfo struct {
	AccountID    string
	UserID       string
	SubmissionID string
	RunID        string
}
