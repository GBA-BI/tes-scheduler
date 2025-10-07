package models

import "time"

// ClusterInfo ...
type ClusterInfo struct {
	ID                 string
	HeartbeatTimestamp time.Time
	Capacity           *Capacity
	Limits             *Limits
}

// Capacity ...
type Capacity struct {
	Count       *int
	CPUCores    *int
	RamGB       *float64 // nolint
	DiskGB      *float64
	GPUCapacity *GPUCapacity
}

// GPUCapacity ...
type GPUCapacity struct {
	GPU map[string]float64
}

// Limits ...
type Limits struct {
	CPUCores *int
	RamGB    *float64 // nolint
	GPULimit *GPULimit
}

// GPULimit ...
type GPULimit struct {
	GPU map[string]float64
}
