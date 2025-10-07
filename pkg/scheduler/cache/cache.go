package cache

import "github.com/GBA-BI/tes-scheduler/pkg/vetesclient"

// Cache ...
type Cache struct {
	ClusterCache       ClusterCache
	TaskCache          TaskCache
	ExtraPriorityCache ExtraPriorityCache
	QuotaCache         QuotaCache
}

// NewCache ...
func NewCache(vetesClient vetesclient.Client, opts *Options) (*Cache, error) {
	clusterCache, err := NewClusterCache(vetesClient, opts)
	if err != nil {
		return nil, err
	}
	taskCache, err := NewTaskCache(vetesClient, opts)
	if err != nil {
		return nil, err
	}
	extraPriorityCache, err := NewExtraPriorityCache(vetesClient, opts)
	if err != nil {
		return nil, err
	}
	quotaCache := NewQuotaCache(vetesClient, opts)
	return &Cache{ClusterCache: clusterCache,
		TaskCache:          taskCache,
		ExtraPriorityCache: extraPriorityCache,
		QuotaCache:         quotaCache,
	}, nil
}
