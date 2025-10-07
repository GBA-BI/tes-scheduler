package cache

import (
	"context"
	"sync"
	"time"

	"github.com/GBA-BI/tes-scheduler/pkg/log"

	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/crontab"
	schemodels "github.com/GBA-BI/tes-scheduler/pkg/scheduler/models"
	"github.com/GBA-BI/tes-scheduler/pkg/vetesclient"
	clientmodels "github.com/GBA-BI/tes-scheduler/pkg/vetesclient/models"
)

// ClusterCache caches cluster info
type ClusterCache interface {
	ListClusters() []*schemodels.ClusterInfo
}

// clusterCacheImpl ...
type clusterCacheImpl struct {
	vetesClient vetesclient.Client

	mutex    sync.RWMutex
	clusters []*schemodels.ClusterInfo
}

var _ ClusterCache = (*clusterCacheImpl)(nil)

// NewClusterCache ...
func NewClusterCache(vetesClient vetesclient.Client, opts *Options) (ClusterCache, error) {
	cache := &clusterCacheImpl{
		vetesClient: vetesClient,
	}
	if err := cache.syncClusters(context.Background()); err != nil {
		return nil, err
	}
	if err := crontab.RegisterCron(opts.SyncPeriod, func() {
		ctx := context.Background()
		if err := cache.syncClusters(ctx); err != nil {
			log.CtxErrorw(ctx, "failed to sync clusters", "err", err)
		}
	}); err != nil {
		return nil, err
	}
	return cache, nil
}

// ListClusters ...
func (i *clusterCacheImpl) ListClusters() []*schemodels.ClusterInfo {
	i.mutex.RLock()
	defer i.mutex.RUnlock()
	return i.clusters
}

func (i *clusterCacheImpl) syncClusters(ctx context.Context) error {
	clusters := make([]*schemodels.ClusterInfo, 0, len(i.clusters))
	resp, err := i.vetesClient.ListClusters(ctx, &clientmodels.ListClustersRequest{})
	if err != nil {
		return err
	}
	for _, cluster := range *resp {
		clusters = append(clusters, clientClusterToClusterInfo(ctx, cluster))
	}

	i.mutex.Lock()
	defer i.mutex.Unlock()
	i.clusters = clusters
	return nil
}

func clientClusterToClusterInfo(ctx context.Context, cluster *clientmodels.Cluster) *schemodels.ClusterInfo {
	if cluster == nil {
		return nil
	}
	res := &schemodels.ClusterInfo{
		ID:       cluster.ID,
		Capacity: clientClusterCapacityToClusterInfoCapacity(cluster.Capacity),
		Limits:   clientClusterLimitsToClusterInfoLimits(cluster.Limits),
	}
	if cluster.HeartbeatTimestamp != "" {
		var err error
		if res.HeartbeatTimestamp, err = time.Parse(time.RFC3339, cluster.HeartbeatTimestamp); err != nil {
			log.CtxErrorw(ctx, "parse heartbeat timestamp of cluster", "cluster", cluster.ID, "err", err)
		}
	}
	return res
}

func clientClusterCapacityToClusterInfoCapacity(capacity *clientmodels.Capacity) *schemodels.Capacity {
	if capacity == nil {
		return nil
	}
	res := &schemodels.Capacity{
		Count:    capacity.Count,
		CPUCores: capacity.CPUCores,
		RamGB:    capacity.RamGB,
		DiskGB:   capacity.DiskGB,
	}
	if capacity.GPUCapacity != nil {
		res.GPUCapacity = &schemodels.GPUCapacity{GPU: capacity.GPUCapacity.GPU}
	}
	return res
}

func clientClusterLimitsToClusterInfoLimits(limits *clientmodels.Limits) *schemodels.Limits {
	if limits == nil {
		return nil
	}
	res := &schemodels.Limits{
		CPUCores: limits.CPUCores,
		RamGB:    limits.RamGB,
	}
	if limits.GPULimit != nil {
		res.GPULimit = &schemodels.GPULimit{GPU: limits.GPULimit.GPU}
	}
	return res
}
