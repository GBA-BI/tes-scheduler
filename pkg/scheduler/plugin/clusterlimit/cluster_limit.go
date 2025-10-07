package clusterlimit

import (
	"context"

	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/cache"
	schemodels "github.com/GBA-BI/tes-scheduler/pkg/scheduler/models"
	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/plugin"
	"github.com/GBA-BI/tes-scheduler/pkg/utils"
)

// Name is the plugin name
const Name = "ClusterLimit"

type impl struct {
	cache *cache.Cache
}

var _ plugin.FilterPlugin = (*impl)(nil)

// New ...
func New(_ interface{}, cache *cache.Cache) (plugin.Plugin, error) {
	return &impl{cache: cache}, nil
}

// Name ...
func (i *impl) Name() string {
	return Name
}

// Filter ...
func (i *impl) Filter(_ context.Context, task *schemodels.TaskInfo, cluster *schemodels.ClusterInfo, _ map[string]interface{}) error {
	return utils.ResourcesMeetLimits(task.Resources, cluster.Limits)
}
