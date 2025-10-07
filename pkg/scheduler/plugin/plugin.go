package plugin

import (
	"context"

	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/cache"
	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/models"
)

// Factory ...
type Factory func(pluginConfig interface{}, cache *cache.Cache) (Plugin, error)

// Plugin ...
type Plugin interface {
	Name() string
}

// SortPlugin ...
type SortPlugin interface {
	Plugin
	// Less are used to sort tasks in the scheduling queue
	Less(taskI *models.TaskInfo, taskJ *models.TaskInfo) bool
}

// GlobalFilterPlugin ...
type GlobalFilterPlugin interface {
	Plugin
	// GlobalFilter carries out before Filter. It checks out global filter logic.
	GlobalFilter(ctx context.Context, task *models.TaskInfo, cycleState map[string]interface{}) error
}

// FilterPlugin ...
type FilterPlugin interface {
	Plugin
	// Filter checks out each cluster.
	Filter(ctx context.Context, task *models.TaskInfo, cluster *models.ClusterInfo, cycleState map[string]interface{}) error
}

// ScorePlugin ...
type ScorePlugin interface {
	Plugin
	Score(ctx context.Context, task *models.TaskInfo, cluster *models.ClusterInfo, cycleState map[string]interface{}) int64
}

const (
	MaxScore int64 = 100
	MinScore int64 = 0
)
