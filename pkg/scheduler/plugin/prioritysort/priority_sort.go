package prioritysort

import (
	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/cache"
	schemodels "github.com/GBA-BI/tes-scheduler/pkg/scheduler/models"
	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/plugin"
)

// Name is the plugin name
const Name = "PrioritySort"

type impl struct {
	cache *cache.Cache
}

var _ plugin.SortPlugin = (*impl)(nil)

// New ...
func New(_ interface{}, cache *cache.Cache) (plugin.Plugin, error) {
	return &impl{cache: cache}, nil
}

// Name ...
func (i *impl) Name() string {
	return Name
}

// Less ...
func (i *impl) Less(taskI *schemodels.TaskInfo, taskJ *schemodels.TaskInfo) bool {
	extraPriorities := i.cache.ExtraPriorityCache.ListExtraPriorities()
	valueI := taskI.PriorityValue
	valueJ := taskJ.PriorityValue
	for _, ep := range extraPriorities {
		if ep.MatchTask(taskI) {
			valueI += ep.ExtraPriorityValue
		}
		if ep.MatchTask(taskJ) {
			valueJ += ep.ExtraPriorityValue
		}
	}
	if valueI == valueJ {
		return taskI.CreationTime.Before(taskJ.CreationTime)
	}
	return valueI > valueJ
}
