package scheduler

import (
	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/plugin"
	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/plugin/clustercapacity"
	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/plugin/clusterlimit"
	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/plugin/prioritysort"
	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/plugin/resourcequota"
)

var registry = map[string]plugin.Factory{
	prioritysort.Name:    prioritysort.New,
	resourcequota.Name:   resourcequota.New,
	clusterlimit.Name:    clusterlimit.New,
	clustercapacity.Name: clustercapacity.New,
}

// extractPluginConfig extract config of different plugin.
// todo: I don't know how to use viper to set config of different plugin
func extractPluginConfig(_ *Options, _ string) interface{} {
	return nil
}
