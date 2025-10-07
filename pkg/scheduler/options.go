package scheduler

import (
	"fmt"
	"time"

	"github.com/spf13/pflag"

	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/cache"
	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/controller"
	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/plugin/clustercapacity"
	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/plugin/clusterlimit"
	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/plugin/prioritysort"
	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/plugin/resourcequota"
)

// Options ...
type Options struct {
	Plugins []string `mapstructure:"plugins"`

	SchedulePeriod         time.Duration `mapstructure:"schedulePeriod"`
	ClusterNotReadyTimeout time.Duration `mapstructure:"clusterNotReadyTimeout"`

	Cache      *cache.Options      `mapstructure:"cache"`
	Controller *controller.Options `mapstructure:"controller"`
}

// NewOptions ...
func NewOptions() *Options {
	return &Options{
		Plugins: []string{
			clustercapacity.Name,
			clusterlimit.Name,
			prioritysort.Name,
			resourcequota.Name,
		},

		SchedulePeriod:         time.Second * 10,
		ClusterNotReadyTimeout: time.Minute * 5,

		Cache:      cache.NewOptions(),
		Controller: controller.NewOptions(),
	}
}

// Validate ...
func (o *Options) Validate() error {
	if err := o.Cache.Validate(); err != nil {
		return nil
	}
	if err := o.Controller.Validate(); err != nil {
		return err
	}
	if o.Controller.ClusterRescheduleTimeout < o.Cache.SyncPeriod {
		return fmt.Errorf("controller cluster rescheduling timeout must be greater than cache sync period")
	}
	if o.SchedulePeriod < o.Cache.SyncPeriod {
		return fmt.Errorf("schedule period must be greater than cache sync period")
	}
	if o.ClusterNotReadyTimeout < o.Cache.SyncPeriod {
		return fmt.Errorf("cluster not ready timeout must be greater than cache sync period")
	}
	return nil
}

// AddFlags ...
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	fs.StringSliceVar(&o.Plugins, "scheduler-plugins", o.Plugins, "comma-separated list of scheduler plugins to enable")
	fs.DurationVar(&o.SchedulePeriod, "scheduler-schedule-period", o.SchedulePeriod, "scheduler schedule period")
	fs.DurationVar(&o.ClusterNotReadyTimeout, "scheduler-cluster-not-ready-timeout", o.ClusterNotReadyTimeout, "timeout for cluster not ready")
	o.Cache.AddFlags(fs)
	o.Controller.AddFlags(fs)
}
