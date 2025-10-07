package options

import (
	"github.com/GBA-BI/tes-scheduler/pkg/log"
	"github.com/spf13/pflag"

	"github.com/GBA-BI/tes-scheduler/pkg/leaderelection"
	"github.com/GBA-BI/tes-scheduler/pkg/scheduler"
	"github.com/GBA-BI/tes-scheduler/pkg/server"
	"github.com/GBA-BI/tes-scheduler/pkg/vetesclient"
)

// Options ...
type Options struct {
	Log            *log.Options            `mapstructure:"log"`
	LeaderElection *leaderelection.Options `mapstructure:"leaderElection"`
	VeTESClient    *vetesclient.Options    `mapstructure:"vetesClient"`
	Server         *server.Options         `mapstructure:"server"`
	Scheduler      *scheduler.Options      `mapstructure:"scheduler"`
}

// NewOptions ...
func NewOptions() *Options {
	return &Options{
		Log:            log.NewOptions(),
		LeaderElection: leaderelection.NewOptions(),
		VeTESClient:    vetesclient.NewOptions(),
		Server:         server.NewOptions(),
		Scheduler:      scheduler.NewOptions(),
	}
}

// Validate ...
func (o *Options) Validate() error {
	if err := o.Log.Validate(); err != nil {
		return err
	}
	if err := o.LeaderElection.Validate(); err != nil {
		return err
	}
	if err := o.VeTESClient.Validate(); err != nil {
		return err
	}
	if err := o.Server.Validate(); err != nil {
		return err
	}
	if err := o.Scheduler.Validate(); err != nil {
		return err
	}
	return nil
}

// AddFlags ...
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	o.Log.AddFlags(fs)
	o.LeaderElection.AddFlags(fs)
	o.VeTESClient.AddFlags(fs)
	o.Server.AddFlags(fs)
	o.Scheduler.AddFlags(fs)
}
