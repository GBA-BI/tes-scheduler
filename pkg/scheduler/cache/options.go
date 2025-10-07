package cache

import (
	"time"

	"github.com/spf13/pflag"
)

// Options ...
type Options struct {
	SyncPeriod time.Duration `mapstructure:"syncPeriod"`
}

// NewOptions ...
func NewOptions() *Options {
	return &Options{
		SyncPeriod: time.Second * 10,
	}
}

// Validate ...
func (o *Options) Validate() error {
	return nil
}

// AddFlags ...
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	fs.DurationVar(&o.SyncPeriod, "scheduler-cache-sync-period", o.SyncPeriod, "sync period of cache resources")
}
