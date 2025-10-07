package controller

import (
	"fmt"
	"time"

	"github.com/spf13/pflag"
)

// Options ...
type Options struct {
	Period                   time.Duration `mapstructure:"period"`
	ClusterRescheduleTimeout time.Duration `mapstructure:"clusterRescheduleTimeout"`
}

// NewOptions ...
func NewOptions() *Options {
	return &Options{
		Period:                   time.Second * 10,
		ClusterRescheduleTimeout: time.Minute * 20,
	}
}

// Validate ...
func (o *Options) Validate() error {
	if o.ClusterRescheduleTimeout < o.Period {
		return fmt.Errorf("clusterRescheduleTimeout must be greater than period")
	}
	return nil
}

// AddFlags ...
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	fs.DurationVar(&o.Period, "controller-period", o.Period, "controller run period")
	fs.DurationVar(&o.ClusterRescheduleTimeout, "controller-cluster-reschedule-timeout", o.ClusterRescheduleTimeout, "cluster reschedule timeout")
}
