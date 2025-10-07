package server

import (
	"fmt"

	"github.com/spf13/pflag"
)

// Options ...
type Options struct {
	Port        uint16 `mapstructure:"port"`
	HealthzPath string `mapstructure:"healthzPath"`
	MetricsPath string `mapstructure:"metricsPath"`
}

// NewOptions ...
func NewOptions() *Options {
	return &Options{
		Port:        8080,
		HealthzPath: "/healthz",
		MetricsPath: "/metrics",
	}
}

// Validate ...
func (o *Options) Validate() error {
	if o.HealthzPath == "" {
		return fmt.Errorf("healthz path cannot be empty")
	}
	if o.MetricsPath == "" {
		return fmt.Errorf("metrics path cannot be empty")
	}
	if o.HealthzPath == o.MetricsPath {
		return fmt.Errorf("healthz and metrics path cannot be the same")
	}
	return nil
}

// AddFlags ...
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	fs.Uint16Var(&o.Port, "http-port", o.Port, "http port to listen on")
	fs.StringVar(&o.HealthzPath, "http-healthz-path", o.HealthzPath, "http path to healthz")
	fs.StringVar(&o.MetricsPath, "http-metrics-path", o.MetricsPath, "http path to metrics")
}
