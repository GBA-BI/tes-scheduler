package leaderelection

import (
	"fmt"

	"github.com/spf13/pflag"
)

// Options ...
type Options struct {
	Enable    bool   `mapstructure:"enable"`
	Namespace string `mapstructure:"namespace"`
	Name      string `mapstructure:"name"`
}

// NewOptions ...
func NewOptions() *Options {
	return &Options{
		Enable:    true,
		Namespace: "vetes-system",
		Name:      "vetes-scheduler",
	}
}

// Validate ...
func (o *Options) Validate() error {
	if o.Enable == false {
		return nil
	}
	if o.Namespace == "" || o.Name == "" {
		return fmt.Errorf("namespace and name must be specified")
	}
	return nil
}

// AddFlags ...
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	fs.BoolVar(&o.Enable, "leader-election", o.Enable, "enable leader election")
	fs.StringVar(&o.Namespace, "leader-election-namespace", o.Namespace, "namespace of the leader election")
	fs.StringVar(&o.Name, "leader-election-name", o.Name, "name of the leader election")
}
