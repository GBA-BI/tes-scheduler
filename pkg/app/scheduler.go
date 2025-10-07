package app

import (
	"fmt"

	applog "github.com/GBA-BI/tes-scheduler/pkg/log"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	genericapiserver "k8s.io/apiserver/pkg/server"

	"github.com/GBA-BI/tes-scheduler/pkg/app/options"
	"github.com/GBA-BI/tes-scheduler/pkg/consts"
	"github.com/GBA-BI/tes-scheduler/pkg/leaderelection"
	"github.com/GBA-BI/tes-scheduler/pkg/scheduler"
	"github.com/GBA-BI/tes-scheduler/pkg/server"
	"github.com/GBA-BI/tes-scheduler/pkg/version"
	"github.com/GBA-BI/tes-scheduler/pkg/vetesclient"
	"github.com/GBA-BI/tes-scheduler/pkg/viper"
)

func newSchedulerCommand(opts *options.Options) *cobra.Command {
	return &cobra.Command{
		Use:          consts.Component,
		Short:        "veTES scheduler",
		Long:         "veTES scheduler",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			version.PrintVersionOrContinue()
			if err := opts.Validate(); err != nil {
				return err
			}

			applog.RegisterLogger(opts.Log)
			defer applog.Sync()

			cmd.Flags().VisitAll(func(flag *pflag.Flag) {
				applog.Infow("FLAG", flag.Name, flag.Value)
			})

			return run(opts)
		},
	}
}

func run(opts *options.Options) error {
	applog.Infow("run veTES scheduler")
	ctx := genericapiserver.SetupSignalContext()

	sche, err := scheduler.NewScheduler(opts.Scheduler, vetesclient.NewClient(opts.VeTESClient))
	if err != nil {
		return err
	}

	if err = leaderelection.Init(opts.LeaderElection); err != nil {
		return err
	}

	go server.Run(opts.Server)

	leaderelection.Run(ctx, sche.Run)

	return fmt.Errorf("unexpected finished")
}

// NewSchedulerCommand create a veTES scheduler command.
func NewSchedulerCommand() (*cobra.Command, error) {
	opts := options.NewOptions()
	cmd := newSchedulerCommand(opts)

	opts.AddFlags(cmd.Flags())
	version.AddFlags(cmd.Flags())
	cmd.Flags().AddFlag(pflag.Lookup(viper.ConfigFlagName))
	if err := viper.LoadConfig(opts); err != nil {
		return nil, err
	}
	return cmd, nil
}
