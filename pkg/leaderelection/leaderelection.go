package leaderelection

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	applog "github.com/GBA-BI/tes-scheduler/pkg/log"
	"github.com/google/uuid"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	"github.com/GBA-BI/tes-scheduler/pkg/consts"
	"github.com/GBA-BI/tes-scheduler/pkg/healthz"
)

const (
	lockType       = resourcelock.LeasesResourceLock
	leaseDuration  = 15 * time.Second
	renewDeadline  = 10 * time.Second
	retryPeriod    = 2 * time.Second
	healthzTimeout = 20 * time.Second
)

var once sync.Once
var cfg *leaderelection.LeaderElectionConfig

// Init ...
func Init(opts *Options) error {
	if opts == nil || !opts.Enable {
		return nil
	}

	var err error
	once.Do(func() {
		if cfg, err = newLeaderElectionConfig(opts); err != nil {
			return
		}
		healthz.RegisterChecker(cfg.WatchDog)
	})
	return err
}

// Run ...
func Run(ctx context.Context, fn func(ctx context.Context)) {
	if cfg == nil {
		fn(ctx)
		return
	}

	cfg.Callbacks = leaderelection.LeaderCallbacks{
		OnStartedLeading: func(ctx context.Context) {
			fn(ctx)
		},
		OnStoppedLeading: func() {
			select {
			case <-ctx.Done():
				// We were asked to terminate. Exit 0
				applog.CtxInfow(ctx, "Requested to terminate, exiting")
				os.Exit(0)
			default:
				// We lost the lock.
				applog.CtxErrorw(ctx, "leaderelection lost")
				applog.Sync()
				os.Exit(1)
			}
		},
	}

	leaderelection.RunOrDie(ctx, *cfg)
}

func newLeaderElectionConfig(opts *Options) (*leaderelection.LeaderElectionConfig, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("failed to get hostname: %w", err)
	}
	id := hostname + "_" + uuid.NewString()

	kubeConfig, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get in cluster kubeconfig: %w", err)
	}

	rl, err := resourcelock.NewFromKubeconfig(lockType, opts.Namespace, opts.Name,
		resourcelock.ResourceLockConfig{
			Identity: id,
		}, kubeConfig, renewDeadline)
	if err != nil {
		return nil, fmt.Errorf("failed to  create leader election config: %w", err)
	}

	return &leaderelection.LeaderElectionConfig{
		Lock:            rl,
		LeaseDuration:   leaseDuration,
		RenewDeadline:   renewDeadline,
		RetryPeriod:     retryPeriod,
		WatchDog:        leaderelection.NewLeaderHealthzAdaptor(healthzTimeout),
		ReleaseOnCancel: true,
		Name:            consts.Component,
	}, nil
}
