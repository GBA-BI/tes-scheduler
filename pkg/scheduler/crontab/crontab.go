package crontab

import (
	"context"
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

var crontab = cron.New()

// RegisterCron ...
func RegisterCron(period time.Duration, fn func()) error {
	runningFlag := false
	if _, err := crontab.AddFunc(fmt.Sprintf("@every %s", period.String()), func() {
		if runningFlag {
			return
		}
		runningFlag = true
		fn()
		runningFlag = false
	}); err != nil {
		return err
	}
	return nil
}

// Start ...
func Start() {
	crontab.Start()
}

// Stop ...
func Stop() context.Context {
	return crontab.Stop()
}
