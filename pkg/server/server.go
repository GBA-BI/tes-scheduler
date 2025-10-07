package server

import (
	"fmt"
	"net/http"

	applog "github.com/GBA-BI/tes-scheduler/pkg/log"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/GBA-BI/tes-scheduler/pkg/healthz"
)

// Run ...
func Run(opts *Options) {
	http.HandleFunc(opts.HealthzPath, healthz.Handler)
	http.Handle(opts.MetricsPath, promhttp.Handler())
	if err := http.ListenAndServe(fmt.Sprintf(":%d", opts.Port), nil); err != nil {
		applog.Fatalw("Failed to start HTTP server", "err", err)
	}
}
