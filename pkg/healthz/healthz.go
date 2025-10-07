package healthz

import (
	"fmt"
	"net/http"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apiserver/pkg/server/healthz"
)

var checkers []healthz.HealthChecker

// RegisterChecker ...
func RegisterChecker(checker healthz.HealthChecker) {
	checkers = append(checkers, checker)
}

// Handler ...
func Handler(w http.ResponseWriter, req *http.Request) {
	var errs []error
	for _, checker := range checkers {
		if err := checker.Check(req); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", checker.Name(), err))
		}
	}

	if len(errs) > 0 {
		http.Error(w, utilerrors.NewAggregate(errs).Error(), http.StatusServiceUnavailable)
	}

	fmt.Fprint(w, "ok")
}
