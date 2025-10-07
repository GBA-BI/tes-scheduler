package cache

import (
	"context"
	"sync"

	"github.com/GBA-BI/tes-scheduler/pkg/log"

	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/crontab"
	schemodels "github.com/GBA-BI/tes-scheduler/pkg/scheduler/models"
	"github.com/GBA-BI/tes-scheduler/pkg/vetesclient"
	clientmodels "github.com/GBA-BI/tes-scheduler/pkg/vetesclient/models"
)

// ExtraPriorityCache ...
type ExtraPriorityCache interface {
	ListExtraPriorities() []*schemodels.ExtraPriorityInfo
}

// extraPriorityCacheImpl ...
type extraPriorityCacheImpl struct {
	vetesClient vetesclient.Client

	mutex           sync.RWMutex
	extraPriorities []*schemodels.ExtraPriorityInfo
}

var _ ExtraPriorityCache = (*extraPriorityCacheImpl)(nil)

// NewExtraPriorityCache ...
func NewExtraPriorityCache(vetesClient vetesclient.Client, opts *Options) (ExtraPriorityCache, error) {
	cache := &extraPriorityCacheImpl{
		vetesClient: vetesClient,
	}
	if err := cache.syncExtraPriorities(context.Background()); err != nil {
		return nil, err
	}
	if err := crontab.RegisterCron(opts.SyncPeriod, func() {
		ctx := context.Background()
		if err := cache.syncExtraPriorities(ctx); err != nil {
			log.CtxErrorw(ctx, "failed to sync extraPriorities", "err", err)
		}
	}); err != nil {
		return nil, err
	}
	return cache, nil
}

// ListExtraPriorities ...
func (i *extraPriorityCacheImpl) ListExtraPriorities() []*schemodels.ExtraPriorityInfo {
	i.mutex.RLock()
	defer i.mutex.RUnlock()
	return i.extraPriorities
}

func (i *extraPriorityCacheImpl) syncExtraPriorities(ctx context.Context) error {
	extraPriorities := make([]*schemodels.ExtraPriorityInfo, 0, len(i.extraPriorities))
	resp, err := i.vetesClient.ListExtraPriority(ctx, &clientmodels.ListExtraPriorityRequest{})
	if err != nil {
		return err
	}
	for _, extraPriority := range *resp {
		extraPriorities = append(extraPriorities, clientExtraPriorityToExtraPriorityInfo(extraPriority))
	}

	i.mutex.Lock()
	defer i.mutex.Unlock()
	i.extraPriorities = extraPriorities
	return nil
}

func clientExtraPriorityToExtraPriorityInfo(extraPriority *clientmodels.ExtraPriority) *schemodels.ExtraPriorityInfo {
	if extraPriority == nil {
		return nil
	}
	return &schemodels.ExtraPriorityInfo{
		AccountID:          extraPriority.AccountID,
		UserID:             extraPriority.UserID,
		SubmissionID:       extraPriority.SubmissionID,
		RunID:              extraPriority.RunID,
		ExtraPriorityValue: extraPriority.ExtraPriorityValue,
	}
}
