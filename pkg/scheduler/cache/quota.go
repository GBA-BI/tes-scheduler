package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/GBA-BI/tes-scheduler/pkg/log"
	"github.com/coocood/freecache"

	schemodels "github.com/GBA-BI/tes-scheduler/pkg/scheduler/models"
	"github.com/GBA-BI/tes-scheduler/pkg/vetesclient"
	clientmodels "github.com/GBA-BI/tes-scheduler/pkg/vetesclient/models"
)

const quotaCacheSize = 128 * 1024 // 128KiB

// QuotaCache ...
type QuotaCache interface {
	GetGlobalQuota(ctx context.Context) (*schemodels.ResourceQuota, error)
	GetAccountQuota(ctx context.Context, accountID string) (*schemodels.ResourceQuota, error)
	GetUserQuota(ctx context.Context, accountID, userID string) (*schemodels.ResourceQuota, error)
}

// QuotaCacheImpl ...
type quotaCacheImpl struct {
	vetesClient  vetesclient.Client
	expireSecond int
	quotaCache   *freecache.Cache
}

var _ QuotaCache = (*quotaCacheImpl)(nil)

// NewQuotaCache ...
func NewQuotaCache(vetesClient vetesclient.Client, opts *Options) QuotaCache {
	quotaCache := freecache.NewCache(quotaCacheSize)
	return &quotaCacheImpl{
		vetesClient:  vetesClient,
		expireSecond: int(opts.SyncPeriod.Seconds()),
		quotaCache:   quotaCache,
	}
}

// GetGlobalQuota ...
func (i *quotaCacheImpl) GetGlobalQuota(ctx context.Context) (*schemodels.ResourceQuota, error) {
	return i.getQuota(ctx, true, "", "")
}

// GetAccountQuota ...
func (i *quotaCacheImpl) GetAccountQuota(ctx context.Context, accountID string) (*schemodels.ResourceQuota, error) {
	return i.getQuota(ctx, false, accountID, "")
}

// GetUserQuota ...
func (i *quotaCacheImpl) GetUserQuota(ctx context.Context, accountID, userID string) (*schemodels.ResourceQuota, error) {
	return i.getQuota(ctx, false, accountID, userID)
}

func (i *quotaCacheImpl) getQuota(ctx context.Context, global bool, accountID, userID string) (*schemodels.ResourceQuota, error) {
	key := quotaCacheKey(global, accountID, userID)
	cache, err := i.quotaCache.Get(key)
	if err == nil {
		res := &schemodels.ResourceQuota{}
		unmarshalErr := json.Unmarshal(cache, res)
		if unmarshalErr == nil {
			return res, nil
		}
		log.CtxErrorw(ctx, "failed to unmarshal resourceQuota", "err", unmarshalErr)
	}
	if !errors.Is(err, freecache.ErrNotFound) {
		log.CtxErrorw(ctx, "failed to get resourceQuota from cache", "err", err)
	}

	var res *schemodels.ResourceQuota
	defer func() {
		if res == nil {
			return
		}
		toCache, marshalErr := json.Marshal(res)
		if marshalErr != nil {
			log.CtxErrorw(ctx, "failed to marshal resourceQuota", "err", marshalErr)
			return
		}
		if cacheErr := i.quotaCache.Set(key, toCache, i.expireSecond); cacheErr != nil {
			log.CtxErrorw(ctx, "failed to set quota cache", "err", cacheErr)
		}
	}()

	resp, err := i.vetesClient.GetQuota(ctx, &clientmodels.GetQuotaRequest{
		Global:    global,
		AccountID: accountID,
		UserID:    userID,
	})
	if err != nil {
		if errors.Is(err, vetesclient.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	res = clientResourceQuotaToResourceQuotaInfo(resp.ResourceQuota)
	return res, nil
}

func clientResourceQuotaToResourceQuotaInfo(quota *clientmodels.ResourceQuota) *schemodels.ResourceQuota {
	if quota == nil {
		return nil
	}
	res := &schemodels.ResourceQuota{
		Count:    quota.Count,
		CPUCores: quota.CPUCores,
		RamGB:    quota.RamGB,
		DiskGB:   quota.DiskGB,
	}
	if quota.GPUQuota != nil {
		res.GPUQuota = &schemodels.GPUQuota{GPU: quota.GPUQuota.GPU}
	}
	return res
}

func quotaCacheKey(global bool, accountID, userID string) []byte {
	if global {
		return []byte("global")
	}
	return []byte(fmt.Sprintf("%s/%s", accountID, userID))
}
