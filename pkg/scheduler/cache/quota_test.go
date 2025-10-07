package cache

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/coocood/freecache"
	"github.com/golang/mock/gomock"
	"github.com/onsi/gomega"

	schemodels "github.com/GBA-BI/tes-scheduler/pkg/scheduler/models"
	"github.com/GBA-BI/tes-scheduler/pkg/utils"
	"github.com/GBA-BI/tes-scheduler/pkg/vetesclient"
	vetesclientfake "github.com/GBA-BI/tes-scheduler/pkg/vetesclient/fake"
	clientmodels "github.com/GBA-BI/tes-scheduler/pkg/vetesclient/models"
)

var clientResourceQuota = &clientmodels.ResourceQuota{
	Count:    utils.Point(10),
	CPUCores: utils.Point(10),
	RamGB:    utils.Point[float64](100),
	DiskGB:   utils.Point[float64](1000),
	GPUQuota: &clientmodels.GPUQuota{GPU: map[string]float64{"type-01": 5}},
}

var scheResourceQuota = &schemodels.ResourceQuota{
	Count:    utils.Point(10),
	CPUCores: utils.Point(10),
	RamGB:    utils.Point[float64](100),
	DiskGB:   utils.Point[float64](1000),
	GPUQuota: &schemodels.GPUQuota{GPU: map[string]float64{"type-01": 5}},
}

func TestGetGlobalQuotaDirect(t *testing.T) {
	g := gomega.NewWithT(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fakeVeTESClient := vetesclientfake.NewFakeClient(ctrl)
	fakeVeTESClient.EXPECT().
		GetQuota(gomock.Any(), &clientmodels.GetQuotaRequest{Global: true}).
		Return(&clientmodels.GetQuotaResponse{
			Global:        true,
			ResourceQuota: clientResourceQuota,
		}, nil)

	i := &quotaCacheImpl{
		vetesClient:  fakeVeTESClient,
		expireSecond: 15,
		quotaCache:   freecache.NewCache(quotaCacheSize),
	}
	resp, err := i.GetGlobalQuota(context.Background())
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(resp).To(gomega.BeEquivalentTo(scheResourceQuota))

	cached, err := i.quotaCache.Get(quotaCacheKey(true, "", ""))
	g.Expect(err).NotTo(gomega.HaveOccurred())
	expCached, _ := json.Marshal(scheResourceQuota)
	g.Expect(cached).To(gomega.BeEquivalentTo(expCached))
}

func TestGetGlobalQuotaDirectNotFound(t *testing.T) {
	g := gomega.NewWithT(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fakeVeTESClient := vetesclientfake.NewFakeClient(ctrl)
	fakeVeTESClient.EXPECT().
		GetQuota(gomock.Any(), &clientmodels.GetQuotaRequest{Global: true}).
		Return(nil, vetesclient.ErrNotFound)

	i := &quotaCacheImpl{
		vetesClient:  fakeVeTESClient,
		expireSecond: 15,
		quotaCache:   freecache.NewCache(quotaCacheSize),
	}
	resp, err := i.GetGlobalQuota(context.Background())
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(resp).To(gomega.BeNil())

	cached, err := i.quotaCache.Get(quotaCacheKey(true, "", ""))
	g.Expect(errors.Is(err, freecache.ErrNotFound)).To(gomega.BeTrue())
	g.Expect(cached).To(gomega.BeNil())
}

func TestGetGlobalQuotaFromCache(t *testing.T) {
	g := gomega.NewWithT(t)

	i := &quotaCacheImpl{
		quotaCache: freecache.NewCache(quotaCacheSize),
	}
	cached, _ := json.Marshal(scheResourceQuota)
	_ = i.quotaCache.Set(quotaCacheKey(true, "", ""), cached, 15)

	resp, err := i.GetGlobalQuota(context.Background())
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(resp).To(gomega.BeEquivalentTo(scheResourceQuota))
}

func TestGetAccountQuotaFromCache(t *testing.T) {
	g := gomega.NewWithT(t)

	i := &quotaCacheImpl{
		quotaCache: freecache.NewCache(quotaCacheSize),
	}
	cached, _ := json.Marshal(scheResourceQuota)
	_ = i.quotaCache.Set(quotaCacheKey(false, "account-01", ""), cached, 15)

	resp, err := i.GetAccountQuota(context.Background(), "account-01")
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(resp).To(gomega.BeEquivalentTo(scheResourceQuota))
}

func TestGetUserQuotaFromCache(t *testing.T) {
	g := gomega.NewWithT(t)

	i := &quotaCacheImpl{
		quotaCache: freecache.NewCache(quotaCacheSize),
	}
	cached, _ := json.Marshal(scheResourceQuota)
	_ = i.quotaCache.Set(quotaCacheKey(false, "account-01", "user-01"), cached, 15)

	resp, err := i.GetUserQuota(context.Background(), "account-01", "user-01")
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(resp).To(gomega.BeEquivalentTo(scheResourceQuota))
}
