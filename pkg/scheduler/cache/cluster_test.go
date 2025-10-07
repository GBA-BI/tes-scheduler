package cache

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/onsi/gomega"

	schemodels "github.com/GBA-BI/tes-scheduler/pkg/scheduler/models"
	"github.com/GBA-BI/tes-scheduler/pkg/utils"
	vetesclientfake "github.com/GBA-BI/tes-scheduler/pkg/vetesclient/fake"
	clientmodels "github.com/GBA-BI/tes-scheduler/pkg/vetesclient/models"
)

func TestSyncClusters(t *testing.T) {
	g := gomega.NewWithT(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	now := time.Now().UTC().Truncate(time.Second)

	fakeVeTESClient := vetesclientfake.NewFakeClient(ctrl)
	fakeVeTESClient.EXPECT().
		ListClusters(gomock.Any(), gomock.Any()).
		Return(&clientmodels.ListClustersResponse{{
			ID:                 "cluster-01",
			HeartbeatTimestamp: now.Format(time.RFC3339),
			Capacity: &clientmodels.Capacity{
				Count:       utils.Point(10),
				CPUCores:    utils.Point(10),
				RamGB:       utils.Point[float64](100),
				DiskGB:      utils.Point[float64](1000),
				GPUCapacity: &clientmodels.GPUCapacity{GPU: map[string]float64{"type-01": 5}},
			},
			Limits: &clientmodels.Limits{
				CPUCores: utils.Point(4),
				RamGB:    utils.Point[float64](10),
				GPULimit: &clientmodels.GPULimit{GPU: map[string]float64{"type-01": 1}},
			},
		}}, nil)

	i := &clusterCacheImpl{vetesClient: fakeVeTESClient, clusters: make([]*schemodels.ClusterInfo, 0)}
	err := i.syncClusters(context.TODO())
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(i.clusters).To(gomega.BeEquivalentTo([]*schemodels.ClusterInfo{{
		ID:                 "cluster-01",
		HeartbeatTimestamp: now,
		Capacity: &schemodels.Capacity{
			Count:       utils.Point(10),
			CPUCores:    utils.Point(10),
			RamGB:       utils.Point[float64](100),
			DiskGB:      utils.Point[float64](1000),
			GPUCapacity: &schemodels.GPUCapacity{GPU: map[string]float64{"type-01": 5}},
		},
		Limits: &schemodels.Limits{
			CPUCores: utils.Point(4),
			RamGB:    utils.Point[float64](10),
			GPULimit: &schemodels.GPULimit{GPU: map[string]float64{"type-01": 1}},
		},
	}}))
}

func TestListClusters(t *testing.T) {
	g := gomega.NewWithT(t)
	i := &clusterCacheImpl{clusters: []*schemodels.ClusterInfo{{ID: "cluster-01"}}}
	resp := i.ListClusters()
	g.Expect(resp).To(gomega.BeEquivalentTo([]*schemodels.ClusterInfo{{ID: "cluster-01"}}))
}
