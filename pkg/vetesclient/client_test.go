package vetesclient

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/GBA-BI/tes-scheduler/pkg/consts"
	"github.com/GBA-BI/tes-scheduler/pkg/utils"
	"github.com/GBA-BI/tes-scheduler/pkg/vetesclient/models"
)

func TestClient(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "vetes client")
}

var fakeEndpoint = "http://vetes-api:8080"
var fakeClient = NewClient(&Options{
	Endpoint: fakeEndpoint,
	Timeout:  5 * time.Second,
})
var fakeTaskID = "task-xxxx"

var _ = ginkgo.BeforeSuite(func() {
	httpmock.ActivateNonDefault(fakeClient.(*impl).cli)
})
var _ = ginkgo.AfterSuite(func() {
	httpmock.DeactivateAndReset()
})
var _ = ginkgo.AfterEach(func() {
	httpmock.Reset()
})

var _ = ginkgo.It("ListTasks", func() {
	fakeResp := &models.ListTasksResponse{
		Tasks: []*models.Task{{
			ID:    fakeTaskID,
			State: consts.TaskQueued,
		}},
		NextPageToken: "next-token",
	}
	responder, _ := httpmock.NewJsonResponder(200, fakeResp)
	httpmock.RegisterResponder(http.MethodGet, fmt.Sprintf("%s%s/tasks?cluster_id=%s&name_prefix=%s&page_size=%s&page_token=%s&state=%s&state=%s&view=%s&without_cluster=%s",
		fakeEndpoint, ga4ghAPIPrefix, "cluster-01", "task-", "256", "last-token", consts.TaskQueued, consts.TaskCanceling, consts.MinimalView, "true"), responder)
	resp, err := fakeClient.ListTasks(context.Background(), &models.ListTasksRequest{
		NamePrefix:     "task-",
		State:          []string{consts.TaskQueued, consts.TaskCanceling},
		ClusterID:      "cluster-01",
		WithoutCluster: true, // this is invalid, but we just test query param here
		View:           consts.MinimalView,
		PageSize:       256,
		PageToken:      "last-token",
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(resp).To(gomega.BeEquivalentTo(fakeResp))
})

var _ = ginkgo.It("GetTask", func() {
	fakeResp := &models.GetTaskResponse{Task: &models.Task{
		ID:    fakeTaskID,
		State: consts.TaskCanceling,
	}}
	responder, _ := httpmock.NewJsonResponder(200, fakeResp)
	httpmock.RegisterResponder(http.MethodGet, fmt.Sprintf("%s%s/tasks/%s?view=%s", fakeEndpoint, ga4ghAPIPrefix, fakeTaskID, consts.MinimalView), responder)
	resp, err := fakeClient.GetTask(context.Background(), &models.GetTaskRequest{
		ID:   fakeTaskID,
		View: consts.MinimalView,
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(resp).To(gomega.BeEquivalentTo(fakeResp))
})

var _ = ginkgo.It("UpdateTask", func() {
	fakeResp := &models.UpdateTaskResponse{}
	responder, _ := httpmock.NewJsonResponder(200, fakeResp)
	httpmock.RegisterResponder(http.MethodPatch, fmt.Sprintf("%s%s/tasks/%s", fakeEndpoint, otherAPIPrefix, fakeTaskID), responder)
	_, err := fakeClient.UpdateTask(context.Background(), &models.UpdateTaskRequest{ID: fakeTaskID})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
})

var _ = ginkgo.It("GatherTasksResources", func() {
	fakeResp := &models.GatherTasksResourcesResponse{
		Count:    10,
		CPUCores: 10,
		RamGB:    100,
		DiskGB:   1000,
		GPU:      map[string]float64{"type-01": 5},
	}
	responder, _ := httpmock.NewJsonResponder(200, fakeResp)
	httpmock.RegisterResponder(http.MethodGet, fmt.Sprintf("%s%s/tasks/resources?account_id=%s&cluster_id=%s&state=%s&state=%s&user_id=%s&with_cluster=%s",
		fakeEndpoint, otherAPIPrefix, "account-01", "cluster-01", consts.TaskInitializing, consts.TaskRunning, "user-01", "true"), responder)
	resp, err := fakeClient.GatherTasksResources(context.Background(), &models.GatherTasksResourcesRequest{
		State:       []string{consts.TaskInitializing, consts.TaskRunning},
		ClusterID:   "cluster-01",
		WithCluster: true,
		AccountID:   "account-01",
		UserID:      "user-01",
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(resp).To(gomega.BeEquivalentTo(fakeResp))
})

var _ = ginkgo.It("ListClusters", func() {
	fakeResp := &models.ListClustersResponse{{
		ID:                 "cluster-01",
		HeartbeatTimestamp: "2023-01-01T00:00:00Z",
		Capacity: &models.Capacity{
			Count:       utils.Point(10),
			CPUCores:    utils.Point(10),
			RamGB:       utils.Point[float64](100),
			DiskGB:      utils.Point[float64](1000),
			GPUCapacity: &models.GPUCapacity{GPU: map[string]float64{"type-01": 5}},
		},
		Limits: &models.Limits{
			CPUCores: utils.Point(4),
			RamGB:    utils.Point[float64](10),
			GPULimit: &models.GPULimit{GPU: map[string]float64{"type-01": 1}},
		},
	}}
	responder, _ := httpmock.NewJsonResponder(200, fakeResp)
	httpmock.RegisterResponder(http.MethodGet, fmt.Sprintf("%s%s/clusters", fakeEndpoint, otherAPIPrefix), responder)
	resp, err := fakeClient.ListClusters(context.Background(), &models.ListClustersRequest{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(resp).To(gomega.BeEquivalentTo(fakeResp))
})

var _ = ginkgo.It("GetQuota", func() {
	fakeResp := &models.GetQuotaResponse{
		Global:    true, // this is invalid, but we just test query param here
		Default:   false,
		AccountID: "account-01",
		UserID:    "user-01",
		ResourceQuota: &models.ResourceQuota{
			Count:    utils.Point(10),
			CPUCores: utils.Point(10),
			RamGB:    utils.Point[float64](100),
			DiskGB:   utils.Point[float64](1000),
			GPUQuota: &models.GPUQuota{GPU: map[string]float64{"type-01": 5}},
		},
	}
	responder, _ := httpmock.NewJsonResponder(200, fakeResp)
	httpmock.RegisterResponder(http.MethodGet, fmt.Sprintf("%s%s/quota?account_id=%s&global=%s&user_id=%s",
		fakeEndpoint, otherAPIPrefix, "account-01", "true", "user-01"), responder)
	resp, err := fakeClient.GetQuota(context.Background(), &models.GetQuotaRequest{
		Global:    true,
		AccountID: "account-01",
		UserID:    "user-01",
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(resp).To(gomega.BeEquivalentTo(fakeResp))
})

var _ = ginkgo.It("ListExtraPriority", func() {
	fakeResp := &models.ListExtraPriorityResponse{{
		AccountID:          "account-01",
		UserID:             "user-01",
		SubmissionID:       "submission-01",
		RunID:              "run-01", // this is invalid, but we just test query param here
		ExtraPriorityValue: 100,
	}}
	responder, _ := httpmock.NewJsonResponder(200, fakeResp)
	httpmock.RegisterResponder(http.MethodGet, fmt.Sprintf("%s%s/extra_priority?account_id=%s&run_id=%s&submission_id=%s",
		fakeEndpoint, otherAPIPrefix, "account-01", "run-01", "submission-01"), responder)
	resp, err := fakeClient.ListExtraPriority(context.Background(), &models.ListExtraPriorityRequest{
		AccountID:    "account-01",
		SubmissionID: "submission-01",
		RunID:        "run-01",
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(resp).To(gomega.BeEquivalentTo(fakeResp))
})
