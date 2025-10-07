package cache

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/onsi/gomega"

	schemodels "github.com/GBA-BI/tes-scheduler/pkg/scheduler/models"
	vetesclientfake "github.com/GBA-BI/tes-scheduler/pkg/vetesclient/fake"
	clientmodels "github.com/GBA-BI/tes-scheduler/pkg/vetesclient/models"
)

func TestSyncExtraPriorities(t *testing.T) {
	g := gomega.NewWithT(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fakeVeTESClient := vetesclientfake.NewFakeClient(ctrl)
	fakeVeTESClient.EXPECT().
		ListExtraPriority(gomock.Any(), gomock.Any()).
		Return(&clientmodels.ListExtraPriorityResponse{{
			SubmissionID:       "submission-01",
			ExtraPriorityValue: -100,
		}}, nil)

	i := &extraPriorityCacheImpl{vetesClient: fakeVeTESClient, extraPriorities: make([]*schemodels.ExtraPriorityInfo, 0)}
	err := i.syncExtraPriorities(context.TODO())
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(i.extraPriorities).To(gomega.BeEquivalentTo([]*schemodels.ExtraPriorityInfo{{
		SubmissionID:       "submission-01",
		ExtraPriorityValue: -100,
	}}))
}

func TestListExtraPriorities(t *testing.T) {
	g := gomega.NewWithT(t)
	i := &extraPriorityCacheImpl{extraPriorities: []*schemodels.ExtraPriorityInfo{{RunID: "run-01", ExtraPriorityValue: 10}}}
	resp := i.ListExtraPriorities()
	g.Expect(resp).To(gomega.BeEquivalentTo([]*schemodels.ExtraPriorityInfo{{RunID: "run-01", ExtraPriorityValue: 10}}))
}
