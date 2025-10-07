package prioritysort

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/onsi/gomega"

	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/cache"
	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/cache/fake"
	schemodels "github.com/GBA-BI/tes-scheduler/pkg/scheduler/models"
)

func TestLess(t *testing.T) {
	g := gomega.NewWithT(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tests := []struct {
		name            string
		taskI           *schemodels.TaskInfo
		taskJ           *schemodels.TaskInfo
		extraPriorities []*schemodels.ExtraPriorityInfo
		expLess         bool
	}{
		{
			name: "no extra, compare priorityValue",
			taskI: &schemodels.TaskInfo{
				PriorityValue: 100,
			},
			taskJ: &schemodels.TaskInfo{
				PriorityValue: -100,
			},
			expLess: true,
		},
		{
			name: "extra match",
			taskI: &schemodels.TaskInfo{
				BioosInfo: &schemodels.BioosInfo{
					AccountID: "account-01",
					UserID:    "user-01",
				},
				PriorityValue: 100,
			},
			taskJ: &schemodels.TaskInfo{
				BioosInfo: &schemodels.BioosInfo{
					AccountID: "account-01",
					UserID:    "user-02",
				},
				PriorityValue: -100,
			},
			extraPriorities: []*schemodels.ExtraPriorityInfo{{
				AccountID:          "account-01",
				UserID:             "user-01",
				ExtraPriorityValue: 10,
			}, {
				AccountID:          "account-01",
				UserID:             "user-02",
				ExtraPriorityValue: 1000,
			}},
			expLess: false,
		},
		{
			name: "equal value, compare CreationTime",
			taskI: &schemodels.TaskInfo{
				CreationTime: time.Now().Add(time.Second),
				BioosInfo: &schemodels.BioosInfo{
					AccountID: "account-01",
					UserID:    "user-01",
				},
				PriorityValue: 100,
			},
			taskJ: &schemodels.TaskInfo{
				CreationTime: time.Now().Add(time.Hour),
				BioosInfo: &schemodels.BioosInfo{
					AccountID: "account-01",
					UserID:    "user-02",
				},
				PriorityValue: -100,
			},
			extraPriorities: []*schemodels.ExtraPriorityInfo{{
				AccountID:          "account-01",
				UserID:             "user-01",
				ExtraPriorityValue: -100,
			}, {
				AccountID:          "account-01",
				UserID:             "user-02",
				ExtraPriorityValue: 100,
			}},
			expLess: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fakeExtraPriorityCache := fake.NewFakeExtraPriorityCache(ctrl)
			fakeExtraPriorityCache.EXPECT().ListExtraPriorities().Return(test.extraPriorities)
			i := &impl{cache: &cache.Cache{ExtraPriorityCache: fakeExtraPriorityCache}}
			g.Expect(i.Less(test.taskI, test.taskJ)).To(gomega.Equal(test.expLess))
		})
	}
}
