package models

import (
	"testing"

	"github.com/onsi/gomega"
)

func TestMatchTask(t *testing.T) {
	g := gomega.NewWithT(t)

	task := &TaskInfo{BioosInfo: &BioosInfo{
		AccountID:    "account-01",
		UserID:       "user-01",
		SubmissionID: "submission-01",
		RunID:        "run-01",
	}}

	tests := []struct {
		name          string
		extraPriority *ExtraPriorityInfo
		expMatch      bool
	}{
		{
			name:          "nil",
			extraPriority: nil,
			expMatch:      false,
		},
		{
			name:          "accountID match",
			extraPriority: &ExtraPriorityInfo{AccountID: "account-01"},
			expMatch:      true,
		},
		{
			name:          "accountID not match",
			extraPriority: &ExtraPriorityInfo{AccountID: "account-02"},
			expMatch:      false,
		},
		{
			name:          "accountID and userID match",
			extraPriority: &ExtraPriorityInfo{AccountID: "account-01", UserID: "user-01"},
			expMatch:      true,
		},
		{
			name:          "accountID and userID not match",
			extraPriority: &ExtraPriorityInfo{AccountID: "account-01", UserID: "user-02"},
			expMatch:      false,
		},
		{
			name:          "submissionID match",
			extraPriority: &ExtraPriorityInfo{SubmissionID: "submission-01"},
			expMatch:      true,
		},
		{
			name:          "submissionID not match",
			extraPriority: &ExtraPriorityInfo{SubmissionID: "submission-02"},
			expMatch:      false,
		},
		{
			name:          "runID match",
			extraPriority: &ExtraPriorityInfo{RunID: "run-01"},
			expMatch:      true,
		},
		{
			name:          "runID not match",
			extraPriority: &ExtraPriorityInfo{RunID: "run-02"},
			expMatch:      false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			g.Expect(test.extraPriority.MatchTask(task)).To(gomega.Equal(test.expMatch))
		})
	}
}
