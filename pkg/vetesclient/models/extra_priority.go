package models

// ListExtraPriorityRequest ...
type ListExtraPriorityRequest struct {
	AccountID    string `query:"account_id"`
	SubmissionID string `query:"submission_id"`
	RunID        string `query:"run_id"`
}

// ListExtraPriorityResponse ...
type ListExtraPriorityResponse []*ExtraPriority

// ExtraPriority ...
type ExtraPriority struct {
	AccountID          string `json:"account_id,omitempty"`
	UserID             string `json:"user_id,omitempty"`
	SubmissionID       string `json:"submission_id,omitempty"`
	RunID              string `json:"run_id,omitempty"`
	ExtraPriorityValue int    `json:"extra_priority_value"`
}
