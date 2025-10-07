package models

// ExtraPriorityInfo ...
type ExtraPriorityInfo struct {
	AccountID          string
	UserID             string
	SubmissionID       string
	RunID              string
	ExtraPriorityValue int
}

// MatchTask ...
func (e *ExtraPriorityInfo) MatchTask(task *TaskInfo) bool {
	if e == nil || task == nil || task.BioosInfo == nil {
		return false
	}
	if e.AccountID != "" && e.AccountID == task.BioosInfo.AccountID {
		return e.UserID == "" || e.UserID == task.BioosInfo.UserID
	}
	if e.SubmissionID != "" && e.SubmissionID == task.BioosInfo.SubmissionID {
		return true
	}
	if e.RunID != "" && e.RunID == task.BioosInfo.RunID {
		return true
	}
	return false
}
