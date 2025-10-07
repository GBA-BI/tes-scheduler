package consts

// Component is the component name
const Component = "veTES-scheduler"

// task state
const (
	TaskQueued        = "QUEUED"
	TaskInitializing  = "INITIALIZING"
	TaskRunning       = "RUNNING"
	TaskComplete      = "COMPLETE"
	TaskSystemError   = "SYSTEM_ERROR"
	TaskExecutorError = "EXECUTOR_ERROR"
	TaskCanceling     = "CANCELING"
	TaskCanceled      = "CANCELED"
)

// task view types
const (
	MinimalView = "MINIMAL"
	BasicView   = "BASIC"
	FullView    = "FULL"
)

// ListTasks pageSize
const (
	DefaultPageSize = 256
	MaximumPageSize = 2048
)
