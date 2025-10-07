package cache

import (
	"context"
	"sync"
	"time"

	"github.com/GBA-BI/tes-scheduler/pkg/log"

	"github.com/GBA-BI/tes-scheduler/pkg/consts"
	"github.com/GBA-BI/tes-scheduler/pkg/scheduler/crontab"
	schemodels "github.com/GBA-BI/tes-scheduler/pkg/scheduler/models"
	"github.com/GBA-BI/tes-scheduler/pkg/vetesclient"
	clientmodels "github.com/GBA-BI/tes-scheduler/pkg/vetesclient/models"
)

// schedulerName is for UpdateTask TaskLog ClusterID
const schedulerName = "scheduler"

// TaskCache caches non-finished taskInfo
type TaskCache interface {
	ListTasks(clusterID string) []*schemodels.TaskInfo
	ListScheduledTasks() []*schemodels.TaskInfo
	ListTaskClusterIDs() []string
	// UpdateTask update actual task and cache
	UpdateTask(ctx context.Context, id string, state, clusterID, message *string) error
}

// taskCacheImpl ...
type taskCacheImpl struct {
	vetesClient vetesclient.Client

	dataLock sync.RWMutex
	data     *data
}

type data struct {
	tasks map[string]*schemodels.TaskInfo
	// clusterID -> taskID set. clusterID may be empty, which means not scheduled
	clusterIndexer map[string]map[string]struct{}
}

func (d *data) addTask(task *schemodels.TaskInfo) {
	d.tasks[task.ID] = task
	if _, ok := d.clusterIndexer[task.ClusterID]; !ok {
		d.clusterIndexer[task.ClusterID] = make(map[string]struct{})
	}
	d.clusterIndexer[task.ClusterID][task.ID] = struct{}{}
}

func (d *data) updateTask(id string, state, clusterID *string) {
	oldTask, ok := d.tasks[id]
	if !ok {
		return
	}
	// we need to copy to newTask, or the ListTasks result may change.
	// shallow copy, because we only change state and clusterID
	newTask := new(schemodels.TaskInfo)
	*newTask = *oldTask

	if state != nil && newTask.State != *state {
		newTask.State = *state
	}
	if clusterID != nil && newTask.ClusterID != *clusterID {
		newTask.ClusterID = *clusterID
		delete(d.clusterIndexer[oldTask.ClusterID], id)
		if len(d.clusterIndexer[oldTask.ClusterID]) == 0 {
			delete(d.clusterIndexer, oldTask.ClusterID)
		}
		if _, ok = d.clusterIndexer[*clusterID]; !ok {
			d.clusterIndexer[*clusterID] = make(map[string]struct{})
		}
		d.clusterIndexer[*clusterID][id] = struct{}{}
	}
	d.tasks[id] = newTask
}

func (d *data) deleteTask(id string) {
	oldTask, ok := d.tasks[id]
	if !ok {
		return
	}
	delete(d.tasks, id)
	delete(d.clusterIndexer[oldTask.ClusterID], id)
	if len(d.clusterIndexer[oldTask.ClusterID]) == 0 {
		delete(d.clusterIndexer, oldTask.ClusterID)
	}
}

var _ TaskCache = (*taskCacheImpl)(nil)

// NewTaskCache ...
func NewTaskCache(vetesClient vetesclient.Client, opts *Options) (TaskCache, error) {
	cache := &taskCacheImpl{
		vetesClient: vetesClient,
		data: &data{
			tasks:          make(map[string]*schemodels.TaskInfo),
			clusterIndexer: make(map[string]map[string]struct{}),
		},
	}
	if err := cache.initCache(context.Background()); err != nil {
		return nil, err
	}
	if err := crontab.RegisterCron(opts.SyncPeriod, func() {
		ctx := context.Background()
		if err := cache.syncTasks(ctx); err != nil {
			log.CtxErrorw(ctx, "failed to sync tasks", "err", err)
		}
	}); err != nil {
		return nil, err
	}

	return cache, nil
}

// ListTasks ...
func (i *taskCacheImpl) ListTasks(clusterID string) []*schemodels.TaskInfo {
	i.dataLock.RLock()
	defer i.dataLock.RUnlock()

	ids := i.data.clusterIndexer[clusterID]
	if len(ids) == 0 {
		return nil
	}
	res := make([]*schemodels.TaskInfo, 0, len(ids))
	for id := range ids {
		res = append(res, i.data.tasks[id])
	}
	return res
}

// ListScheduledTasks ...
func (i *taskCacheImpl) ListScheduledTasks() []*schemodels.TaskInfo {
	i.dataLock.RLock()
	defer i.dataLock.RUnlock()

	res := make([]*schemodels.TaskInfo, 0)
	for _, task := range i.data.tasks {
		if task.ClusterID != "" {
			res = append(res, task)
		}
	}
	return res
}

// ListTaskClusterIDs ...
func (i *taskCacheImpl) ListTaskClusterIDs() []string {
	i.dataLock.RLock()
	defer i.dataLock.RUnlock()
	res := make([]string, 0, len(i.data.clusterIndexer))
	for clusterID := range i.data.clusterIndexer {
		if clusterID != "" {
			res = append(res, clusterID)
		}
	}
	return res
}

// UpdateTask ...
func (i *taskCacheImpl) UpdateTask(ctx context.Context, taskID string, state, clusterID, message *string) error {
	req := &clientmodels.UpdateTaskRequest{
		ID:        taskID,
		ClusterID: clusterID,
		State:     state,
	}
	if message != nil {
		req.Logs = []*clientmodels.TaskLog{{
			ClusterID:  schedulerName,
			SystemLogs: []string{*message},
		}}
	}
	if _, err := i.vetesClient.UpdateTask(ctx, req); err != nil {
		return err
	}

	i.dataLock.Lock()
	defer i.dataLock.Unlock()

	if state != nil && isFinished(*state) {
		i.data.deleteTask(taskID)
		return nil
	}
	i.data.updateTask(taskID, state, clusterID)
	return nil
}

func (i *taskCacheImpl) initCache(ctx context.Context) error {
	tasks, err := i.listTasks(ctx, consts.BasicView, consts.DefaultPageSize)
	if err != nil {
		return err
	}
	for _, task := range tasks {
		i.data.addTask(clientTaskToTaskInfo(ctx, task))
	}
	return nil
}

func (i *taskCacheImpl) syncTasks(ctx context.Context) error {
	// We have to lock here, because if we lock after listTasks, the listTasks action may
	// be before UpdateTask, and cache may be rolled back.
	i.dataLock.Lock()
	defer i.dataLock.Unlock()

	tasks, err := i.listTasks(ctx, consts.MinimalView, consts.MaximumPageSize)
	if err != nil {
		return err
	}

	newData := &data{
		tasks:          make(map[string]*schemodels.TaskInfo, len(i.data.tasks)),
		clusterIndexer: make(map[string]map[string]struct{}, len(i.data.clusterIndexer)),
	}

	for _, task := range tasks {
		if oldTask, ok := i.data.tasks[task.ID]; ok {
			newData.addTask(oldTask)
			newData.updateTask(task.ID, &task.State, nil) // just change state
			continue
		}
		gotTask, err := i.vetesClient.GetTask(ctx, &clientmodels.GetTaskRequest{ID: task.ID, View: consts.BasicView})
		if err != nil {
			return err
		}
		newData.addTask(clientTaskToTaskInfo(ctx, gotTask.Task))
	}

	i.data = newData
	return nil
}

func (i *taskCacheImpl) listTasks(ctx context.Context, view string, pageSize int) ([]*clientmodels.Task, error) {
	res := make([]*clientmodels.Task, 0)
	var pageToken string
	for {
		resp, err := i.vetesClient.ListTasks(ctx, &clientmodels.ListTasksRequest{
			State:     nonFinishedStates,
			View:      view,
			PageSize:  pageSize,
			PageToken: pageToken,
		})
		if err != nil {
			return nil, err
		}
		res = append(res, resp.Tasks...)
		if resp.NextPageToken == "" {
			break
		}
		pageToken = resp.NextPageToken
	}
	return res, nil
}

func clientTaskToTaskInfo(ctx context.Context, task *clientmodels.Task) *schemodels.TaskInfo {
	if task == nil {
		return nil
	}
	res := &schemodels.TaskInfo{
		ID:            task.ID,
		State:         task.State,
		ClusterID:     task.ClusterID,
		Resources:     clientTaskResourcesToTaskInfoResources(task.Resources),
		BioosInfo:     clientTaskBioosInfoToTaskInfoBioosInfo(task.BioosInfo),
		PriorityValue: task.PriorityValue,
	}
	var err error
	res.CreationTime, err = time.Parse(time.RFC3339, task.CreationTime)
	if err != nil {
		log.CtxErrorw(ctx, "parse CreationTime of task", "task", task.ID, "err", err)
	}
	return res
}

func clientTaskResourcesToTaskInfoResources(taskResources *clientmodels.Resources) *schemodels.Resources {
	if taskResources == nil {
		return nil
	}
	res := &schemodels.Resources{
		CPUCores: taskResources.CPUCores,
		RamGB:    taskResources.RamGB,
		DiskGB:   taskResources.DiskGB,
	}
	if taskResources.GPU != nil {
		res.GPU = &schemodels.GPUResource{
			Count: taskResources.GPU.Count,
			Type:  taskResources.GPU.Type,
		}
	}
	return res
}

func clientTaskBioosInfoToTaskInfoBioosInfo(bioosInfo *clientmodels.BioosInfo) *schemodels.BioosInfo {
	if bioosInfo == nil {
		return nil
	}
	return &schemodels.BioosInfo{
		AccountID:    bioosInfo.AccountID,
		UserID:       bioosInfo.UserID,
		SubmissionID: bioosInfo.SubmissionID,
		RunID:        bioosInfo.RunID,
	}
}

var nonFinishedStates = []string{consts.TaskQueued, consts.TaskInitializing, consts.TaskRunning, consts.TaskCanceling}

func isFinished(state string) bool {
	for _, nonFinishedState := range nonFinishedStates {
		if state == nonFinishedState {
			return false
		}
	}
	return true
}
