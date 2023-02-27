package distribute_framework

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	tidbutil "github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	DefaultConcurrency        = 8
	DefaultSubtaskConcurrency = 16
	checkTaskFinishedInterval = 300 * time.Millisecond
	checkTaskRunningInterval  = 300 * time.Millisecond
)

type DistPlanner struct {
	concurrency       int
	distributionType  string
	needRedistributed bool
	splitter          Splitter
}

type Splitter interface {
	// SplitTask splits the task into subtasks.
	SplitTask() ([]*task, error)
}

type AddIndexSplitter struct {
	startKey kv.Key
	endKey   kv.Key
}

func (splitter *AddIndexSplitter) SplitTask() ([]*task, error) {
	return nil, nil
}

type Dispatcher struct {
	ctx        context.Context
	cancel     context.CancelFunc
	gTaskMgr   *globalTaskManager
	subTaskMgr *subTaskManager
	wg         tidbutil.WaitGroupWrapper

	runningGlobalTasks struct {
		sync.RWMutex
		tasks map[int64]*globalTask
	}
}

func (d *Dispatcher) getRunningGlobalTasks() map[int64]*globalTask {
	d.runningGlobalTasks.RLock()
	defer d.runningGlobalTasks.RUnlock()
	return d.runningGlobalTasks.tasks
}

func (d *Dispatcher) setRunningGlobalTasks(gTask *globalTask) {
	d.runningGlobalTasks.RLock()
	defer d.runningGlobalTasks.RUnlock()
	d.runningGlobalTasks.tasks[gTask.id] = gTask
}

func (d *Dispatcher) delRunningGlobalTasks(globalTaskID int64) {
	d.runningGlobalTasks.Lock()
	defer d.runningGlobalTasks.Unlock()
	delete(d.runningGlobalTasks.tasks, globalTaskID)
}

// GenerateDistPlan generates distributed plan from Task.
func (d *Dispatcher) GenerateDistPlan(gTask *globalTask) (*DistPlanner, error) {
	distPlan := &DistPlanner{
		concurrency:      DefaultSubtaskConcurrency,
		distributionType: gTask.tp,
	}
	switch gTask.tp {
	case "add_index_backfill":
		distPlan.splitter = &AddIndexSplitter{
			// TODO: Get key range for gTask.meta.
		}
	default:
		return nil, errors.New("type is invalid")
	}
	return distPlan, nil
}

func (d *Dispatcher) FinishTask(gTask *globalTask) error {
	// TODO: Consider putting the following operations into a transaction.
	err := d.gTaskMgr.UpdateTask(gTask)
	if err != nil {
		return err
	}
	if gTask.state != SuccState {
		// TODO: Update tasks states to cancelled.
		return d.subTaskMgr.RemoveTasks(gTask.id)
	}
	return nil
}

func (d *Dispatcher) detectionTask(gTask *globalTask) (isFinished bool) {
	// TODO: Consider putting the following operations into a transaction.
	// TODO: Consider retry
	// TODO: Consider collect some information about the tasks.
	tasks, err := d.subTaskMgr.GetInterruptedTask(gTask.id)
	if err != nil {
		logutil.BgLogger().Info("detection task failed", zap.Error(err))
		return false
	}
	if len(tasks) == 0 {
		isFinished, err = d.subTaskMgr.IsFinishedTask(gTask.id)
		if err != nil {
			logutil.BgLogger().Info("get task cnt failed", zap.Int64("global task ID", gTask.id), zap.Error(err))
		}
		if isFinished {
			gTask.state = SuccState
		}
	} else {
		// TODO: Consider cancelled or failed.
		gTask.state = tasks[0].state
	}
	if err := d.FinishTask(gTask); err != nil {
		logutil.BgLogger().Info("finish task failed", zap.Int64("global task ID", gTask.id), zap.Error(err))
		return false
	}
	return true
}

// DetectionTaskLoop monitors the status of the subtasks.
func (d *Dispatcher) DetectionTaskLoop() {
	ticker := time.NewTicker(checkTaskFinishedInterval)
	defer ticker.Stop()
	for {
		select {
		case <-d.ctx.Done():
			logutil.BgLogger().Info("detection task loop exits", zap.Error(d.ctx.Err()))
		case <-ticker.C:
			gTasks := d.getRunningGlobalTasks()
			// TODO: Do we need to handle it asynchronously.
			for _, gTask := range gTasks {
				isFinished := d.detectionTask(gTask)
				if isFinished {
					d.delRunningGlobalTasks(gTask.id)
				}
			}
		}
	}
}

func (d *Dispatcher) loadTaskAndRun(gTask *globalTask) (err error) {
	// TODO: Consider retry
	if gTask.metaM.DistPlan == nil {
		distPlan, err := d.GenerateDistPlan(gTask)
		if err != nil {
			logutil.BgLogger().Warn("gen dist-plan failed", zap.Error(err))
			return err
		}
		gTask.metaM.DistPlan = distPlan
		err = d.gTaskMgr.UpdateTask(gTask)
		if err != nil {
			logutil.BgLogger().Warn("update global task failed", zap.Error(err))
			return err
		}
	}

	// TODO: Consider batch splitting
	// TODO: Synchronization interruption problem, e.g. AddNewTask failed
	subTasks, err := gTask.metaM.DistPlan.splitter.SplitTask()
	if err != nil {
		logutil.BgLogger().Warn("update global task failed", zap.Error(err))
		return err
	}
	for _, subTask := range subTasks {
		// TODO: Get TiDB_Instance_ID
		err := d.subTaskMgr.AddNewTask(subTask.globalTaskID, "", subTask.meta)
		if err != nil {
			logutil.BgLogger().Warn("add subtask failed", zap.Stringer("subTask", subTask), zap.Error(err))
			return err
		}
	}
	d.setRunningGlobalTasks(gTask)
	return nil
}

// DispatchTaskLoop dispatches the global tasks.
func (d *Dispatcher) DispatchTaskLoop() {
	ticker := time.NewTicker(checkTaskRunningInterval)
	defer ticker.Stop()
	for {
		select {
		case <-d.ctx.Done():
			logutil.BgLogger().Info("dispatch task loop exits", zap.Error(d.ctx.Err()))
		case <-ticker.C:
			cnt := len(d.getRunningGlobalTasks())
			for cnt < DefaultConcurrency {
				// TODO: Consider retry
				gTask, err := d.gTaskMgr.GetNewTask()
				if err != nil {
					logutil.BgLogger().Warn("get new task failed", zap.Error(err))
					continue
				}
				d.wg.Run(func() {
					d.loadTaskAndRun(gTask)
				})
				cnt++
			}
		}
	}
}

func NewDispatcher(ctx context.Context) (*Dispatcher, error) {
	gTaskManager, err := getGlobalTaskManager()
	if err != nil {
		return nil, err
	}
	dispatcher := &Dispatcher{
		gTaskMgr: gTaskManager,
		subTaskMgr: &subTaskManager{
			// TODO: set this session
			se: gTaskManager.se,
		},
	}
	dispatcher.ctx, dispatcher.cancel = context.WithCancel(ctx)
	dispatcher.subTaskMgr.ctx = dispatcher.ctx
	dispatcher.runningGlobalTasks.tasks = make(map[int64]*globalTask)

	return dispatcher, nil
}

func (d *Dispatcher) Start() {
	d.wg.Run(func() {
		d.DispatchTaskLoop()
	})
	d.wg.Run(func() {
		d.DetectionTaskLoop()
	})
}

func (d *Dispatcher) Stop() {
	d.cancel()
	d.wg.Wait()
}
