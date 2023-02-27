package distribute_framework

import (
	"context"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
	"sync"
	"sync/atomic"
	"time"
)

type globalTask struct {
	id           int64
	tp           string
	dispatcherId string
	state        string
	startTime    time.Time
	meta         []byte
}

type globalTaskManager struct {
	ctx context.Context
	se  sessionctx.Context
	mu  sync.Mutex
}

// globalInfoSyncer stores the global infoSyncer.
// Use a global variable for simply the code, use the domain.infoSyncer will have circle import problem in some pkg.
// Use atomic.Value to avoid data race in the test.
var globalTaskManagerInstance atomic.Value

func NewGlobalTaskManager(ctx context.Context, se sessionctx.Context) *globalTaskManager {
	return &globalTaskManager{
		ctx: ctx,
		se:  se,
	}
}

func NewSubTaskManager(ctx context.Context, se sessionctx.Context) *subTaskManager {
	return &subTaskManager{
		ctx: ctx,
		se:  se,
	}
}

func getGlobalTaskManager() (*globalTaskManager, error) {
	v := globalTaskManagerInstance.Load()
	if v == nil {
		return nil, errors.New("globalInfoSyncer is not initialized")
	}
	return v.(*globalTaskManager), nil
}

func setGlobalTaskManager(is *globalTaskManager) {
	globalTaskManagerInstance.Store(is)
}

func execSQL(ctx context.Context, se sessionctx.Context, sql string, args ...interface{}) ([]chunk.Row, error) {
	rs, err := se.(sqlexec.SQLExecutor).ExecuteInternal(ctx, sql, args...)
	if rs != nil {
		//nolint: errcheck
		defer rs.Close()
	}
	if err != nil {
		return nil, err
	}
	if rs != nil {
		return sqlexec.DrainRecordSet(ctx, rs, 1)
	}
	return nil, nil
}

func (stm *globalTaskManager) AddNewTask(tp string, meta []byte) (int64, error) {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	_, err := execSQL(stm.ctx, stm.se, "insert into mysql.tidb_global_task(type, meta) values (?, ?)", tp, meta)
	if err != nil {
		return 0, err
	}

	rs, err := execSQL(stm.ctx, stm.se, "select @@last_insert_id")
	if err != nil {
		return 0, err
	}

	return rs[0].GetInt64(0), nil
}

// GetNewTask get a new task from global task table, it's used by dispatcher only.
func (stm *globalTaskManager) GetNewTask() (task globalTask, err error) {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	rs, err := execSQL(stm.ctx, stm.se, "select * from mysql.tidb_global_task where state is NULL limit 1")
	if err != nil {
		return task, err
	}

	task = globalTask{
		id:           rs[0].GetInt64(0),
		tp:           rs[0].GetString(1),
		dispatcherId: rs[0].GetString(2),
		state:        rs[0].GetString(3),
		meta:         rs[0].GetBytes(5),
	}
	task.startTime, _ = rs[0].GetTime(4).GoTime(time.UTC)

	return task, nil
}

func (stm *globalTaskManager) UpdateTask(task globalTask) error {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	_, err := execSQL(stm.ctx, stm.se, "update mysql.tidb_global_task set state = ?, dispatcher_id = ?, start_time = ? where id = ?", task.state, task.dispatcherId, task.startTime, task.id)
	if err != nil {
		return err
	}

	return nil
}

func (stm *globalTaskManager) GetRunnableTask() (task globalTask, err error) {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	rs, err := execSQL(stm.ctx, stm.se, "select * from mysql.tidb_global_task where state is not NULL limit 1")
	if err != nil {
		return task, err
	}

	task = globalTask{
		id:           rs[0].GetInt64(0),
		tp:           rs[0].GetString(1),
		dispatcherId: rs[0].GetString(2),
		state:        rs[0].GetString(3),
		meta:         rs[0].GetBytes(5),
	}
	task.startTime, _ = rs[0].GetTime(4).GoTime(time.UTC)

	return task, nil
}

type subTaskManager struct {
	ctx context.Context
	se  sessionctx.Context
	mu  sync.Mutex
}

func (stm *subTaskManager) AddNewTask(globalTaskID int64, designatedTiDBID string, meta []byte) error {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	_, err := execSQL(stm.ctx, stm.se, "insert into mysql.tidb_sub_task(global_task_id, designate_tidb_id, meta) values (?, ?, ?)", globalTaskID, designatedTiDBID, meta)
	if err != nil {
		return err
	}

	return nil
}

func (stm *subTaskManager) GetTaskByTiDBID(TiDBID string) (subTaskID int64, meta []byte, err error) {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	rs, err := execSQL(stm.ctx, stm.se, "select id, meta from mysql.tidb_sub_task where designate_tidb_id = ? limit 1", TiDBID)
	if err != nil {
		return 0, nil, err
	}

	id := rs[0].GetInt64(0)
	meta = rs[0].GetBytes(1)

	return id, meta, nil
}

func (stm *subTaskManager) UpdateTask(subTaskID int64, meta []byte) error {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	_, err := execSQL(stm.ctx, stm.se, "update mysql.tidb_sub_task set meta = ? where id = ?", meta, subTaskID)
	if err != nil {
		return err
	}

	return nil
}

func (stm *subTaskManager) DeleteTask(subTaskID int64) error {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	_, err := execSQL(stm.ctx, stm.se, "delete from mysql.tidb_sub_task where where id = ?", subTaskID)
	if err != nil {
		return err
	}

	return nil
}
