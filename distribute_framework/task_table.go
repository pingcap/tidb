package distribute_framework

import (
	"context"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
	"sync"
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
