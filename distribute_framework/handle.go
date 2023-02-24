package distribute_framework

import (
	"context"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"time"
)

type Handle struct {
	ctx context.Context
	se  sessionctx.Context
	gm  *globalTaskManager
}

func (h *Handle) checkGlobalTaskDone(id int64, ch chan struct{}) {
	tk := time.Tick(50 * time.Millisecond)
	for {
		select {
		case <-tk:
			r, err := execSQL(h.ctx, h.se, "select count(*) from mysql.global_task where state = 'done' and id = ?", id)
			if err != nil {
				logutil.BgLogger().Error("check global task done failed", zap.Error(err))
			}
			finish := r[0].GetInt64(0) == 1
			if finish {
				close(ch)
				return
			}
		}
	}
}

func NewHandle(ctx context.Context, se sessionctx.Context) (Handle, error) {
	gm, err := getGlobalTaskManager()
	if err != nil {
		return Handle{}, err
	}
	return Handle{
		ctx: ctx,
		se:  se,
		gm:  gm,
	}, nil
}

func (h *Handle) SubmitGlobalTaskAndRun(taskMeta GlobalTaskMeta) (taskID int64, done chan struct{}, err error) {
	id, err := h.gm.AddNewTask(taskMeta.GetType(), taskMeta.Serialize())
	if err != nil {
		return 0, nil, err
	}

	done = make(chan struct{})
	go func() {
		h.checkGlobalTaskDone(id, done)
	}()

	return id, done, nil
}
