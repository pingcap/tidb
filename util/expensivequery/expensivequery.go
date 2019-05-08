package expensivequery

import (
	"context"
	"sync"
	"time"
	"strconv"
	"strings"
	"fmt"

	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/memory"
	"go.uber.org/zap"
	"github.com/pingcap/log"
	"go.uber.org/zap/zapcore"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/util/logutil"
)

var zeroTime = time.Time{}

type expensiveQueryCheckItems struct {
	sql string
	sc sessionctx.Context
	startTime time.Time
	memTracker *memory.Tracker
	plan plannercore.Plan
}

type expensiveQueryHandler struct {
	sync.Map
	ticker          *time.Ticker
	memExceedConnCh chan uint64
}

func (eqh *expensiveQueryHandler) Register(sc sessionctx.Context, sql string, startTime time.Time, plan plannercore.Plan) {
	connID := sc.GetSessionVars().ConnectionID
	memTracker := sc.GetSessionVars().StmtCtx.MemTracker
	eqh.Store(connID, &expensiveQueryCheckItems{sql, sc, startTime, memTracker, plan})
}

func (eqh *expensiveQueryHandler) Unregister(connID uint64) {
	eqh.Delete(connID)
}

func (eqh *expensiveQueryHandler) Run(ctx context.Context) {
	defer eqh.ticker.Stop()
	checkExceedTime := func(key, value interface{})bool{
		item := value.(*expensiveQueryCheckItems)
		startTime := item.startTime
		if !startTime.Equal(zeroTime) &&
			time.Since(startTime) >= time.Second * time.Duration(item.sc.GetSessionVars().ExpensiveQueryTimeThreshold) {
				// Set startTime to Zero to avoid print duplicated log.
				item.startTime = zeroTime
				logExpensiveQuery(ctx, item.sc, item.plan, item.sql)
		}
		return true
	}
	for {
		select {
		case <-eqh.ticker.C:
			eqh.Range(checkExceedTime)
		case connID := <-eqh.memExceedConnCh:
			value, _ := eqh.Load(connID)
			item := value.(*expensiveQueryCheckItems)
			logExpensiveQuery(ctx, item.sc, item.plan, item.sql)
		}
	}
}

var GlobalExpensiveQueryHandler = &expensiveQueryHandler{
	sync.Map{},
	time.NewTicker(time.Second),
	make(chan uint64, 1024),
}

// LogExpensiveQuery logs the queries which exceed the time threshold or memory threshold.
func logExpensiveQuery(ctx context.Context, sctx sessionctx.Context, p plannercore.Plan, sql string) {
	level := log.GetLevel()
	if level > zapcore.WarnLevel {
		return
	}
	logFields := make([]zap.Field, 0, 20)
	sessVars := sctx.GetSessionVars()
	execDetail := sessVars.StmtCtx.GetExecDetails()
	logFields = append(logFields, execDetail.ToZapFields()...)
	if copTaskInfo := sessVars.StmtCtx.CopTasksDetails(); copTaskInfo != nil {
		logFields = append(logFields, copTaskInfo.ToZapFields()...)
	}
	if statsInfos := plannercore.GetStatsInfo(p); len(statsInfos) > 0 {
		var buf strings.Builder
		firstComma := false
		vStr := ""
		for k, v := range statsInfos {
			if v == 0 {
				vStr = "pseudo"
			} else {
				vStr = strconv.FormatUint(v, 10)
			}
			if firstComma {
				buf.WriteString("," + k + ":" + vStr)
			} else {
				buf.WriteString(k + ":" + vStr)
				firstComma = true
			}
		}
		logFields = append(logFields, zap.String("stats", buf.String()))
	}
	if sessVars.ConnectionID != 0 {
		logFields = append(logFields, zap.Uint64("conn_id", sessVars.ConnectionID))
	}
	if sessVars.User != nil {
		logFields = append(logFields, zap.String("user", sessVars.User.String()))
	}
	if len(sessVars.CurrentDB) > 0 {
		logFields = append(logFields, zap.String("database", sessVars.CurrentDB))
	}
	var tableIDs, indexIDs string
	if len(sessVars.StmtCtx.TableIDs) > 0 {
		tableIDs = strings.Replace(fmt.Sprintf("%v", sessVars.StmtCtx.TableIDs), " ", ",", -1)
		logFields = append(logFields, zap.String("table_ids", tableIDs))
	}
	if len(sessVars.StmtCtx.IndexIDs) > 0 {
		indexIDs = strings.Replace(fmt.Sprintf("%v", sessVars.StmtCtx.IndexIDs), " ", ",", -1)
		logFields = append(logFields, zap.String("index_ids", indexIDs))
	}
	txnTs := sessVars.TxnCtx.StartTS
	logFields = append(logFields, zap.Uint64("txn_start_ts", txnTs))
	if memTracker := sessVars.StmtCtx.MemTracker; memTracker != nil {
		logFields = append(logFields, zap.String("mem_max", memTracker.BytesToString(memTracker.MaxConsumed())))
	}

	const logSQLLen = 1024 * 8
	if len(sql) > logSQLLen {
		sql = fmt.Sprintf("%s len(%d)", sql[:logSQLLen], len(sql))
	}
	logFields = append(logFields, zap.String("sql", sql))

	logutil.Logger(ctx).Warn("expensive_query", logFields...)
}
