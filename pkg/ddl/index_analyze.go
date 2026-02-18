// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

type analyzeStatus int

const (
	analyzeUnknown analyzeStatus = iota
	analyzeRunning
	analyzeFinished
	analyzeFailed
)

// queryAnalyzeStatusSince queries `analyze_jobs` for the specified table
// and start_time >= startTS. It returns one of analyzeStatus values. If the
// sessPool cannot provide a session or the query fails, it returns
// analyzeUnknown and a nil error so the caller may decide to proceed with
// starting ANALYZE.
func (w *worker) queryAnalyzeStatusSince(startTS uint64, dbName, tblName string) (analyzeStatus, error) {
	sessCtx, err := w.sessPool.Get()
	if err != nil {
		return analyzeUnknown, err
	}
	defer w.sessPool.Put(sessCtx)

	startTimeStr := time.Now().UTC().Format(time.DateTime)
	if startTS > 0 {
		startTimeStr = model.TSConvert2Time(startTS).UTC().Format(time.DateTime)
	}
	kctx := kv.WithInternalSourceType(w.ctx, kv.InternalTxnStats)

	// set session time zone to UTC to match the time format in `startTimeStr`
	originalTimeZone := sessCtx.GetSessionVars().TimeZone
	sessCtx.GetSessionVars().TimeZone = time.UTC
	defer func() {
		sessCtx.GetSessionVars().TimeZone = originalTimeZone
	}()
	exec := sessCtx.GetRestrictedSQLExecutor()
	rows, _, chkErr := exec.ExecRestrictedSQL(kctx, []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession},
		"SELECT state FROM mysql.analyze_jobs WHERE table_schema = %? AND table_name = %? AND start_time >= %?", dbName, tblName, startTimeStr)
	if chkErr != nil {
		return analyzeUnknown, chkErr
	}
	if len(rows) == 0 {
		return analyzeUnknown, nil
	}

	for _, r := range rows {
		var state string
		if !r.IsNull(0) {
			state = r.GetString(0)
		}
		switch {
		case strings.EqualFold(state, "running"):
			return analyzeRunning, nil
		case strings.EqualFold(state, "failed"):
			return analyzeFailed, nil
		case strings.EqualFold(state, "finished"):
			return analyzeFinished, nil
		default:
			// unknown state: continue checking other rows
		}
	}
	// If no recognizable state found, treat as unknown so caller may attempt to start ANALYZE.
	return analyzeUnknown, nil
}

// analyzeStatusDecision encapsulates the decision logic after observing
// the analyze status for a table. It returns three booleans:
//
//	done: whether the caller should consider analyze finished (true) and continue DDL;
//	timedOut: whether the analyze has exceeded cumulative timeout and caller should proceed with timeout handling;
//	failed: whether the analyze has failed;
//	proceed: whether the caller should proceed to start ANALYZE locally (true for unknown status).
func (w *worker) analyzeStatusDecision(job *model.Job, dbName, tblName string, status analyzeStatus, cumulativeTimeout time.Duration) (done, timedOut, failed, proceed bool) {
	switch status {
	case analyzeFinished:
		logutil.DDLLogger().Info("analyze already finished by other owner", zap.Int64("jobID", job.ID), zap.String("db", dbName), zap.String("table", tblName))
		w.ddlCtx.clearAnalyzeStartTime(job.ID)
		w.ddlCtx.clearAnalyzeCumulativeTimeout(job.ID)
		return true, false, false, false
	case analyzeFailed:
		logutil.DDLLogger().Warn("analyze previously failed on another owner, continue finishing DDL", zap.Int64("jobID", job.ID), zap.String("db", dbName), zap.String("table", tblName))
		w.ddlCtx.clearAnalyzeStartTime(job.ID)
		w.ddlCtx.clearAnalyzeCumulativeTimeout(job.ID)
		return true, false, true, false
	case analyzeRunning:
		// If analyze is running, respect cumulative timeout. If expired, return timeout to let caller proceed.
		if start, ok := w.ddlCtx.getAnalyzeStartTime(job.ID); ok {
			if time.Since(start) > cumulativeTimeout {
				logutil.DDLLogger().Warn("analyze table is running but exceeded cumulative timeout, proceeding to finish DDL", zap.Int64("jobID", job.ID), zap.Duration("elapsed", time.Since(start)))
				w.ddlCtx.clearAnalyzeStartTime(job.ID)
				w.ddlCtx.clearAnalyzeCumulativeTimeout(job.ID)
				return false, true, false, false
			}
		} else {
			w.ddlCtx.setAnalyzeStartTime(job.ID, time.Now())
		}
		select {
		case <-w.ctx.Done():
			logutil.DDLLogger().Info("analyze table after create index context done", zap.Int64("jobID", job.ID), zap.Error(w.ctx.Err()))
			w.ddlCtx.clearAnalyzeStartTime(job.ID)
			w.ddlCtx.clearAnalyzeCumulativeTimeout(job.ID)
			return true, false, false, false
		case <-time.After(DefaultAnalyzeCheckInterval):
			return false, false, false, false
		}
	default:
		// analyzeUnknown: caller should proceed to start ANALYZE locally.
		return false, false, false, true
	}
}

// doAnalyzeWithoutReorg performs analyze for the table if needed without the logic of
// reorg and and returns whether the table info is updated.
func (w *worker) doAnalyzeWithoutReorg(job *model.Job, tblInfo *model.TableInfo) (finished bool) {
	switch job.ReorgMeta.AnalyzeState {
	case model.AnalyzeStateNone:
		if checkNeedAnalyze(job, tblInfo) {
			// Start analyze for the next time.
			job.ReorgMeta.AnalyzeState = model.AnalyzeStateRunning
		} else {
			job.ReorgMeta.AnalyzeState = model.AnalyzeStateSkipped
		}
		return false
	case model.AnalyzeStateRunning:
		w.startAnalyzeAndWait(job, tblInfo)
		return false
	default:
		logutil.DDLLogger().Info("analyze skipped or finished for multi-schema change",
			zap.Int64("job", job.ID), zap.Int8("state", job.ReorgMeta.AnalyzeState))
		return true
	}
}

func (w *worker) startAnalyzeAndWait(job *model.Job, tblInfo *model.TableInfo) {
	done, timedOut, failed := w.analyzeTableInner(job, tblInfo, job.SchemaName)
	failpoint.InjectCall("analyzeTableDone", job)
	if done || timedOut || failed {
		if done {
			job.ReorgMeta.AnalyzeState = model.AnalyzeStateDone
		}
		if timedOut {
			job.ReorgMeta.AnalyzeState = model.AnalyzeStateTimeout
		}
		if failed {
			job.ReorgMeta.AnalyzeState = model.AnalyzeStateFailed
		}
	}
}

// analyzeTableInner analyzes the table after creating index/modify column.
func (w *worker) analyzeTableInner(job *model.Job, tblInfo *model.TableInfo, dbName string) (done, timedOut, failed bool) {
	doneCh := w.ddlCtx.getAnalyzeDoneCh(job.ID)
	tblName := tblInfo.Name.L
	cumulativeTimeout, found := w.ddlCtx.getAnalyzeCumulativeTimeout(job.ID)
	if !found {
		cumulativeTimeout = DefaultCumulativeTimeout
		if job.RealStartTS != 0 {
			addStart := model.TSConvert2Time(job.RealStartTS)
			elapsed := time.Since(addStart)
			if elapsed*2 > cumulativeTimeout {
				cumulativeTimeout = elapsed * 2
			}
		}
		w.ddlCtx.setAnalyzeCumulativeTimeout(job.ID, cumulativeTimeout)
	}

	if doneCh == nil {
		status, err := w.queryAnalyzeStatusSince(job.StartTS, dbName, tblName)
		if err != nil {
			logutil.DDLLogger().Warn("query analyze status failed", zap.Int64("jobID", job.ID), zap.Error(err))
			status = analyzeUnknown
		}

		done, timedOut, failed, proceed := w.analyzeStatusDecision(job, dbName, tblName, status, cumulativeTimeout)
		if done || timedOut || failed {
			return done, timedOut, failed
		}
		if !proceed {
			// We decided not to proceed to start ANALYZE locally (i.e. it's running and we waited),
			// so simply return and retry later.
			return false, false, false
		}

		if _, ok := w.ddlCtx.getAnalyzeStartTime(job.ID); !ok {
			w.ddlCtx.setAnalyzeStartTime(job.ID, time.Now())
		}

		doneCh = make(chan error)
		eg := util.NewErrorGroupWithRecover()
		eg.Go(func() error {
			sessCtx, err := w.sessPool.Get()
			if err != nil {
				return err
			}
			defer func() {
				w.sessPool.Put(sessCtx)
				close(doneCh)
			}()
			dbTable := fmt.Sprintf("`%s`.`%s`", dbName, tblName)

			exec, ok := sessCtx.(sqlexec.RestrictedSQLExecutor)
			if !ok {
				return errors.Errorf("not restricted SQL executor: %T", sessCtx)
			}
			// internal sql may not init the analysis related variable correctly.
			err = statsutil.UpdateSCtxVarsForStats(sessCtx)
			if err != nil {
				return err
			}
			failpoint.InjectCall("beforeAnalyzeTable")
			_, _, err = exec.ExecRestrictedSQL(w.ctx, []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession, sqlexec.ExecOptionEnableDDLAnalyze}, "ANALYZE TABLE "+dbTable+";", "ddl analyze table")
			failpoint.InjectCall("afterAnalyzeTable", &err)
			if err != nil {
				logutil.DDLLogger().Warn("analyze table failed",
					zap.Int64("jobID", job.ID),
					zap.String("db", dbName),
					zap.String("table", tblName),
					zap.Error(err),
					zap.Stack("stack"))
				// We can continue to finish the job even if analyze table failed.
				doneCh <- err
			}
			return nil
		})
		w.ddlCtx.setAnalyzeDoneCh(job.ID, doneCh)
	}
	select {
	case err := <-doneCh:
		logutil.DDLLogger().Info("analyze table after create index done", zap.Int64("jobID", job.ID))
		w.ddlCtx.clearAnalyzeStartTime(job.ID)
		w.ddlCtx.clearAnalyzeCumulativeTimeout(job.ID)
		return true, false, err != nil
	case <-w.ctx.Done():
		logutil.DDLLogger().Info("analyze table after create index context done",
			zap.Int64("jobID", job.ID), zap.Error(w.ctx.Err()))
		w.ddlCtx.clearAnalyzeStartTime(job.ID)
		w.ddlCtx.clearAnalyzeCumulativeTimeout(job.ID)
		return true, false, false
	case <-time.After(DefaultAnalyzeCheckInterval):
		failpoint.Inject("mockAnalyzeTimeout", func(val failpoint.Value) {
			if v, ok := val.(int); ok {
				cumulativeTimeout = time.Duration(v) * time.Millisecond
			}
		})
		if start, ok := w.ddlCtx.getAnalyzeStartTime(job.ID); ok {
			if time.Since(start) > cumulativeTimeout {
				logutil.DDLLogger().Warn("analyze table after create index exceed cumulative timeout, proceeding to finish DDL",
					zap.Int64("jobID", job.ID), zap.Duration("elapsed", time.Since(start)))
				// Do not persist job here. Let the caller mark AnalyzeStateTimeout and persist.
				w.ddlCtx.clearAnalyzeStartTime(job.ID)
				w.ddlCtx.clearAnalyzeCumulativeTimeout(job.ID)
				return false, true, false
			}
		} else {
			w.ddlCtx.setAnalyzeStartTime(job.ID, time.Now())
		}
		return false, false, false
	}
}
