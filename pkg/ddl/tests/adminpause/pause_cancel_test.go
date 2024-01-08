// Copyright 2023 PingCAP, Inc.
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

package adminpause

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"

	testddlutil "github.com/pingcap/tidb/pkg/ddl/testutil"
	"github.com/pingcap/tidb/pkg/ddl/util/callback"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// pauseAndCancelStmt pauses and cancel the `stmtCase`
// The variables cancelResultChn, pauseResultChn should not be out of the function domain because of parallel case
// execution
func pauseAndCancelStmt(t *testing.T, stmtKit *testkit.TestKit, adminCommandKit *testkit.TestKit, dom *domain.Domain, stmtCase *StmtCase) {
	Logger.Info("pauseAndCancelStmt: case start,",
		zap.Int("Global ID", stmtCase.globalID),
		zap.String("Statement", stmtCase.stmt),
		zap.String("Schema state", stmtCase.schemaState.String()),
		zap.Strings("Pre-condition", stmtCase.preConditionStmts),
		zap.Strings("Rollback statement", stmtCase.rollbackStmts),
		zap.Bool("Job pausable", stmtCase.isJobPausable))

	var jobID = &atomic.Int64{}

	var isPaused = &atomic.Bool{}
	var pauseResultChn = make(chan []sqlexec.RecordSet, 1)
	var pauseErrChn = make(chan error, 1)
	var pauseFunc = func(job *model.Job) {
		Logger.Debug("pauseAndCancelStmt: OnJobRunBeforeExported, ",
			zap.String("Job Type", job.Type.String()),
			zap.String("Job State", job.State.String()),
			zap.String("Job Schema State", job.SchemaState.String()),
			zap.String("Expected Schema State", stmtCase.schemaState.String()))

		// stmtCase is read-only among the whole test suite which is not necessary to be atomic
		if testddlutil.TestMatchCancelState(t, job, stmtCase.schemaState, stmtCase.stmt) &&
			stmtCase.isJobPausable && //
			!isPaused.Load() {
			jobID.Store(job.ID)
			var pauseStmt = "admin pause ddl jobs " + strconv.FormatInt(jobID.Load(), 10)
			var pr, pe = adminCommandKit.Session().Execute(context.Background(), pauseStmt)

			pauseErrChn <- pe
			pauseResultChn <- pr

			Logger.Info("pauseAndCancelStmt: pause command by hook.OnJobRunBeforeExported, result to channel")

			// In case that it runs into this scope again and again
			isPaused.CompareAndSwap(false, true)
		}
	}
	var verifyPauseResult = func(t *testing.T, adminCommandKit *testkit.TestKit) {
		require.True(t, isPaused.Load())

		pauseErr := <-pauseErrChn
		pauseResult := <-pauseResultChn

		require.NoError(t, pauseErr)
		result := adminCommandKit.ResultSetToResultWithCtx(context.Background(),
			pauseResult[0], "pause ddl job successfully")
		result.Check(testkit.Rows(fmt.Sprintf("%d successful", jobID.Load())))

		isPaused.CompareAndSwap(true, false)
	}

	var isCancelled = &atomic.Bool{}
	var cancelResultChn = make(chan []sqlexec.RecordSet, 1)
	var cancelErrChn = make(chan error, 1)
	var cancelFunc = func(jobType string) {
		Logger.Debug("pauseAndCancelStmt: OnGetJobBeforeExported, ",
			zap.String("Expected Schema State", stmtCase.schemaState.String()))

		if isPaused.Load() && !isCancelled.Load() {
			// Only the 'OnGetJobBeforeExported' hook works for `resume`, because the only job has been paused that
			// it could not get into other hooks.
			var cancelStmt = "admin cancel ddl jobs " + strconv.FormatInt(jobID.Load(), 10)
			rr, re := adminCommandKit.Session().Execute(context.Background(), cancelStmt)

			cancelErrChn <- re
			cancelResultChn <- rr

			Logger.Info("pauseAndCancelStmt: cancel command by hook.OnGetJobBeforeExported, result to channel")

			isCancelled.CompareAndSwap(false, true) // In case that it runs into this scope again and again
		}
	}
	var verifyCancelResult = func(t *testing.T, adminCommandKit *testkit.TestKit) {
		require.True(t, isCancelled.Load())

		cancelErr := <-cancelErrChn
		cancelResult := <-cancelResultChn

		require.NoError(t, cancelErr)
		result := adminCommandKit.ResultSetToResultWithCtx(context.Background(),
			cancelResult[0], "resume ddl job successfully")
		result.Check(testkit.Rows(fmt.Sprintf("%d successful", jobID.Load())))

		isCancelled.CompareAndSwap(true, false)
	}

	for _, prepareStmt := range stmtCase.preConditionStmts {
		stmtKit.MustExec(prepareStmt)
	}

	var hook = &callback.TestDDLCallback{Do: dom}
	originalHook := dom.DDL().GetHook()

	hook.OnJobRunBeforeExported = pauseFunc
	hook.OnGetJobBeforeExported = cancelFunc
	dom.DDL().SetHook(hook.Clone())

	isPaused.Store(false)
	isCancelled.Store(false)
	Logger.Debug("pauseAndCancelStmt: statement execute", zap.String("DDL Statement", stmtCase.stmt))
	if stmtCase.isJobPausable {
		stmtKit.MustGetErrCode(stmtCase.stmt, errno.ErrCancelledDDLJob)
		Logger.Info("pauseAndCancelStmt: statement execution should have been cancelled.")

		verifyPauseResult(t, adminCommandKit)
		verifyCancelResult(t, adminCommandKit)
	} else {
		stmtKit.MustExec(stmtCase.stmt)
		Logger.Info("pauseAndCancelStmt: statement execution should have been finished successfully.")

		require.False(t, isPaused.Load())
		require.False(t, isCancelled.Load())
	}

	// Release the hook, so that we could run the `rollbackStmts` successfully.
	dom.DDL().SetHook(originalHook)

	for _, rollbackStmt := range stmtCase.rollbackStmts {
		// no care about the result here, since the `statement` could have been cancelled OR finished successfully.
		_, _ = stmtKit.Exec(rollbackStmt)
	}

	Logger.Info("pauseAndCancelStmt: statement case finished, ",
		zap.String("Global ID", strconv.Itoa(stmtCase.globalID)))
}

func TestPauseCancelAndRerunSchemaStmt(t *testing.T) {
	var dom, stmtKit, adminCommandKit = prepareDomain(t)

	require.Nil(t, generateTblUser(stmtKit, 10))

	for _, stmtCase := range schemaDDLStmtCase {
		pauseAndCancelStmt(t, stmtKit, adminCommandKit, dom, &stmtCase)

		Logger.Info("TestPauseCancelAndRerunSchemaStmt: statement execution again after `admin cancel`",
			zap.String("DDL Statement", stmtCase.stmt))
		stmtCase.simpleRunStmt(stmtKit)
	}
	for _, stmtCase := range tableDDLStmt {
		pauseAndCancelStmt(t, stmtKit, adminCommandKit, dom, &stmtCase)

		Logger.Info("TestPauseCancelAndRerunSchemaStmt: statement execution again after `admin cancel`",
			zap.String("DDL Statement", stmtCase.stmt))
		stmtCase.simpleRunStmt(stmtKit)
	}

	for _, stmtCase := range placeRulDDLStmtCase {
		pauseAndCancelStmt(t, stmtKit, adminCommandKit, dom, &stmtCase)

		Logger.Info("TestPauseCancelAndRerunSchemaStmt: statement execution again after `admin cancel`",
			zap.String("DDL Statement", stmtCase.stmt))
		stmtCase.simpleRunStmt(stmtKit)
	}
}

func TestPauseCancelAndRerunIndexStmt(t *testing.T) {
	var dom, stmtKit, adminCommandKit = prepareDomain(t)

	require.Nil(t, generateTblUser(stmtKit, 10))

	for _, stmtCase := range indexDDLStmtCase {
		pauseAndCancelStmt(t, stmtKit, adminCommandKit, dom, &stmtCase)

		Logger.Info("TestPauseCancelAndRerunIndexStmt: statement execution again after `admin cancel`",
			zap.String("DDL Statement", stmtCase.stmt))
		stmtCase.simpleRunStmt(stmtKit)
	}
}

func TestPauseCancelAndRerunColumnStmt(t *testing.T) {
	var dom, stmtKit, adminCommandKit = prepareDomain(t)

	require.Nil(t, generateTblUser(stmtKit, 10))

	for _, stmtCase := range columnDDLStmtCase {
		pauseAndCancelStmt(t, stmtKit, adminCommandKit, dom, &stmtCase)

		Logger.Info("TestPauseCancelAndRerunColumnStmt: statement execution again after `admin cancel`",
			zap.String("DDL Statement", stmtCase.stmt))
		stmtCase.simpleRunStmt(stmtKit)
	}
}

func TestPauseCancelAndRerunPartitionTableStmt(t *testing.T) {
	var dom, stmtKit, adminCommandKit = prepareDomain(t)

	require.Nil(t, generateTblUser(stmtKit, 0))

	require.Nil(t, generateTblUserParition(stmtKit))
	for _, stmtCase := range tablePartitionDDLStmtCase {
		pauseAndCancelStmt(t, stmtKit, adminCommandKit, dom, &stmtCase)

		Logger.Info("TestPauseCancelAndRerunPartitionTableStmt: statement execution again after `admin cancel`",
			zap.String("DDL Statement", stmtCase.stmt))
		stmtCase.simpleRunStmt(stmtKit)
	}
}
