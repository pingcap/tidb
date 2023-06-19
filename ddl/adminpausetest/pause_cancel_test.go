// Copyright 2022 PingCAP, Inc.
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

package adminpausetest

import (
	"context"
	"fmt"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	ddlctrl "github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/ddl/util/callback"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var localPCRJobID = &atomic.Int64{}

var localPCRPauseErrChn = make(chan error, 1)
var localPCRPauseResultChn = make(chan []sqlexec.RecordSet, 1)
var localPCRIsPaused = &atomic.Bool{}

func localPCRPauseFunc(adminCommandKit *testkit.TestKit, stmtCase *AdminPauseResumeStmtCase) func(*model.Job) {
	return func(job *model.Job) {
		Logger.Debug("TestPauseCancelAndRerun: OnJobRunBeforeExported, ",
			zap.String("Job Type", job.Type.String()),
			zap.String("Job State", job.State.String()),
			zap.String("Job Schema State", job.SchemaState.String()),
			zap.String("Expected Schema State", stmtCase.schemaState.String()))

		// stmtCase is read-only among the whole test suite which is not necessary to be atomic
		if MatchTargetState(job, stmtCase.schemaState) &&
			stmtCase.isJobPausable && //
			!localPCRIsPaused.Load() {
			localPCRJobID.Store(job.ID)
			var pauseStmt = "admin pause ddl jobs " + strconv.FormatInt(localPCRJobID.Load(), 10)
			var pr, pe = adminCommandKit.Session().Execute(context.Background(), pauseStmt)

			localPCRPauseErrChn <- pe
			localPCRPauseResultChn <- pr

			Logger.Info("TestPauseCancelAndRerun: pause command by hook.OnJobRunBeforeExported, result to channel")

			// In case that it runs into this scope again and again
			localPCRIsPaused.CompareAndSwap(false, true)
		}
	}
}
func localPCRVerifyPauseResult(t *testing.T, adminCommandKit *testkit.TestKit) {
	require.True(t, localPCRIsPaused.Load())

	pauseErr := <-localPCRPauseErrChn
	pauseResult := <-localPCRPauseResultChn

	require.NoError(t, pauseErr)
	result := adminCommandKit.ResultSetToResultWithCtx(context.Background(),
		pauseResult[0], "pause ddl job successfully")
	result.Check(testkit.Rows(fmt.Sprintf("%d successful", localPCRJobID.Load())))

	localPCRIsPaused.CompareAndSwap(true, false)
}

var localPCRCancelErrChn = make(chan error, 1)
var localPCRCancelResultChn = make(chan []sqlexec.RecordSet, 1)
var localPCRIsCancelled = &atomic.Bool{}

func localPCRCancelFunc(adminCommandKit *testkit.TestKit, stmtCase *AdminPauseResumeStmtCase) func(string) {
	return func(jobType string) {
		Logger.Debug("TestPauseCancelAndRerun: OnGetJobBeforeExported, ",
			zap.String("Expected Schema State", stmtCase.schemaState.String()))

		if localPCRIsPaused.Load() && !localPCRIsCancelled.Load() {
			// Only the 'OnGetJobBeforeExported' hook works for `resume`, because the only job has been paused that
			// it could not get into other hooks.
			var cancelStmt = "admin cancel ddl jobs " + strconv.FormatInt(localPCRJobID.Load(), 10)
			rr, re := adminCommandKit.Session().Execute(context.Background(), cancelStmt)

			localPCRCancelErrChn <- re
			localPCRCancelResultChn <- rr

			Logger.Info("TestPauseCancelAndRerun: cancel command by hook.OnGetJobBeforeExported, result to channel")

			localPCRIsCancelled.CompareAndSwap(false, true) // In case that it runs into this scope again and again
		}
	}
}
func localPCRVerifyCancelResult(t *testing.T, adminCommandKit *testkit.TestKit) {
	require.True(t, localPCRIsCancelled.Load())

	cancelErr := <-localPCRCancelErrChn
	cancelResult := <-localPCRCancelResultChn

	require.NoError(t, cancelErr)
	result := adminCommandKit.ResultSetToResultWithCtx(context.Background(),
		cancelResult[0], "resume ddl job successfully")
	result.Check(testkit.Rows(fmt.Sprintf("%d successful", localPCRJobID.Load())))

	localPCRIsCancelled.CompareAndSwap(true, false)
}

func pauseAndCancelStmt(t *testing.T, stmtKit *testkit.TestKit, adminCommandKit *testkit.TestKit, dom *domain.Domain, stmtCase *AdminPauseResumeStmtCase) {
	Logger.Info("TestPauseCancelAndRerun: case start,",
		zap.Int("GlobalID", stmtCase.global_id),
		zap.String("statement", stmtCase.stmt),
		zap.String("schema state", stmtCase.schemaState.String()),
		zap.Strings("pre-condition", stmtCase.preConditionStmts),
		zap.Strings("rollback statement", stmtCase.rollbackStmts),
		zap.Bool("isJobPausable", stmtCase.isJobPausable))

	for _, prepareStmt := range stmtCase.preConditionStmts {
		stmtKit.MustExec(prepareStmt)
	}

	var hook = &callback.TestDDLCallback{Do: dom}
	originalHook := dom.DDL().GetHook()

	hook.OnJobRunBeforeExported = localPCRPauseFunc(adminCommandKit, stmtCase)
	hook.OnGetJobBeforeExported = localPCRCancelFunc(adminCommandKit, stmtCase)
	dom.DDL().SetHook(hook)

	localPCRIsPaused.Store(false)
	localPCRIsCancelled.Store(false)
	Logger.Info("TestPauseCancelAndRerun: statement execute", zap.String("DDL Statement", stmtCase.stmt))
	if stmtCase.isJobPausable {
		stmtKit.MustGetErrCode(stmtCase.stmt, errno.ErrCancelledDDLJob)
		Logger.Info("TestPauseCancelAndRerun: statement execution should have been cancelled.")

		localPCRVerifyPauseResult(t, adminCommandKit)
		localPCRVerifyCancelResult(t, adminCommandKit)
	} else {
		stmtKit.MustExec(stmtCase.stmt)
		Logger.Info("TestPauseCancelAndRerun: statement execution should have been finished successfully.")

		require.False(t, localPCRIsPaused.Load())
		require.False(t, localPCRIsCancelled.Load())
	}

	// Release the hook, so that we could run the `rollbackStmts` successfully.
	dom.DDL().SetHook(originalHook)

	for _, rollbackStmt := range stmtCase.rollbackStmts {
		// no care about the result here, since the `statement` could have been cancelled OR finished successfully.
		stmtKit.Exec(rollbackStmt)
	}
}

// TestPauseCancelAndRerun pause the DDL and cancel it, then run it again
func TestPauseCancelAndRerun(t *testing.T) {
	var store, dom = testkit.CreateMockStoreAndDomainWithSchemaLease(t, 100*time.Millisecond)
	var stmtKit = testkit.NewTestKit(t, store)
	var adminCommandKit = testkit.NewTestKit(t, store)

	ddlctrl.ReorgWaitTimeout = 10 * time.Millisecond
	stmtKit.MustExec("set @@global.tidb_ddl_reorg_batch_size = 2")
	stmtKit.MustExec("set @@global.tidb_ddl_reorg_worker_cnt = 1")
	stmtKit.MustExec("use test")

	require.Nil(t, generateTblUser(stmtKit, 1000))

	for _, stmtCase := range schemaDDLStmtCase {
		pauseAndCancelStmt(t, stmtKit, adminCommandKit, dom, &stmtCase)

		Logger.Info("TestPauseCancelAndRerun: statement execution again after `admin cancel`",
			zap.String("DDL Statement", stmtCase.stmt))
		stmtCase.simpleRunStmt(stmtKit)
	}
	for _, stmtCase := range tableDDLStmt {
		pauseAndCancelStmt(t, stmtKit, adminCommandKit, dom, &stmtCase)

		Logger.Info("TestPauseCancelAndRerun: statement execution again after `admin cancel`",
			zap.String("DDL Statement", stmtCase.stmt))
		stmtCase.simpleRunStmt(stmtKit)
	}
	for _, stmtCase := range indexDDLStmtCase {
		pauseAndCancelStmt(t, stmtKit, adminCommandKit, dom, &stmtCase)

		Logger.Info("TestPauseCancelAndRerun: statement execution again after `admin cancel`",
			zap.String("DDL Statement", stmtCase.stmt))
		stmtCase.simpleRunStmt(stmtKit)
	}
	for _, stmtCase := range columnDDLStmtCase {
		pauseAndCancelStmt(t, stmtKit, adminCommandKit, dom, &stmtCase)

		Logger.Info("TestPauseCancelAndRerun: statement execution again after `admin cancel`",
			zap.String("DDL Statement", stmtCase.stmt))
		stmtCase.simpleRunStmt(stmtKit)
	}
	for _, stmtCase := range placeRulDDLStmtCase {
		pauseAndCancelStmt(t, stmtKit, adminCommandKit, dom, &stmtCase)

		Logger.Info("TestPauseCancelAndRerun: statement execution again after `admin cancel`",
			zap.String("DDL Statement", stmtCase.stmt))
		stmtCase.simpleRunStmt(stmtKit)
	}

	// It will be out of control if the tuples generated is not in the range of some cases for partition. Then it would
	// fail. Just truncate the tuples here because we don't care about the partition itself but the DDL.
	stmtKit.MustExec("truncate " + adminPauseTestTable)

	require.Nil(t, generateTblUserParition(stmtKit, 0))
	for _, stmtCase := range tablePartitionDDLStmtCase {
		pauseAndCancelStmt(t, stmtKit, adminCommandKit, dom, &stmtCase)

		Logger.Info("TestPauseCancelAndRerun: statement execution again after `admin cancel`",
			zap.String("DDL Statement", stmtCase.stmt))
		stmtCase.simpleRunStmt(stmtKit)
	}

	Logger.Info("TestPauseCancelAndRerun: all cases finished.")
}
