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
	"sync"
	"testing"
	"time"

	ddlctrl "github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/ddl/util/callback"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var localPRCJobID int64 = 0
var localPRCAdminCommandMutex sync.RWMutex

var localPRCPauseResult []sqlexec.RecordSet
var localPRCPauseErr error
var localPRCIsPaused = false

func localPRCPauseFunc(adminCommandKit *testkit.TestKit, stmtCase *StmtCase) func(*model.Job) {
	return func(job *model.Job) {
		Logger.Debug("pauseResumeAndCancel: OnJobRunBeforeExported, ",
			zap.String("Job Type", job.Type.String()),
			zap.String("Job State", job.State.String()),
			zap.String("Job Schema State", job.SchemaState.String()),
			zap.String("Expected Schema State", stmtCase.schemaState.String()))

		// All hooks are running inside a READ mutex `d.mu()`, we need to lock for the variable's modification.
		localPRCAdminCommandMutex.Lock()
		defer localPRCAdminCommandMutex.Unlock()
		if MatchTargetState(job, stmtCase.schemaState) &&
			stmtCase.isJobPausable && //
			!localPRCIsPaused {
			localPRCJobID = job.ID
			stmt := "admin pause ddl jobs " + strconv.FormatInt(localPRCJobID, 10)
			localPRCPauseResult, localPRCPauseErr = adminCommandKit.Session().Execute(context.Background(), stmt)

			Logger.Info("pauseResumeAndCancel: pause command by hook.OnJobRunBeforeExported,",
				zap.Error(localPRCPauseErr), zap.String("Statement", stmtCase.stmt), zap.Int64("Job ID", localPRCJobID))

			// In case that it runs into this scope again and again
			localPRCIsPaused = true
		}
	}
}
func localPRCVerifyPauseResult(t *testing.T, adminCommandKit *testkit.TestKit) {
	localPRCAdminCommandMutex.RLock()
	defer localPRCAdminCommandMutex.RUnlock()

	require.True(t, localPRCIsPaused)
	require.NoError(t, localPRCPauseErr)
	result := adminCommandKit.ResultSetToResultWithCtx(context.Background(),
		localPRCPauseResult[0], "pause ddl job successfully")
	result.Check(testkit.Rows(fmt.Sprintf("%d successful", localPRCJobID)))

	localPRCIsPaused = false
}

var localPRCIsResumed = false
var localPRCResumeResult []sqlexec.RecordSet
var localPRCResumeErr error

func localPRCResumeFunc(adminCommandKit *testkit.TestKit, stmtCase *StmtCase) func(*model.Job) {
	return func(job *model.Job) {
		Logger.Debug("pauseResumeAndCancel: OnJobUpdatedExported, ",
			zap.String("Job Type", job.Type.String()),
			zap.String("Job State", job.State.String()),
			zap.String("Job Schema State", job.SchemaState.String()),
			zap.String("Expected Schema State", stmtCase.schemaState.String()))

		localPRCAdminCommandMutex.Lock()
		defer localPRCAdminCommandMutex.Unlock()
		if job.IsPaused() && localPRCIsPaused && !localPRCIsResumed {
			stmt := "admin resume ddl jobs " + strconv.FormatInt(localPRCJobID, 10)
			localPRCResumeResult, localPRCResumeErr = adminCommandKit.Session().Execute(context.Background(), stmt)

			Logger.Info("pauseResumeAndCancel: resume command by hook.OnJobUpdatedExported: ",
				zap.Error(localPRCResumeErr), zap.String("Statement", stmtCase.stmt), zap.Int64("Job ID", localPRCJobID))

			// In case that it runs into this scope again and again
			localPRCIsResumed = true
		}
	}
}
func localPRCVerifyResumeResult(t *testing.T, adminCommandKit *testkit.TestKit) {
	localPRCAdminCommandMutex.RLock()
	defer localPRCAdminCommandMutex.RUnlock()

	require.True(t, localPRCIsResumed)
	require.NoError(t, localPRCResumeErr)
	result := adminCommandKit.ResultSetToResultWithCtx(context.Background(),
		localPRCResumeResult[0], "resume ddl job successfully")
	result.Check(testkit.Rows(fmt.Sprintf("%d successful", localPRCJobID)))

	localPRCIsResumed = false
}

var localPRCCancelResult []sqlexec.RecordSet
var localPRCCancelErr error
var localPRCIsCancelled = false

func localPRCCancelFunc(adminCommandKit *testkit.TestKit, stmtCase *StmtCase) func(string) {
	return func(jobType string) {
		localPRCAdminCommandMutex.Lock()
		defer localPRCAdminCommandMutex.Unlock()
		if localPRCIsPaused && localPRCIsResumed && !localPRCIsCancelled {
			stmt := "admin cancel ddl jobs " + strconv.FormatInt(localPRCJobID, 10)
			localPRCCancelResult, localPRCCancelErr = adminCommandKit.Session().Execute(context.Background(), stmt)

			Logger.Info("pauseResumeAndCancel: cancel command by hook.OnGetJobBeforeExported: ",
				zap.Error(localPRCCancelErr), zap.String("Statement", stmtCase.stmt), zap.Int64("Job ID", localPRCJobID))

			// In case that it runs into this scope again and again
			localPRCIsCancelled = true
		}
	}
}

func localPRCVerifyCancelResult(t *testing.T, adminCommandKit *testkit.TestKit) {
	localPRCAdminCommandMutex.RLock()
	defer localPRCAdminCommandMutex.RUnlock()

	require.True(t, localPRCIsCancelled)
	require.NoError(t, localPRCCancelErr)
	result := adminCommandKit.ResultSetToResultWithCtx(context.Background(),
		localPRCCancelResult[0], "resume ddl job successfully")
	result.Check(testkit.Rows(fmt.Sprintf("%d successful", localPRCJobID)))

	localPRCIsCancelled = false
}

func pauseResumeAndCancel(t *testing.T, stmtKit *testkit.TestKit, adminCommandKit *testkit.TestKit, dom *domain.Domain, stmtCase *StmtCase, doCancel bool) {
	Logger.Info("pauseResumeAndCancel: case start,",
		zap.Int("Global ID", stmtCase.globalID),
		zap.String("Statement", stmtCase.stmt),
		zap.String("Schema state", stmtCase.schemaState.String()),
		zap.Strings("Pre-condition", stmtCase.preConditionStmts),
		zap.Strings("Rollback statement", stmtCase.rollbackStmts),
		zap.Bool("Job pausable", stmtCase.isJobPausable))

	for _, prepareStmt := range stmtCase.preConditionStmts {
		Logger.Debug("pauseResumeAndCancel: ", zap.String("Prepare schema before test", prepareStmt))
		stmtKit.MustExec(prepareStmt)
	}

	var hook = &callback.TestDDLCallback{Do: dom}
	originalHook := dom.DDL().GetHook()

	hook.OnJobRunBeforeExported = localPRCPauseFunc(adminCommandKit, stmtCase)
	var rf = localPRCResumeFunc(adminCommandKit, stmtCase)
	hook.OnJobUpdatedExported.Store(&rf)

	Logger.Info("pauseResumeAndCancel: statement execute", zap.String("DDL Statement", stmtCase.stmt))
	if stmtCase.isJobPausable {
		if doCancel {
			hook.OnGetJobBeforeExported = localPRCCancelFunc(adminCommandKit, stmtCase)
			dom.DDL().SetHook(hook.Clone())

			stmtKit.MustGetErrCode(stmtCase.stmt, errno.ErrCancelledDDLJob)
			Logger.Info("pauseResumeAndCancel: statement execution should be cancelled.")

			localPRCVerifyCancelResult(t, adminCommandKit)
		} else {
			dom.DDL().SetHook(hook.Clone())

			stmtKit.MustExec(stmtCase.stmt)
			Logger.Info("pauseResumeAndCancel: statement execution should finish successfully.")

			// If not `doCancel`, then we should not run the `admin cancel`, which the indication should be false.
			require.False(t, localPRCIsCancelled)
		}

		// The reason why we don't check the result of `admin pause`, `admin resume` and `admin cancel` inside the
		// hook is that the hook may not be triggered, which we may not know it.
		localPRCVerifyPauseResult(t, adminCommandKit)
		localPRCVerifyResumeResult(t, adminCommandKit)
	} else {
		dom.DDL().SetHook(hook.Clone())
		stmtKit.MustExec(stmtCase.stmt)

		require.False(t, localPRCIsPaused)
		require.False(t, localPRCIsResumed)
		require.False(t, localPRCIsCancelled)
	}

	// Should not affect the 'stmtCase.rollbackStmts'
	dom.DDL().SetHook(originalHook)

	// Statement in `stmtCase` will be finished successfully all the way, need to roll it back.
	for _, rollbackStmt := range stmtCase.rollbackStmts {
		Logger.Info("pauseResumeAndCancel: ", zap.String("rollback statement", rollbackStmt))
		stmtKit.Exec(rollbackStmt)
	}

	Logger.Info("pauseResumeAndCancel: statement case finished, ", zap.String("Statement", stmtCase.stmt))
}

// TestPauseAndResumePositive
// - positive cases
// - pause the job, and resume it, then it should finish successfully
// - iterate all the `StmtCase` statements
func TestPauseAndResume(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, dbTestLease)
	stmtKit := testkit.NewTestKit(t, store)
	adminCommandKit := testkit.NewTestKit(t, store)

	ddlctrl.ReorgWaitTimeout = 10 * time.Millisecond
	stmtKit.MustExec("set @@global.tidb_ddl_reorg_batch_size = 2")
	stmtKit.MustExec("set @@global.tidb_ddl_reorg_worker_cnt = 1")
	stmtKit = testkit.NewTestKit(t, store)
	stmtKit.MustExec("use test")

	require.Nil(t, generateTblUser(stmtKit, 1000))

	for _, stmtCase := range schemaDDLStmtCase {
		pauseResumeAndCancel(t, stmtKit, adminCommandKit, dom, &stmtCase, false)
	}
	for _, stmtCase := range tableDDLStmt {
		pauseResumeAndCancel(t, stmtKit, adminCommandKit, dom, &stmtCase, false)
	}
	for _, stmtCase := range indexDDLStmtCase {
		pauseResumeAndCancel(t, stmtKit, adminCommandKit, dom, &stmtCase, false)
	}
	for _, stmtCase := range columnDDLStmtCase {
		pauseResumeAndCancel(t, stmtKit, adminCommandKit, dom, &stmtCase, false)
	}
	for _, stmtCase := range placeRulDDLStmtCase {
		pauseResumeAndCancel(t, stmtKit, adminCommandKit, dom, &stmtCase, false)
	}

	// It will be out of control if the tuples generated is not in the range of some cases for partition. Then it would
	// fail. Just truncate the tuples here because we don't care about the partition itself but the DDL.
	stmtKit.MustExec("truncate " + adminPauseTestTable)

	require.Nil(t, generateTblUserParition(stmtKit, 0))
	for _, stmtCase := range tablePartitionDDLStmtCase {
		pauseResumeAndCancel(t, stmtKit, adminCommandKit, dom, &stmtCase, false)
	}

	Logger.Info("TestPauseAndResumePositive: all cases finished.")
}

// TestPauseAndResumePositive
// - positive cases
// - pause the job, and resume it, and then cancel it. The statement should be cancelled
// - run the `stmt` again
// - iterate all the `StmtCase` statements
func TestPauseResumeCancelAndRerun(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, dbTestLease)

	stmtKit := testkit.NewTestKit(t, store)
	adminCommandKit := testkit.NewTestKit(t, store)

	ddlctrl.ReorgWaitTimeout = 10 * time.Millisecond
	stmtKit.MustExec("set @@global.tidb_ddl_reorg_batch_size = 2")
	stmtKit.MustExec("set @@global.tidb_ddl_reorg_worker_cnt = 1")
	stmtKit = testkit.NewTestKit(t, store)
	stmtKit.MustExec("use test")

	require.Nil(t, generateTblUser(stmtKit, 1000))

	for _, stmtCase := range schemaDDLStmtCase {
		pauseResumeAndCancel(t, stmtKit, adminCommandKit, dom, &stmtCase, true)

		Logger.Info("TestPauseResumeCancelAndRerun: statement execution again after `admin cancel`",
			zap.String("DDL Statement", stmtCase.stmt))
		stmtCase.simpleRunStmt(stmtKit)
	}
	for _, stmtCase := range tableDDLStmt {
		pauseResumeAndCancel(t, stmtKit, adminCommandKit, dom, &stmtCase, true)

		Logger.Info("TestPauseResumeCancelAndRerun: statement execution again after `admin cancel`",
			zap.String("DDL Statement", stmtCase.stmt))
		stmtCase.simpleRunStmt(stmtKit)
	}
	for _, stmtCase := range indexDDLStmtCase {
		pauseResumeAndCancel(t, stmtKit, adminCommandKit, dom, &stmtCase, true)

		Logger.Info("TestPauseResumeCancelAndRerun: statement execution again after `admin cancel`",
			zap.String("DDL Statement", stmtCase.stmt))
		stmtCase.simpleRunStmt(stmtKit)
	}
	for _, stmtCase := range columnDDLStmtCase {
		pauseResumeAndCancel(t, stmtKit, adminCommandKit, dom, &stmtCase, true)

		Logger.Info("TestPauseResumeCancelAndRerun: statement execution again after `admin cancel`",
			zap.String("DDL Statement", stmtCase.stmt))
		stmtCase.simpleRunStmt(stmtKit)
	}
	for _, stmtCase := range placeRulDDLStmtCase {
		pauseResumeAndCancel(t, stmtKit, adminCommandKit, dom, &stmtCase, true)

		Logger.Info("TestPauseResumeCancelAndRerun: statement execution again after `admin cancel`",
			zap.String("DDL Statement", stmtCase.stmt))
		stmtCase.simpleRunStmt(stmtKit)
	}

	// It will be out of control if the tuples generated is not in the range of some cases for partition. Then it would
	// fail. Just truncate the tuples here because we don't care about the partition itself but the DDL.
	stmtKit.MustExec("truncate " + adminPauseTestTable)

	require.Nil(t, generateTblUserParition(stmtKit, 0))
	for _, stmtCase := range tablePartitionDDLStmtCase {
		pauseResumeAndCancel(t, stmtKit, adminCommandKit, dom, &stmtCase, true)

		Logger.Info("TestPauseResumeCancelAndRerun: statement execution again after `admin cancel`",
			zap.String("DDL Statement", stmtCase.stmt))
		stmtCase.simpleRunStmt(stmtKit)
	}

	Logger.Info("TestPauseResumeCancelAndRerun: all cases finished.")
}
