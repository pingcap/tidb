// Copyright 2026 PingCAP, Inc.
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
	"context"
	"errors"
	"testing"

	ddlmock "github.com/pingcap/tidb/pkg/ddl/mock"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/owner"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

func newTestDDLCtx(ctx context.Context, store kv.Storage, ownerMgr owner.Manager) *ddlCtx {
	d := &ddlCtx{
		ctx:          ctx,
		store:        store,
		ownerManager: ownerMgr,
	}
	d.jobCtx.jobCtxMap = make(map[int64]*ReorgContext)
	return d
}

func TestStepCheckpointSameTxnAndFallback(t *testing.T) {
	ctx := context.Background()

	t.Run("same txn rollback on empty memdb checkpoint handles same-length overwrite", func(t *testing.T) {
		store, err := mockstore.NewMockStore()
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, store.Close()) })

		sessCtx := mock.NewContext()
		sessCtx.Store = store
		if sessCtx.GetSessionVars().TxnCtx == nil {
			sessCtx.GetSessionVars().TxnCtx = &variable.TransactionContext{}
		}
		w := newWorker(ctx, generalWorker, nil, nil, newTestDDLCtx(ctx, store, nil))
		w.sess = sess.NewSession(sessCtx)
		t.Cleanup(w.sess.Rollback)

		job := &model.Job{ID: 101, Version: model.GetJobVerInUse(), Type: model.ActionModifyEngineAttribute}
		entryBytes, err := job.Encode(true)
		require.NoError(t, err)
		jobW := model.NewJobW(job, entryBytes)

		txn, err := w.prepareTxn(job)
		require.NoError(t, err)
		origStartTS := txn.StartTS()

		txnCtx := sessCtx.GetSessionVars().TxnCtx
		txnCtx.TableDeltaMap = map[int64]variable.TableDelta{
			1: {Delta: 2, Count: 5},
		}
		txnCtx.InsertTTLRowsCount = 3

		cp, err := captureStepCheckpoint(w.sess, txn, jobW)
		require.NoError(t, err)
		jobCtx := &jobContext{ctx: ctx, logger: zap.NewNop(), metaMut: meta.NewMutator(txn)}

		// Simulate step mutations (including same-length overwrite + StmtCommit).
		require.NoError(t, txn.Set(kv.Key("step-key"), []byte("val1")))
		require.NoError(t, txn.Set(kv.Key("step-key"), []byte("val2")))
		w.sess.StmtCommit(ctx)
		txnCtx.TableDeltaMap[2] = variable.TableDelta{Delta: 9, Count: 11}
		txnCtx.InsertTTLRowsCount = 99

		curTxn, ok := cp.canRollbackInSameTxn()
		require.True(t, ok)
		require.Equal(t, origStartTS, curTxn.StartTS())

		require.NoError(t, cp.discardWrites(w, jobCtx, jobW))

		afterTxn, err := w.sess.Txn()
		require.NoError(t, err)
		require.Equal(t, origStartTS, afterTxn.StartTS())
		_, err = afterTxn.Get(ctx, kv.Key("step-key"))
		require.True(t, kv.ErrNotExist.Equal(err), "%v", err)

		require.Equal(t, map[int64]variable.TableDelta{1: {Delta: 2, Count: 5}}, sessCtx.GetSessionVars().TxnCtx.TableDeltaMap)
		require.Equal(t, 3, sessCtx.GetSessionVars().TxnCtx.InsertTTLRowsCount)
		require.NotNil(t, jobCtx.metaMut)
	})

	t.Run("dirty pre-step memdb falls back to reopen instead of checkpointing", func(t *testing.T) {
		store, err := mockstore.NewMockStore()
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, store.Close()) })

		ctrl := gomock.NewController(t)
		sysTblMgr := ddlmock.NewMockManager(ctrl)

		sessCtx := mock.NewContext()
		sessCtx.Store = store
		if sessCtx.GetSessionVars().TxnCtx == nil {
			sessCtx.GetSessionVars().TxnCtx = &variable.TransactionContext{}
		}
		w := newWorker(ctx, generalWorker, nil, nil, newTestDDLCtx(ctx, store, nil))
		w.sess = sess.NewSession(sessCtx)
		t.Cleanup(w.sess.Rollback)

		job := &model.Job{ID: 1011, Version: model.GetJobVerInUse(), Type: model.ActionModifyEngineAttribute}
		entryBytes, err := job.Encode(true)
		require.NoError(t, err)
		jobW := model.NewJobW(job, entryBytes)

		// Commit baseline value first, then seed pre-step MemDB write before capture.
		require.NoError(t, kv.RunInNewTxn(ctx, store, true, func(_ context.Context, preTxn kv.Transaction) error {
			return preTxn.Set(kv.Key("base-key"), []byte("base-val"))
		}))

		txn, err := w.prepareTxn(job)
		require.NoError(t, err)
		origStartTS := txn.StartTS()
		require.NoError(t, txn.Set(kv.Key("base-key"), []byte("pre-valu")))

		cp, err := captureStepCheckpoint(w.sess, txn, jobW)
		require.NoError(t, err)
		_, canSameTxn := cp.canRollbackInSameTxn()
		require.False(t, canSameTxn)

		// Same-length overwrite inside step would corrupt MemDB checkpoint if allowed.
		require.NoError(t, txn.Set(kv.Key("base-key"), []byte("new-valu")))
		w.sess.StmtCommit(ctx)

		sysTblMgr.EXPECT().
			GetJobBytesByIDWithSe(gomock.Any(), gomock.Any(), job.ID).
			Return(entryBytes, nil)

		jobCtx := &jobContext{ctx: ctx, logger: zap.NewNop(), sysTblMgr: sysTblMgr, metaMut: meta.NewMutator(txn)}
		require.NoError(t, cp.discardWrites(w, jobCtx, jobW))

		afterTxn, err := w.sess.Txn()
		require.NoError(t, err)
		require.NotEqual(t, origStartTS, afterTxn.StartTS())
		baseVal, err := afterTxn.Get(ctx, kv.Key("base-key"))
		require.NoError(t, err)
		require.Equal(t, []byte("base-val"), baseVal.Value)
	})

	t.Run("fallback reopens txn and guards durable job bytes", func(t *testing.T) {
		store, err := mockstore.NewMockStore()
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, store.Close()) })

		ctrl := gomock.NewController(t)
		sysTblMgr := ddlmock.NewMockManager(ctrl)

		sessCtx := mock.NewContext()
		sessCtx.Store = store
		if sessCtx.GetSessionVars().TxnCtx == nil {
			sessCtx.GetSessionVars().TxnCtx = &variable.TransactionContext{}
		}
		w := newWorker(ctx, generalWorker, nil, nil, newTestDDLCtx(ctx, store, nil))
		w.sess = sess.NewSession(sessCtx)
		t.Cleanup(w.sess.Rollback)

		job := &model.Job{ID: 102, Version: model.GetJobVerInUse(), Type: model.ActionModifyEngineAttribute}
		rawBytes, err := job.Encode(true)
		require.NoError(t, err)
		jobW := model.NewJobW(job, rawBytes)

		txn, err := w.prepareTxn(job)
		require.NoError(t, err)
		cp, err := captureStepCheckpoint(w.sess, txn, jobW)
		require.NoError(t, err)
		jobCtx := &jobContext{
			ctx:       ctx,
			logger:    zap.NewNop(),
			sysTblMgr: sysTblMgr,
			metaMut:   meta.NewMutator(txn),
		}

		// Force fallback by replacing the session transaction with a new StartTS.
		w.sess.Rollback()
		stepTxn, err := w.prepareTxn(job)
		require.NoError(t, err)
		stepStartTS := stepTxn.StartTS()
		_, ok := cp.canRollbackInSameTxn()
		require.False(t, ok)

		sysTblMgr.EXPECT().GetJobBytesByIDWithSe(ctx, w.sess, job.ID).Return(rawBytes, nil)
		require.NoError(t, cp.discardWrites(w, jobCtx, jobW))
		reopenedTxn, err := w.sess.Txn()
		require.NoError(t, err)
		require.NotEqual(t, stepStartTS, reopenedTxn.StartTS())
		require.NotNil(t, jobCtx.metaMut)

		// Concurrent job metadata modification must fail closed before overwrite.
		sysTblMgr.EXPECT().GetJobBytesByIDWithSe(ctx, w.sess, job.ID).Return([]byte("concurrent-update"), nil)
		err = cp.discardWrites(w, jobCtx, jobW)
		require.ErrorContains(t, err, "job meta changed by others")
	})
}

func TestStepCheckpointRestoreJobPreservesDiagnostics(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	sessCtx := mock.NewContext()
	sessCtx.Store = store
	if sessCtx.GetSessionVars().TxnCtx == nil {
		sessCtx.GetSessionVars().TxnCtx = &variable.TransactionContext{}
	}
	se := sess.NewSession(sessCtx)
	require.NoError(t, se.Begin(context.Background()))
	t.Cleanup(se.Rollback)
	txn, err := se.Txn()
	require.NoError(t, err)

	entryJob := &model.Job{
		ID:                201,
		Version:           model.JobVersion2,
		Type:              model.ActionModifyEngineAttribute,
		State:             model.JobStateRunning,
		SchemaState:       model.StatePublic,
		LastSchemaVersion: 42,
		ReorgMeta:         &model.DDLReorgMeta{UseCloudStorage: false},
	}
	entryJob.FillArgs(&model.ModifyTableEngineAttributeArgs{EngineAttribute: `{"storage_class":"IA"}`})
	_, err = entryJob.Encode(true)
	require.NoError(t, err)

	// Capture without pre-existing jobW.Bytes so captureStepCheckpoint exercises
	// Encode(false) while keeping RawArgs intact.
	jobW := model.NewJobW(entryJob, nil)
	cp, err := captureStepCheckpoint(se, txn, jobW)
	require.NoError(t, err)
	require.False(t, cp.hasDurableBytes)

	job := jobW.Job
	job.FillArgs(&model.ModifyTableEngineAttributeArgs{EngineAttribute: `{"storage_class":"STANDARD"}`})
	jobCtx := &jobContext{jobArgs: &model.ModifyTableEngineAttributeArgs{EngineAttribute: "stale"}}
	job.State = model.JobStateCancelled
	job.AdminOperator = model.AdminCommandByEndUser
	job.SchemaState = model.StateDeleteOnly
	job.Error = nil
	job.ErrorCount = 3
	job.RealStartTS = 777
	job.LastSchemaVersion = 99
	job.ReorgMeta.UseCloudStorage = true

	restoredJob, err := cp.restoreJob(jobCtx, job, func(restored, _ *model.Job) {
		restored.Warning = toTError(dbterror.ErrCancelledDDLJob)
	})
	require.NoError(t, err)
	require.NotSame(t, job, restoredJob)
	// Verify restoreJob leaves the post-step Job struct untouched for concurrent readers.
	require.Equal(t, model.StateDeleteOnly, job.SchemaState)
	require.Equal(t, int64(99), job.LastSchemaVersion)

	require.Equal(t, model.JobStateCancelled, restoredJob.State)
	require.Equal(t, model.AdminCommandByEndUser, restoredJob.AdminOperator)
	require.True(t, dbterror.ErrCancelledDDLJob.Equal(restoredJob.Error), "%v", restoredJob.Error)
	require.Equal(t, int64(3), restoredJob.ErrorCount)
	require.Equal(t, uint64(777), restoredJob.RealStartTS)
	require.Zero(t, restoredJob.LastSchemaVersion)
	require.True(t, restoredJob.ReorgMeta.UseCloudStorage)
	require.Equal(t, model.StatePublic, restoredJob.SchemaState)
	require.NotNil(t, restoredJob.Warning)
	require.Nil(t, jobCtx.jobArgs)

	restoredArgs, err := model.GetModifyTableEngineAttributeArgs(restoredJob)
	require.NoError(t, err)
	require.Equal(t, `{"storage_class":"IA"}`, restoredArgs.EngineAttribute)
}

func TestResolveStepOutcomeContracts(t *testing.T) {
	t.Run("panic on unmigrated action uses stepCheckpoint", func(t *testing.T) {
		res := runStepResult{ver: 15, updateRawArgs: true, panicked: true}
		job := &model.Job{Type: model.ActionCreateTable, State: model.JobStateCancelling}
		outcome, ok := resolveStepOutcome(job, &jobContext{}, res)
		require.True(t, ok)
		require.Equal(t, stepWritesDiscard, outcome.writes)
		require.Equal(t, stepJobEntryBaseline, outcome.baseline)
		require.Equal(t, stepFinishUpdateJob, outcome.finish)
		require.Zero(t, outcome.schemaVer)
		require.False(t, outcome.updateRawArgs)

		job.State = model.JobStateCancelled
		outcome, ok = resolveStepOutcome(job, &jobContext{}, res)
		require.True(t, ok)
		require.Equal(t, stepFinishFinalizeJob, outcome.finish)
	})

	t.Run("explicit stepOutcome upgrades cancelled discard to finalize", func(t *testing.T) {
		job := &model.Job{Type: model.ActionMultiSchemaChange, State: model.JobStateCancelled}
		jobCtx := &jobContext{
			stepOutcome: &stepOutcome{
				writes:   stepWritesDiscard,
				baseline: stepJobEntryBaseline,
				finish:   stepFinishUpdateJob,
				err:      errors.New("boom"),
			},
		}
		outcome, ok := resolveStepOutcome(job, jobCtx, runStepResult{})
		require.True(t, ok)
		require.Equal(t, stepFinishFinalizeJob, outcome.finish)
	})

	t.Run("explicit stepOutcome preserves pausing-to-paused transition with ErrPausedDDLJob", func(t *testing.T) {
		job := &model.Job{Type: model.ActionMultiSchemaChange, State: model.JobStatePaused}
		jobCtx := &jobContext{
			stepOutcome: &stepOutcome{
				writes:   stepWritesDiscard,
				baseline: stepJobCurrent,
				finish:   stepFinishUpdateJob,
				err:      dbterror.ErrPausedDDLJob,
			},
		}
		outcome, ok := resolveStepOutcome(job, jobCtx, runStepResult{err: dbterror.ErrPausedDDLJob})
		require.True(t, ok)
		require.Equal(t, stepWritesDiscard, outcome.writes)
		require.Equal(t, stepJobCurrent, outcome.baseline)
		require.Equal(t, stepFinishUpdateJob, outcome.finish)
		require.True(t, dbterror.ErrPausedDDLJob.Equal(outcome.err))
	})

	t.Run("migrated actions default by result", func(t *testing.T) {
		migrated := []*model.Job{
			{Type: model.ActionModifyEngineAttribute},
			{
				Type: model.ActionMultiSchemaChange,
				MultiSchemaInfo: &model.MultiSchemaInfo{
					SubJobs: []*model.SubJob{{Type: model.ActionModifyEngineAttribute}},
				},
			},
		}
		for _, job := range migrated {
			job.State = model.JobStateRunning
			out, ok := resolveStepOutcome(job, &jobContext{}, runStepResult{ver: 8, updateRawArgs: true})
			require.True(t, ok)
			require.Equal(t, stepWritesKeep, out.writes)
			require.Equal(t, stepJobCurrent, out.baseline)
			require.Equal(t, stepFinishUpdateJob, out.finish)
			require.Equal(t, int64(8), out.schemaVer)
			require.True(t, out.updateRawArgs)

			out, ok = resolveStepOutcome(job, &jobContext{}, runStepResult{ver: 8, err: errors.New("fail")})
			require.True(t, ok)
			require.Equal(t, stepWritesDiscard, out.writes)
			require.Equal(t, stepJobEntryBaseline, out.baseline)
			require.Equal(t, stepFinishUpdateJob, out.finish)
			require.Zero(t, out.schemaVer)

			job.State = model.JobStateCancelled
			out, ok = resolveStepOutcome(job, &jobContext{}, runStepResult{})
			require.True(t, ok)
			require.Equal(t, stepWritesDiscard, out.writes)
			require.Equal(t, stepJobEntryBaseline, out.baseline)
			require.Equal(t, stepFinishFinalizeJob, out.finish)
		}
	})

	t.Run("unmigrated action without panic falls back to legacy", func(t *testing.T) {
		_, ok := resolveStepOutcome(&model.Job{Type: model.ActionCreateTable, State: model.JobStateRunning}, &jobContext{}, runStepResult{})
		require.False(t, ok)
	})
}

func TestApplyStepOutcomeGuards(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	t.Run("lost owner aborts before apply", func(t *testing.T) {
		ctx := context.Background()
		ownerMgr := owner.NewMockManager(ctx, "not-owner", store, ddlutil.DDLOwnerKey)
		sessCtx := mock.NewContext()
		sessCtx.Store = store
		w := newWorker(ctx, generalWorker, nil, nil, newTestDDLCtx(ctx, store, ownerMgr))
		w.sess = sess.NewSession(sessCtx)
		require.NoError(t, w.sess.Begin(ctx))
		txn, err := w.sess.Txn()
		require.NoError(t, err)
		require.NoError(t, txn.Set(kv.Key("k"), []byte("v")))

		_, err = w.applyStepOutcome(&jobContext{ctx: ctx}, model.NewJobW(&model.Job{ID: 1}, nil), nil, stepOutcome{
			finish: stepFinishUpdateJob,
		})
		require.True(t, dbterror.ErrNotOwner.Equal(err), "%v", err)
		curTxn, err := sessCtx.Txn(false)
		require.NoError(t, err)
		require.False(t, curTxn != nil && curTxn.Valid())
	})

	t.Run("canceled workCtx aborts before apply", func(t *testing.T) {
		baseCtx := context.Background()
		ownerID := "active-owner"
		ownerMgr := owner.NewMockManager(baseCtx, ownerID, store, ddlutil.DDLOwnerKey)
		owner.MockGlobalStateEntry.OwnerKey(store.UUID(), ddlutil.DDLOwnerKey).SetOwner(ownerID)
		t.Cleanup(func() {
			owner.MockGlobalStateEntry.OwnerKey(store.UUID(), ddlutil.DDLOwnerKey).UnsetOwner(ownerID)
		})

		workCtx, cancel := context.WithCancel(baseCtx)
		cancel()
		sessCtx := mock.NewContext()
		sessCtx.Store = store
		w := newWorker(workCtx, generalWorker, nil, nil, newTestDDLCtx(baseCtx, store, ownerMgr))
		w.sess = sess.NewSession(sessCtx)
		require.NoError(t, w.sess.Begin(baseCtx))

		_, err = w.applyStepOutcome(&jobContext{ctx: baseCtx}, model.NewJobW(&model.Job{ID: 2}, nil), nil, stepOutcome{
			finish: stepFinishUpdateJob,
		})
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("abandon round rolls back and returns outcome err", func(t *testing.T) {
		ctx := context.Background()
		ownerID := "abandon-owner"
		ownerMgr := owner.NewMockManager(ctx, ownerID, store, ddlutil.DDLOwnerKey)
		owner.MockGlobalStateEntry.OwnerKey(store.UUID(), ddlutil.DDLOwnerKey).SetOwner(ownerID)
		t.Cleanup(func() {
			owner.MockGlobalStateEntry.OwnerKey(store.UUID(), ddlutil.DDLOwnerKey).UnsetOwner(ownerID)
		})

		sessCtx := mock.NewContext()
		sessCtx.Store = store
		w := newWorker(ctx, generalWorker, nil, nil, newTestDDLCtx(ctx, store, ownerMgr))
		w.sess = sess.NewSession(sessCtx)
		require.NoError(t, w.sess.Begin(ctx))
		txn, err := w.sess.Txn()
		require.NoError(t, err)
		require.NoError(t, txn.Set(kv.Key("k2"), []byte("v2")))

		abandonErr := errors.New("abandon step")
		ver, err := w.applyStepOutcome(&jobContext{ctx: ctx}, model.NewJobW(&model.Job{ID: 3}, nil), nil, stepOutcome{
			finish: stepFinishAbandonRound,
			err:    abandonErr,
		})
		require.Zero(t, ver)
		require.ErrorIs(t, err, abandonErr)
		curTxn, err := sessCtx.Txn(false)
		require.NoError(t, err)
		require.False(t, curTxn != nil && curTxn.Valid())
	})
}
