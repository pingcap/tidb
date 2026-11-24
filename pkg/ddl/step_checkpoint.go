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
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/tikv/client-go/v2/tikv"
)

// stepWriteDisposition decides whether KV / MemDB / TxnCtx mutations produced
// during runOneJobStep are kept or discarded.
type stepWriteDisposition uint8

const (
	stepWritesKeep stepWriteDisposition = iota
	stepWritesDiscard
)

// stepJobBaseline decides which job metadata state is persisted on exit.
type stepJobBaseline uint8

const (
	stepJobCurrent stepJobBaseline = iota
	stepJobEntryBaseline
)

// stepFinishAction decides how transitOneJobStep concludes after applying
// write disposition and job baseline restoration.
type stepFinishAction uint8

const (
	stepFinishUpdateJob stepFinishAction = iota
	stepFinishFinalizeJob
	stepFinishAbandonRound
)

// stepOutcome explicitly separates the three orthogonal step exit decisions:
// whether step KV writes survive, which job baseline/delta is persisted, and
// whether the round updates the active job, archives it, or abandons the step.
type stepOutcome struct {
	writes        stepWriteDisposition
	baseline      stepJobBaseline
	finish        stepFinishAction
	schemaVer     int64
	updateRawArgs bool
	err           error
	applyJobDelta func(restored, postStep *model.Job)
}

type runStepResult struct {
	ver           int64
	updateRawArgs bool
	panicked      bool
	err           error
}

// stepCheckpoint captures the outer worker transaction identity, MemDB
// checkpoint, TransactionContext savepoint, and immutable entry job bytes at
// the start of a DDL job step.
type stepCheckpoint struct {
	se              *sess.Session
	origTxnCtx      *variable.TransactionContext
	origTxnStartTS  uint64
	memDBCheckpoint *tikv.MemDBCheckpoint
	txnCtxSavepoint variable.TxnCtxNeedToRestore
	entryJobBytes   []byte
	hasDurableBytes bool
}

func captureStepCheckpoint(se *sess.Session, txn kv.Transaction, jobW *model.JobW) (*stepCheckpoint, error) {
	entryBytes := jobW.Bytes
	hasDurableBytes := len(entryBytes) > 0
	if !hasDurableBytes && jobW.Job != nil {
		var err error
		entryBytes, err = jobW.Job.Encode(false)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	origTxnCtx := se.GetSessionVars().TxnCtx
	var memCP *tikv.MemDBCheckpoint
	var startTS uint64
	if txn != nil && txn.Valid() {
		startTS = txn.StartTS()
		if !txn.IsPipelined() {
			memCP = txn.GetMemDBCheckpoint()
		}
	}
	return &stepCheckpoint{
		se:              se,
		origTxnCtx:      origTxnCtx,
		origTxnStartTS:  startTS,
		memDBCheckpoint: memCP,
		txnCtxSavepoint: origTxnCtx.GetCurrentSavepoint(),
		entryJobBytes:   entryBytes,
		hasDurableBytes: hasDurableBytes,
	}, nil
}

func (cp *stepCheckpoint) canRollbackInSameTxn() (kv.Transaction, bool) {
	if cp == nil || cp.memDBCheckpoint == nil || cp.origTxnCtx == nil || cp.origTxnStartTS == 0 {
		return nil, false
	}
	curTxn, err := cp.se.Session().Txn(false)
	if err != nil || curTxn == nil || !curTxn.Valid() || curTxn.IsPipelined() {
		return nil, false
	}
	curTxnCtx := cp.se.GetSessionVars().TxnCtx
	if curTxnCtx != cp.origTxnCtx || curTxnCtx.IsPessimistic {
		return nil, false
	}
	if curTxn.StartTS() != cp.origTxnStartTS {
		return nil, false
	}
	return curTxn, true
}

// discardWrites reverts uncommitted and statement-committed step mutations.
// When the original optimistic transaction is still active and matches its
// captured identity, MemDB and TxnCtx are rolled back in place on the same TSO.
// Otherwise, the framework rolls back, reopens a transaction, and re-verifies
// jobW.Bytes against persistent storage so an already-committed or concurrently
// modified step is never overwritten.
func (cp *stepCheckpoint) discardWrites(w *worker, jobCtx *jobContext, jobW *model.JobW) error {
	if curTxn, ok := cp.canRollbackInSameTxn(); ok {
		curTxn.RollbackMemDBToCheckpoint(cp.memDBCheckpoint)
		cp.origTxnCtx.RestoreBySavepoint(cp.txnCtxSavepoint)
		jobCtx.metaMut = meta.NewMutator(curTxn)
		return nil
	}

	cp.se.Rollback()
	newTxn, err := w.prepareTxn(jobW.Job)
	if err != nil {
		return errors.Trace(err)
	}
	if cp.hasDurableBytes {
		if err = w.checkJobMetaUnchanged(jobCtx, jobW); err != nil {
			cp.se.Rollback()
			return errors.Trace(err)
		}
	}
	jobCtx.metaMut = meta.NewMutator(newTxn)
	return nil
}

func (cp *stepCheckpoint) restoreJob(
	jobCtx *jobContext,
	job *model.Job,
	applyDelta func(restored, postStep *model.Job),
) error {
	if cp == nil || len(cp.entryJobBytes) == 0 {
		return nil
	}
	var restored model.Job
	if err := restored.Decode(cp.entryJobBytes); err != nil {
		return errors.Trace(err)
	}
	applyAbortedStepDiagnostics(&restored, job)
	if applyDelta != nil {
		applyDelta(&restored, job)
	}
	job.RestoreFrom(&restored)
	jobCtx.jobArgs = nil
	return nil
}

// applyAbortedStepDiagnostics transfers post-runOneJobStep error accounting and
// durable runtime choices from postStep onto the decoded entry baseline.
func applyAbortedStepDiagnostics(restored, postStep *model.Job) {
	restored.Error = postStep.Error
	restored.ErrorCount = postStep.ErrorCount
	restored.RealStartTS = postStep.RealStartTS
	restored.LastSchemaVersion = 0
	if restored.ReorgMeta != nil && postStep.ReorgMeta != nil && postStep.ReorgMeta.UseCloudStorage {
		restored.ReorgMeta.UseCloudStorage = true
	}
	if postStep.IsCancelling() || postStep.IsCancelled() {
		restored.State = postStep.State
		restored.AdminOperator = postStep.AdminOperator
	}
	if restored.IsCancelled() && restored.Error == nil {
		restored.Error = toTError(dbterror.ErrCancelledDDLJob)
	}
}

func multiSchemaHasAction(job *model.Job, target model.ActionType) bool {
	if job == nil || job.MultiSchemaInfo == nil {
		return false
	}
	for _, sub := range job.MultiSchemaInfo.SubJobs {
		if sub.Type == target {
			return true
		}
	}
	return false
}

func usesStepCheckpointContract(job *model.Job) bool {
	if job == nil {
		return false
	}
	if job.Type == model.ActionModifyEngineAttribute {
		return true
	}
	return job.Type == model.ActionMultiSchemaChange &&
		multiSchemaHasAction(job, model.ActionModifyEngineAttribute)
}

func resolveStepOutcome(job *model.Job, jobCtx *jobContext, res runStepResult) (stepOutcome, bool) {
	if res.panicked {
		finish := stepFinishUpdateJob
		if job.IsCancelled() {
			finish = stepFinishFinalizeJob
		}
		return stepOutcome{
			writes:        stepWritesDiscard,
			baseline:      stepJobEntryBaseline,
			finish:        finish,
			schemaVer:     0,
			updateRawArgs: false,
			err:           res.err,
		}, true
	}

	if jobCtx.stepOutcome != nil {
		outcome := *jobCtx.stepOutcome
		if job.IsCancelled() && outcome.finish == stepFinishUpdateJob && outcome.writes == stepWritesDiscard {
			outcome.finish = stepFinishFinalizeJob
		}
		return outcome, true
	}

	if !usesStepCheckpointContract(job) {
		return stepOutcome{}, false
	}

	if job.IsCancelled() {
		return stepOutcome{
			writes:        stepWritesDiscard,
			baseline:      stepJobEntryBaseline,
			finish:        stepFinishFinalizeJob,
			schemaVer:     0,
			updateRawArgs: false,
			err:           nil,
		}, true
	}

	if res.err != nil {
		return stepOutcome{
			writes:        stepWritesDiscard,
			baseline:      stepJobEntryBaseline,
			finish:        stepFinishUpdateJob,
			schemaVer:     0,
			updateRawArgs: false,
			err:           res.err,
		}, true
	}

	return stepOutcome{
		writes:        stepWritesKeep,
		baseline:      stepJobCurrent,
		finish:        stepFinishUpdateJob,
		schemaVer:     res.ver,
		updateRawArgs: res.updateRawArgs,
		err:           nil,
	}, true
}

func (w *worker) applyStepOutcome(
	jobCtx *jobContext,
	jobW *model.JobW,
	cp *stepCheckpoint,
	outcome stepOutcome,
) (int64, error) {
	job := jobW.Job
	if err := w.checkBeforeCommit(); err != nil {
		return 0, err
	}

	if outcome.writes == stepWritesDiscard {
		if err := cp.discardWrites(w, jobCtx, jobW); err != nil {
			return 0, err
		}
		failpoint.InjectCall("afterRollbackStepForStepCheckpoint", job)
	}

	if outcome.baseline == stepJobEntryBaseline {
		if err := cp.restoreJob(jobCtx, job, outcome.applyJobDelta); err != nil {
			w.sess.Rollback()
			return 0, err
		}
	}

	switch outcome.finish {
	case stepFinishAbandonRound:
		w.sess.Rollback()
		return 0, outcome.err
	case stepFinishFinalizeJob:
		if job.IsCancelled() {
			deleteAffinityGroupsOfCancelledJob(jobCtx, job)
		}
		return 0, w.handleJobDone(jobCtx, job)
	case stepFinishUpdateJob:
		return w.commitActiveJobStep(jobCtx, jobW, outcome.schemaVer, outcome.updateRawArgs, outcome.err)
	default:
		w.sess.Rollback()
		return 0, dbterror.ErrInvalidDDLJob.GenWithStack("invalid step finish action: %d", outcome.finish)
	}
}
