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

// stepFinishAction decides how transitOneJobStep concludes after applying
// write discard and job baseline restoration.
type stepFinishAction uint8

const (
	stepFinishUpdateJob stepFinishAction = iota
	stepFinishFinalizeJob
	stepFinishAbandonRound
)

// stepOutcome describes how transitOneJobStep concludes a step:
// whether to discard KV/MemDB/TxnCtx writes, whether to restore the entry job
// baseline (plus an optional delta), and how to persist or finalize the job.
type stepOutcome struct {
	discardWrites   bool
	restoreEntryJob bool
	finish          stepFinishAction
	schemaVer       int64
	updateRawArgs   bool
	err             error
	applyJobDelta   func(restored, postStep *model.Job)
}

func keepStepOutcome(schemaVer int64, updateRawArgs bool, err error) *stepOutcome {
	return &stepOutcome{
		finish:        stepFinishUpdateJob,
		schemaVer:     schemaVer,
		updateRawArgs: updateRawArgs,
		err:           err,
	}
}

func discardStepOutcome(err error, applyDelta func(restored, postStep *model.Job)) *stepOutcome {
	return &stepOutcome{
		discardWrites:   true,
		restoreEntryJob: true,
		finish:          stepFinishUpdateJob,
		err:             err,
		applyJobDelta:   applyDelta,
	}
}

type runStepResult struct {
	ver           int64
	updateRawArgs bool
	panicked      bool
	err           error
}

// stepSavepoint captures the outer worker transaction identity, MemDB
// rollback handle, TransactionContext savepoint, and immutable entry job bytes
// at the start of a DDL job step.
type stepSavepoint struct {
	se              *sess.Session
	origTxnCtx      *variable.TransactionContext
	origTxnStartTS  uint64
	memDBCheckpoint *tikv.MemDBCheckpoint
	txnCtxSavepoint variable.TxnCtxNeedToRestore
	entryJobBytes   []byte
	hasDurableBytes bool
}

func captureStepSavepoint(se *sess.Session, txn kv.Transaction, jobW *model.JobW) (*stepSavepoint, error) {
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
		// MemDB.Update can overwrite pre-savepoint same-length keys in place;
		// only use in-place MemDB rollback when the transaction buffer starts empty.
		if !txn.IsPipelined() && txn.Len() == 0 {
			memCP = txn.GetMemDBCheckpoint()
		}
	}
	return &stepSavepoint{
		se:              se,
		origTxnCtx:      origTxnCtx,
		origTxnStartTS:  startTS,
		memDBCheckpoint: memCP,
		txnCtxSavepoint: origTxnCtx.GetCurrentSavepoint(),
		entryJobBytes:   entryBytes,
		hasDurableBytes: hasDurableBytes,
	}, nil
}

func (sp *stepSavepoint) canRollbackInSameTxn() (kv.Transaction, bool) {
	if sp == nil || sp.memDBCheckpoint == nil || sp.origTxnCtx == nil || sp.origTxnStartTS == 0 {
		return nil, false
	}
	curTxn, err := sp.se.Session().Txn(false)
	if err != nil || curTxn == nil || !curTxn.Valid() || curTxn.IsPipelined() {
		return nil, false
	}
	curTxnCtx := sp.se.GetSessionVars().TxnCtx
	if curTxnCtx != sp.origTxnCtx || curTxnCtx.IsPessimistic {
		return nil, false
	}
	if curTxn.StartTS() != sp.origTxnStartTS {
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
func (sp *stepSavepoint) discardWrites(w *worker, jobCtx *jobContext, jobW *model.JobW) error {
	if curTxn, ok := sp.canRollbackInSameTxn(); ok {
		curTxn.RollbackMemDBToCheckpoint(sp.memDBCheckpoint)
		sp.origTxnCtx.RestoreBySavepoint(sp.txnCtxSavepoint)
		jobCtx.metaMut = meta.NewMutator(curTxn)
		return nil
	}

	sp.se.Rollback()
	newTxn, err := w.prepareTxn(jobW.Job)
	if err != nil {
		return errors.Trace(err)
	}
	if sp.hasDurableBytes {
		if err = w.checkJobMetaUnchanged(jobCtx, jobW); err != nil {
			sp.se.Rollback()
			return errors.Trace(err)
		}
	}
	jobCtx.metaMut = meta.NewMutator(newTxn)
	return nil
}

func (sp *stepSavepoint) restoreJob(
	jobCtx *jobContext,
	postStep *model.Job,
	applyDelta func(restored, postStep *model.Job),
) (*model.Job, error) {
	if sp == nil || len(sp.entryJobBytes) == 0 {
		return postStep, nil
	}
	var restored model.Job
	if err := restored.Decode(sp.entryJobBytes); err != nil {
		return nil, errors.Trace(err)
	}
	applyAbortedStepDiagnostics(&restored, postStep)
	if applyDelta != nil {
		applyDelta(&restored, postStep)
	}
	jobCtx.jobArgs = nil
	return &restored, nil
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

func usesStepOutcomeContract(job *model.Job) bool {
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
		outcome := *discardStepOutcome(res.err, nil)
		if job.IsCancelled() {
			outcome.finish = stepFinishFinalizeJob
		}
		return outcome, true
	}

	if jobCtx.stepOutcome != nil {
		outcome := *jobCtx.stepOutcome
		if job.IsCancelled() && outcome.finish == stepFinishUpdateJob && outcome.discardWrites {
			outcome.finish = stepFinishFinalizeJob
		}
		return outcome, true
	}

	if !usesStepOutcomeContract(job) {
		return stepOutcome{}, false
	}

	if job.IsCancelled() {
		outcome := *discardStepOutcome(nil, nil)
		outcome.finish = stepFinishFinalizeJob
		return outcome, true
	}
	if res.err != nil {
		return *discardStepOutcome(res.err, nil), true
	}
	return *keepStepOutcome(res.ver, res.updateRawArgs, nil), true
}

func (w *worker) applyStepOutcome(
	jobCtx *jobContext,
	jobW *model.JobW,
	sp *stepSavepoint,
	outcome stepOutcome,
) (int64, error) {
	job := jobW.Job
	if err := w.checkBeforeCommit(); err != nil {
		return 0, err
	}

	if outcome.discardWrites {
		if err := sp.discardWrites(w, jobCtx, jobW); err != nil {
			return 0, err
		}
		failpoint.InjectCall("afterRollbackStepForStepSavepoint", job)
	}

	updateRawArgs := outcome.updateRawArgs
	if outcome.restoreEntryJob {
		restoredJob, err := sp.restoreJob(jobCtx, job, outcome.applyJobDelta)
		if err != nil {
			w.sess.Rollback()
			return 0, err
		}
		jobW.Job = restoredJob
		job = restoredJob
		// Restored baseline jobs only carry RawArgs (`job.args == nil`), so
		// updating or recovering must not serialize nil args over RawArgs.
		updateRawArgs = false
	}

	switch outcome.finish {
	case stepFinishAbandonRound:
		w.sess.Rollback()
		return 0, outcome.err
	case stepFinishFinalizeJob:
		if job.IsCancelled() {
			deleteAffinityGroupsOfCancelledJob(jobCtx, job)
		}
		return 0, w.handleJobDone(jobCtx, job, !outcome.restoreEntryJob)
	case stepFinishUpdateJob:
		return w.commitActiveJobStep(jobCtx, jobW, outcome.schemaVer, updateRawArgs, outcome.err)
	default:
		w.sess.Rollback()
		return 0, dbterror.ErrInvalidDDLJob.GenWithStack("invalid step finish action: %d", outcome.finish)
	}
}
