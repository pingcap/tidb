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

package ddl

import (
	"sync"

	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
)

func onMultiSchemaChange(w *worker, d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	if job.MultiSchemaInfo.Revertible {
		// Handle the rolling back job.
		if job.IsRollingback() || job.IsCancelling() {
			// Rollback/cancel the sub-jobs in reverse order.
			for i := len(job.MultiSchemaInfo.SubJobs) - 1; i >= 0; i-- {
				sub := job.MultiSchemaInfo.SubJobs[i]
				if isFinished(sub) {
					continue
				}
				proxyJob := cloneFromSubJob(job, sub)
				ver, err = w.runDDLJob(d, t, proxyJob)
				mergeBackToSubJob(proxyJob, sub)
				if i == 0 {
					// The last rollback/cancelling sub-job is done.
					if job.IsRollingback() {
						job.State = model.JobStateRollbackDone
					}
					if job.IsCancelling() {
						job.State = model.JobStateCancelled
					}
				}
				return ver, err
			}
		}

		// The sub-jobs are normally running.
		// Run the first executable sub-job.
		for i, sub := range job.MultiSchemaInfo.SubJobs {
			if !sub.Revertible {
				// Skip the sub jobs which related schema states
				// are in the last revertible point.
				continue
			}
			proxyJob := cloneFromSubJob(job, sub)
			ver, err = w.runDDLJob(d, t, proxyJob)
			mergeBackToSubJob(proxyJob, sub)
			handleRevertibleException(job, sub.State, i)
			return ver, err
		}
		// All the sub-jobs are non-revertible.
		job.MultiSchemaInfo.Revertible = false
		// Step the sub-jobs to the non-revertible states all at once.
		for _, sub := range job.MultiSchemaInfo.SubJobs {
			proxyJob := cloneFromSubJob(job, sub)
			ver, err = w.runDDLJob(d, t, proxyJob)
			mergeBackToSubJob(proxyJob, sub)
		}
		return ver, err
	}
	// Run the rest non-revertible sub-jobs one by one.
	for _, sub := range job.MultiSchemaInfo.SubJobs {
		if isFinished(sub) {
			continue
		}
		proxyJob := cloneFromSubJob(job, sub)
		ver, err = w.runDDLJob(d, t, proxyJob)
		mergeBackToSubJob(proxyJob, sub)
		return ver, err
	}
	job.State = model.JobStateDone
	return ver, err
}

func isFinished(job *model.SubJob) bool {
	return job.State == model.JobStateDone ||
		job.State == model.JobStateRollbackDone ||
		job.State == model.JobStateCancelled
}

func cloneFromSubJob(job *model.Job, sub *model.SubJob) *model.Job {
	return &model.Job{
		ID:              0,
		Type:            sub.Type,
		SchemaID:        job.SchemaID,
		TableID:         job.TableID,
		SchemaName:      job.SchemaName,
		State:           job.State,
		Error:           nil,
		ErrorCount:      0,
		RowCount:        0,
		Mu:              sync.Mutex{},
		CtxVars:         nil,
		Args:            sub.Args,
		RawArgs:         sub.RawArgs,
		SchemaState:     sub.SchemaState,
		SnapshotVer:     sub.SnapshotVer,
		RealStartTS:     job.RealStartTS,
		StartTS:         job.StartTS,
		DependencyID:    job.DependencyID,
		Query:           job.Query,
		BinlogInfo:      job.BinlogInfo,
		Version:         job.Version,
		ReorgMeta:       job.ReorgMeta,
		MultiSchemaInfo: &model.MultiSchemaInfo{Revertible: sub.Revertible},
		Priority:        job.Priority,
		SeqNum:          job.SeqNum,
	}
}

func mergeBackToSubJob(job *model.Job, sub *model.SubJob) {
	sub.Revertible = job.MultiSchemaInfo.Revertible
	sub.SchemaState = job.SchemaState
	sub.SnapshotVer = job.SnapshotVer
	sub.Args = job.Args
	sub.State = job.State
}

func handleRevertibleException(job *model.Job, res model.JobState, idx int) {
	if res == model.JobStateRollingback || res == model.JobStateCancelling {
		job.State = res
	}
	// Flush the rollback state and cancelled state to sub-jobs.
	for i, sub := range job.MultiSchemaInfo.SubJobs {
		if i < idx {
			sub.State = model.JobStateRollingback
		}
		if i > idx {
			sub.State = model.JobStateCancelled
		}
	}
}
