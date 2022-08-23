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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
)

func (w *worker) onFlashbackCluster(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	var flashbackTS uint64
	if err := job.DecodeArgs(&flashbackTS); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	nowSchemaVersion, err := t.GetSchemaVersion()
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	flashbackSchemaVersion, err := meta.NewSnapshotMeta(d.store.GetSnapshot(kv.NewVersion(flashbackTS))).GetSchemaVersion()
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	// If flashbackSchemaVersion not same as nowSchemaVersion, we've done ddl during [flashbackTs, now).
	if flashbackSchemaVersion != nowSchemaVersion {
		job.State = model.JobStateCancelled
		return ver, errors.Errorf("schema version not same, have done ddl during [flashbackTS, now)")
	}

	sess, err := w.sessPool.get()
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	defer w.sessPool.put(sess)

	jobs, err := GetAllDDLJobs(sess, t)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	// Other non-flashback ddl jobs in queue, return error.
	if len(jobs) != 1 {
		job.State = model.JobStateCancelled
		var otherJob *model.Job
		for _, j := range jobs {
			if j.ID != job.ID {
				otherJob = j
				break
			}
		}
		return ver, errors.Errorf("have other ddl jobs(jobID: %d) in queue, can't do flashback", otherJob.ID)
	}

	job.State = model.JobStateDone
	return ver, errors.Trace(err)
}
