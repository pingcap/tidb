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
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
)

func (w *worker) onFlashbackCluster(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	var flashbackTS uint64
	if err := job.DecodeArgs(&flashbackTS); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	txn, err := d.store.Begin()
	if err != nil {
		return ver, errors.Trace(err)
	}

	fn := func(jobs []*model.Job) (bool, error) {
		for _, j := range jobs {
			if j.ID == job.ID {
				continue
			}
			if !j.IsFinished() {
				return true, errors.Errorf("Can't flashback cluster, other ddl jobs in ddl queue")
			}
			if j.Type != model.ActionFlashbackCluster && j.BinlogInfo != nil && j.BinlogInfo.FinishedTS >= flashbackTS {
				return true, errors.Errorf("Can't flashback cluster because of ddl history")
			}
			if j.BinlogInfo != nil && j.BinlogInfo.FinishedTS < flashbackTS {
				return true, nil
			}
		}
		return false, nil
	}

	ctx, err := w.sessPool.get()
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	defer w.sessPool.put(ctx)

	if err = IterAllDDLJobs(ctx, txn, fn); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	job.State = model.JobStateDone

	return ver, errors.Trace(err)
}
