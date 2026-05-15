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
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

func pkdbOnAddColumnEnterWriteReorg(job *model.Job, columnInfo *model.ColumnInfo) {
	if mysql.HasAutoIncrementFlag(columnInfo.GetFlag()) {
		// AUTO_INCREMENT is not compatible with instant add column. We need to backfill values
		// for existing rows in reorganization state.
		job.SnapshotVer = 0
		if job.ReorgMeta != nil && job.ReorgMeta.Stage == model.ReorgStageNone {
			job.ReorgMeta.Stage = model.ReorgStageAddAutoIncrementColumnBackfill
		}
		return
	}

	// Instant add column: mark it non-revertible in multi-schema change so that we can make all
	// sub-jobs public using a single schema version.
	job.MarkNonRevertible()
}

func pkdbMaybeHandleAddAutoIncrementColumnWriteReorg(
	jobCtx *jobContext,
	w *worker,
	job *model.Job,
	tblInfo *model.TableInfo,
	columnInfo *model.ColumnInfo,
) (shouldReturn bool, ver int64, err error) {
	if !mysql.HasAutoIncrementFlag(columnInfo.GetFlag()) {
		return false, 0, nil
	}

	if job.ReorgMeta == nil {
		job.ReorgMeta = &model.DDLReorgMeta{}
	}
	if job.ReorgMeta.Stage == model.ReorgStageNone {
		job.ReorgMeta.Stage = model.ReorgStageAddAutoIncrementColumnBackfill
	}

	switch job.ReorgMeta.Stage {
	case model.ReorgStageAddAutoIncrementColumnBackfill:
		tbl, err1 := getTable(jobCtx.getAutoIDRequirement(), job.SchemaID, tblInfo)
		if err1 != nil {
			return true, ver, errors.Trace(err1)
		}
		done, ver1, err1 := doReorgWorkForAddAutoIncrementColumn(jobCtx, w, job, tbl, columnInfo)
		if !done {
			return true, ver1, err1
		}
		ver = ver1
		job.ReorgMeta.Stage = model.ReorgStageAddAutoIncrementColumnCompleted
	case model.ReorgStageAddAutoIncrementColumnCompleted:
		// ready to publish
	}

	// In multi-schema change revertible phase, stop here so other sub-jobs can catch up, and
	// then publish all schema changes using a single schema version.
	if job.MultiSchemaInfo != nil && job.MultiSchemaInfo.Revertible {
		// Keep a stable hook for tests to run DML while the column is still in WriteReorg state.
		// In multi-schema change we may return early here, so the later InjectCall might not be reached.
		failpoint.InjectCall("onAddColumnStateWriteReorg")
		checkAndMarkNonRevertible(job)
		return true, ver, nil
	}
	return false, ver, nil
}
