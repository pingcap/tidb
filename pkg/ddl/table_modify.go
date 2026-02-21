// Copyright 2015 PingCAP, Inc.
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
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/charset"
	field_types "github.com/pingcap/tidb/pkg/parser/types"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
)

func onRebaseAutoIncrementIDType(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	return onRebaseAutoID(jobCtx, job, autoid.AutoIncrementType)
}

func onRebaseAutoRandomType(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	return onRebaseAutoID(jobCtx, job, autoid.AutoRandomType)
}

func onRebaseAutoID(jobCtx *jobContext, job *model.Job, tp autoid.AllocatorType) (ver int64, _ error) {
	args, err := model.GetRebaseAutoIDArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	schemaID := job.SchemaID
	newBase, force := args.NewBase, args.Force

	if job.MultiSchemaInfo != nil && job.MultiSchemaInfo.Revertible {
		job.MarkNonRevertible()
		return ver, nil
	}

	tblInfo, err := GetTableInfoAndCancelFaultJob(jobCtx.metaMut, job, schemaID)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	tbl, err := getTable(jobCtx.getAutoIDRequirement(), schemaID, tblInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	if !force {
		newBaseTemp, err := adjustNewBaseToNextGlobalID(nil, tbl, tp, newBase)
		if err != nil {
			return ver, errors.Trace(err)
		}
		if newBase != newBaseTemp {
			job.Warning = toTError(fmt.Errorf("Can't reset AUTO_INCREMENT to %d without FORCE option, using %d instead",
				newBase, newBaseTemp,
			))
		}
		newBase = newBaseTemp
	}

	if tp == autoid.AutoIncrementType {
		tblInfo.AutoIncID = newBase
	} else {
		tblInfo.AutoRandID = newBase
	}

	if alloc := tbl.Allocators(nil).Get(tp); alloc != nil {
		// The next value to allocate is `newBase`.
		newEnd := newBase - 1
		if force {
			err = alloc.ForceRebase(newEnd)
		} else {
			err = alloc.Rebase(context.Background(), newEnd, false)
		}
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
	}
	ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func onModifyTableAutoIDCache(jobCtx *jobContext, job *model.Job) (int64, error) {
	args, err := model.GetModifyTableAutoIDCacheArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return 0, errors.Trace(err)
	}

	tblInfo, err := GetTableInfoAndCancelFaultJob(jobCtx.metaMut, job, job.SchemaID)
	if err != nil {
		return 0, errors.Trace(err)
	}

	tblInfo.AutoIDCache = args.NewCache
	ver, err := updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func (w *worker) onShardRowID(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	args, err := model.GetShardRowIDArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	shardRowIDBits := args.ShardRowIDBits
	tblInfo, err := GetTableInfoAndCancelFaultJob(jobCtx.metaMut, job, job.SchemaID)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	if shardRowIDBits < tblInfo.ShardRowIDBits {
		tblInfo.ShardRowIDBits = shardRowIDBits
	} else {
		tbl, err := getTable(jobCtx.getAutoIDRequirement(), job.SchemaID, tblInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		err = verifyNoOverflowShardBits(w.sessPool, tbl, shardRowIDBits)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, err
		}
		tblInfo.ShardRowIDBits = shardRowIDBits
		// MaxShardRowIDBits use to check the overflow of auto ID.
		tblInfo.MaxShardRowIDBits = shardRowIDBits
	}
	ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func verifyNoOverflowShardBits(s *sess.Pool, tbl table.Table, shardRowIDBits uint64) error {
	if shardRowIDBits == 0 {
		return nil
	}
	ctx, err := s.Get()
	if err != nil {
		return errors.Trace(err)
	}
	defer s.Put(ctx)
	// Check next global max auto ID first.
	autoIncID, err := tbl.Allocators(ctx.GetTableCtx()).Get(autoid.RowIDAllocType).NextGlobalAutoID()
	if err != nil {
		return errors.Trace(err)
	}
	if tables.OverflowShardBits(autoIncID, shardRowIDBits, autoid.RowIDBitLength, true) {
		return autoid.ErrAutoincReadFailed.GenWithStack("shard_row_id_bits %d will cause next global auto ID %v overflow", shardRowIDBits, autoIncID)
	}
	return nil
}


func onModifyTableComment(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	args, err := model.GetModifyTableCommentArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	tblInfo, err := GetTableInfoAndCancelFaultJob(jobCtx.metaMut, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	if job.MultiSchemaInfo != nil && job.MultiSchemaInfo.Revertible {
		job.MarkNonRevertible()
		return ver, nil
	}

	tblInfo.Comment = args.Comment
	ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func onModifyTableCharsetAndCollate(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	args, err := model.GetModifyTableCharsetAndCollateArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	toCharset, toCollate, needsOverwriteCols := args.ToCharset, args.ToCollate, args.NeedsOverwriteCols
	metaMut := jobCtx.metaMut
	dbInfo, err := checkSchemaExistAndCancelNotExistJob(metaMut, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	tblInfo, err := GetTableInfoAndCancelFaultJob(metaMut, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	// double check.
	_, err = checkAlterTableCharset(tblInfo, dbInfo, toCharset, toCollate, needsOverwriteCols)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	if job.MultiSchemaInfo != nil && job.MultiSchemaInfo.Revertible {
		job.MarkNonRevertible()
		return ver, nil
	}

	tblInfo.Charset = toCharset
	tblInfo.Collate = toCollate

	if needsOverwriteCols {
		// update column charset.
		for _, col := range tblInfo.Columns {
			if field_types.HasCharset(&col.FieldType) {
				col.SetCharset(toCharset)
				col.SetCollate(toCollate)
			} else {
				col.SetCharset(charset.CharsetBin)
				col.SetCollate(charset.CharsetBin)
			}
		}
	}

	ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

