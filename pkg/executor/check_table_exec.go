// Copyright 2024 PingCAP, Inc.
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

package executor

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/admin"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/logutil"
	atomicutil "go.uber.org/atomic"
	"go.uber.org/zap"
)
var errCheckPartialIndexWithoutFastCheck = dbterror.ClassExecutor.NewStd(errno.ErrCheckPartialIndexWithoutFastCheck)

// CheckTableExec represents a check table executor.
// It is built from the "admin check table" statement, and it checks if the
// index matches the records in the table.
type CheckTableExec struct {
	exec.BaseExecutor

	dbName     string
	table      table.Table
	indexInfos []*model.IndexInfo
	srcs       []*IndexLookUpExecutor
	done       bool
	exitCh     chan struct{}
	retCh      chan error
	checkIndex bool
}

var _ exec.Executor = &CheckTableExec{}

// Open implements the Executor Open interface.
func (e *CheckTableExec) Open(ctx context.Context) error {
	if err := e.BaseExecutor.Open(ctx); err != nil {
		return err
	}
	for _, src := range e.srcs {
		if err := exec.Open(ctx, src); err != nil {
			return errors.Trace(err)
		}
	}
	e.done = false
	return nil
}

// Close implements the Executor Close interface.
func (e *CheckTableExec) Close() error {
	var firstErr error
	close(e.exitCh)
	for _, src := range e.srcs {
		if err := exec.Close(src); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (e *CheckTableExec) checkTableIndexHandle(ctx context.Context, idxInfo *model.IndexInfo) error {
	// For partition table, there will be multi same index indexLookUpReaders on different partitions.
	for _, src := range e.srcs {
		if src.index.Name.L == idxInfo.Name.L {
			err := e.checkIndexHandle(ctx, src)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *CheckTableExec) checkIndexHandle(ctx context.Context, src *IndexLookUpExecutor) error {
	cols := src.Schema().Columns
	retFieldTypes := make([]*types.FieldType, len(cols))
	for i := range cols {
		retFieldTypes[i] = cols[i].RetType
	}
	chk := chunk.New(retFieldTypes, e.InitCap(), e.MaxChunkSize())

	var err error
	for {
		err = exec.Next(ctx, src, chk)
		if err != nil {
			e.retCh <- errors.Trace(err)
			break
		}
		if chk.NumRows() == 0 {
			break
		}
	}
	return errors.Trace(err)
}

func (e *CheckTableExec) handlePanic(r any) {
	if r != nil {
		e.retCh <- errors.Errorf("%v", r)
	}
}

// Next implements the Executor Next interface.
func (e *CheckTableExec) Next(ctx context.Context, _ *chunk.Chunk) error {
	if e.done || len(e.srcs) == 0 {
		return nil
	}
	defer func() { e.done = true }()

	idxNames := make([]string, 0, len(e.indexInfos))
	for _, idx := range e.indexInfos {
		if idx.MVIndex || idx.IsColumnarIndex() {
			continue
		}
		if idx.HasCondition() {
			return errors.Trace(errCheckPartialIndexWithoutFastCheck)
		}
		idxNames = append(idxNames, idx.Name.O)
	}
	greater, idxOffset, err := admin.CheckIndicesCount(e.Ctx(), e.dbName, e.table.Meta().Name.O, idxNames)
	if err != nil {
		// For admin check index statement, for speed up and compatibility, doesn't do below checks.
		if e.checkIndex {
			return errors.Trace(err)
		}
		if greater == admin.IdxCntGreater {
			err = e.checkTableIndexHandle(ctx, e.indexInfos[idxOffset])
		} else if greater == admin.TblCntGreater {
			err = e.checkTableRecord(ctx, idxOffset)
		}
		return errors.Trace(err)
	}

	// The number of table rows is equal to the number of index rows.
	// TODO: Make the value of concurrency adjustable. And we can consider the number of records.
	if len(e.srcs) == 1 {
		err = e.checkIndexHandle(ctx, e.srcs[0])
		if err == nil && e.srcs[0].index.MVIndex {
			err = e.checkTableRecord(ctx, 0)
		}
		if err != nil {
			return err
		}
	}
	taskCh := make(chan *IndexLookUpExecutor, len(e.srcs))
	failure := atomicutil.NewBool(false)
	concurrency := min(3, len(e.srcs))
	var wg util.WaitGroupWrapper
	for _, src := range e.srcs {
		taskCh <- src
	}
	for range concurrency {
		wg.RunWithRecover(func() {
			for {
				if fail := failure.Load(); fail {
					return
				}
				select {
				case src := <-taskCh:
					err1 := e.checkIndexHandle(ctx, src)
					if err1 == nil && src.index.MVIndex {
						for offset, idx := range e.indexInfos {
							if idx.ID == src.index.ID {
								err1 = e.checkTableRecord(ctx, offset)
								break
							}
						}
					}
					if err1 != nil {
						failure.Store(true)
						logutil.Logger(ctx).Info("check index handle failed", zap.Error(err1))
						return
					}
				case <-e.exitCh:
					return
				default:
					return
				}
			}
		}, e.handlePanic)
	}

	wg.Wait()
	select {
	case err := <-e.retCh:
		return errors.Trace(err)
	default:
		return nil
	}
}

func (e *CheckTableExec) checkTableRecord(ctx context.Context, idxOffset int) error {
	idxInfo := e.indexInfos[idxOffset]
	txn, err := e.Ctx().Txn(true)
	if err != nil {
		return err
	}
	if e.table.Meta().GetPartitionInfo() == nil {
		idx, err := tables.NewIndex(e.table.Meta().ID, e.table.Meta(), idxInfo)
		if err != nil {
			return err
		}
		return admin.CheckRecordAndIndex(ctx, e.Ctx(), txn, e.table, idx)
	}

	info := e.table.Meta().GetPartitionInfo()
	for _, def := range info.Definitions {
		pid := def.ID
		partition := e.table.(table.PartitionedTable).GetPartition(pid)
		idx, err := tables.NewIndex(def.ID, e.table.Meta(), idxInfo)
		if err != nil {
			return err
		}
		if err := admin.CheckRecordAndIndex(ctx, e.Ctx(), txn, partition, idx); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}
