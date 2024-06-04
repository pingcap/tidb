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
	"context"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/ddl/copr"
	"github.com/pingcap/tidb/pkg/ddl/internal/session"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
)

type resultChanForTest struct {
	ch chan IndexRecordChunk
}

func (r *resultChanForTest) AddTask(rs IndexRecordChunk) {
	r.ch <- rs
}

func FetchChunk4Test(copCtx copr.CopContext, tbl table.PhysicalTable, startKey, endKey kv.Key, store kv.Storage,
	batchSize int) *chunk.Chunk {
	variable.SetDDLReorgBatchSize(int32(batchSize))
	task := &reorgBackfillTask{
		id:            1,
		startKey:      startKey,
		endKey:        endKey,
		physicalTable: tbl,
	}
	taskCh := make(chan *reorgBackfillTask, 5)
	resultCh := make(chan IndexRecordChunk, 5)
	resPool := pools.NewResourcePool(func() (pools.Resource, error) {
		ctx := mock.NewContext()
		ctx.Store = store
		return ctx, nil
	}, 8, 8, 0)
	sessPool := session.NewSessionPool(resPool, store)
	pool := newCopReqSenderPool(context.Background(), copCtx, store, taskCh, sessPool, nil)
	pool.chunkSender = &resultChanForTest{ch: resultCh}
	pool.adjustSize(1)
	pool.tasksCh <- task
	rs := <-resultCh
	close(taskCh)
	pool.close(false)
	sessPool.Close()
	return rs.Chunk
}

func ConvertRowToHandleAndIndexDatum(
	ctx expression.EvalContext,
	handleDataBuf, idxDataBuf []types.Datum,
	row chunk.Row, copCtx copr.CopContext, idxID int64) (kv.Handle, []types.Datum, error) {
	c := copCtx.GetBase()
	idxData := extractDatumByOffsets(ctx, row, copCtx.IndexColumnOutputOffsets(idxID), c.ExprColumnInfos, idxDataBuf)
	handleData := extractDatumByOffsets(ctx, row, c.HandleOutputOffsets, c.ExprColumnInfos, handleDataBuf)
	handle, err := buildHandle(handleData, c.TableInfo, c.PrimaryKeyInfo, time.Local, errctx.StrictNoWarningContext)
	return handle, idxData, err
}

// ExtractDatumByOffsetsForTest is used for test.
var ExtractDatumByOffsetsForTest = extractDatumByOffsets

// CalculateRegionBatchForTest is used for test.
var CalculateRegionBatchForTest = calculateRegionBatch
