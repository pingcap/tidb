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

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

func SetBatchInsertDeleteRangeSize(i int) {
	batchInsertDeleteRangeSize = i
}

var NewCopContext4Test = newCopContext

type resultChanForTest struct {
	ch chan idxRecResult
}

func (r *resultChanForTest) AddTask(rs idxRecResult) {
	r.ch <- rs
}

func FetchChunk4Test(copCtx *copContext, tbl table.PhysicalTable, startKey, endKey kv.Key, store kv.Storage,
	batchSize int) *chunk.Chunk {
	variable.SetDDLReorgBatchSize(int32(batchSize))
	task := &reorgBackfillTask{
		id:            1,
		startKey:      startKey,
		endKey:        endKey,
		physicalTable: tbl,
	}
	taskCh := make(chan *reorgBackfillTask, 5)
	resultCh := make(chan idxRecResult, 5)
	pool := newCopReqSenderPool(context.Background(), copCtx, store, taskCh, nil)
	pool.chunkSender = &resultChanForTest{ch: resultCh}
	pool.adjustSize(1)
	pool.tasksCh <- task
	rs := <-resultCh
	close(taskCh)
	pool.close(false)
	return rs.chunk
}

func ConvertRowToHandleAndIndexDatum(row chunk.Row, copCtx *copContext) (kv.Handle, []types.Datum, error) {
	idxData := extractDatumByOffsets(row, copCtx.idxColOutputOffsets, copCtx.expColInfos, nil)
	handleData := extractDatumByOffsets(row, copCtx.handleOutputOffsets, copCtx.expColInfos, nil)
	handle, err := buildHandle(handleData, copCtx.tblInfo, copCtx.pkInfo, &stmtctx.StatementContext{TimeZone: time.Local})
	return handle, idxData, err
}
