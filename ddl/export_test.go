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

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
)

func SetBatchInsertDeleteRangeSize(i int) {
	batchInsertDeleteRangeSize = i
}

var NewCopContext4Test = newCopContext

func FetchRowsFromCop4Test(copCtx *copContext, tbl table.PhysicalTable, startKey, endKey kv.Key, store kv.Storage,
	batchSize int) ([]*indexRecord, bool, error) {
	variable.SetDDLReorgBatchSize(int32(batchSize))
	task := &reorgBackfillTask{
		id:            1,
		startKey:      startKey,
		endKey:        endKey,
		physicalTable: tbl,
	}
	pool := newCopReqSenderPool(context.Background(), copCtx, store)
	pool.adjustSize(1)
	pool.tasksCh <- task
	idxRec, _, _, done, err := pool.fetchRowColValsFromCop(*task)
	pool.close()
	return idxRec, done, err
}

type IndexRecord4Test = *indexRecord

func (i IndexRecord4Test) GetHandle() kv.Handle {
	return i.handle
}

func (i IndexRecord4Test) GetIndexValues() []types.Datum {
	return i.vals
}
