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

package ddl_test

import (
	"context"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/copr"
	"github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/ddl/testutil"
	"github.com/pingcap/tidb/pkg/disttask/operator"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
)

func FetchChunk4Test(copCtx copr.CopContext, tbl table.PhysicalTable, startKey, endKey kv.Key, store kv.Storage,
	batchSize int) (*chunk.Chunk, error) {
	resPool := pools.NewResourcePool(func() (pools.Resource, error) {
		ctx := mock.NewContext()
		ctx.Store = store
		return ctx, nil
	}, 8, 8, 0)
	sessPool := session.NewSessionPool(resPool)
	srcChkPool := make(chan *chunk.Chunk, 10)
	for i := 0; i < 10; i++ {
		srcChkPool <- chunk.NewChunkWithCapacity(copCtx.GetBase().FieldTypes, batchSize)
	}
	opCtx := ddl.NewLocalOperatorCtx(context.Background(), 1)
	src := testutil.NewOperatorTestSource(ddl.TableScanTask{1, startKey, endKey})
	scanOp := ddl.NewTableScanOperator(opCtx, sessPool, copCtx, srcChkPool, 1, nil)
	sink := testutil.NewOperatorTestSink[ddl.IndexRecordChunk]()

	operator.Compose[ddl.TableScanTask](src, scanOp)
	operator.Compose[ddl.IndexRecordChunk](scanOp, sink)

	pipeline := operator.NewAsyncPipeline(src, scanOp, sink)
	err := pipeline.Execute()
	if err != nil {
		return nil, err
	}
	err = pipeline.Close()
	if err != nil {
		return nil, err
	}

	results := sink.Collect()
	return results[0].Chunk, nil
}

func ConvertRowToHandleAndIndexDatum(
	ctx expression.EvalContext,
	handleDataBuf, idxDataBuf []types.Datum,
	row chunk.Row, copCtx copr.CopContext, idxID int64) (kv.Handle, []types.Datum, error) {
	c := copCtx.GetBase()
	idxData := ddl.ExtractDatumByOffsets(ctx, row, copCtx.IndexColumnOutputOffsets(idxID), c.ExprColumnInfos, idxDataBuf)
	handleData := ddl.ExtractDatumByOffsets(ctx, row, c.HandleOutputOffsets, c.ExprColumnInfos, handleDataBuf)
	handle, err := ddl.BuildHandle(handleData, c.TableInfo, c.PrimaryKeyInfo, time.Local, errctx.StrictNoWarningContext)
	return handle, idxData, err
}
