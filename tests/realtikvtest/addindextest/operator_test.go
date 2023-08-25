// Copyright 2023 PingCAP, Inc.
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

package addindextest

import (
	"context"
	"fmt"
	"testing"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/ddl/ingest"
	"github.com/pingcap/tidb/disttask/operator"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestBackfillOperators(t *testing.T) {
	store, dom := realtikvtest.CreateMockStoreAndDomainAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists op;")
	tk.MustExec("create database op;")
	tk.MustExec("use op;")
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)

	tk.MustExec("create table t(a int primary key, b int, index idx(b));")
	for i := 0; i < 10; i++ {
		tk.MustExec("insert into t values (?, ?)", i*10000, i)
	}
	regionCnt := 10
	tk.MustQuery(fmt.Sprintf("split table t between (0) and (100000) regions %d;", regionCnt)).
		Check(testkit.Rows(fmt.Sprintf("%d 1", regionCnt)))
	// Refresh the region cache.
	tk.MustQuery("select count(*) from t;").Check(testkit.Rows("10"))

	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr("op"), model.NewCIStr("t"))
	require.NoError(t, err)
	startKey := tbl.RecordPrefix()
	endKey := tbl.RecordPrefix().PrefixNext()

	tblInfo := tbl.Meta()
	idxInfo := tblInfo.FindIndexByName("idx")
	copCtx, err := ddl.NewCopContext(tblInfo, idxInfo, tk.Session())
	require.NoError(t, err)

	sessPool := newSessPoolForTest(t, store)

	// Test TableScanTaskSource operator.
	var opTasks []ddl.TableScanTask
	{
		ctx := context.Background()
		pTbl := tbl.(table.PhysicalTable)
		src := ddl.NewTableScanTaskSource(ctx, store, pTbl, startKey, endKey)
		sink := newTestSink[ddl.TableScanTask]()

		operator.Compose[ddl.TableScanTask](src, sink)

		pipeline := operator.NewAsyncPipeline(src, sink)
		err = pipeline.Execute()
		require.NoError(t, err)
		err = pipeline.Close()
		require.NoError(t, err)

		tasks := sink.collect()
		require.Len(t, tasks, 10)
		require.Equal(t, 1, tasks[0].ID)
		require.Equal(t, startKey, tasks[0].Start)
		require.Equal(t, endKey, tasks[9].End)
		opTasks = tasks
	}

	// Test TableScanOperator.
	var chunkResults []ddl.IndexRecordChunk
	{
		// Make sure the buffer is large enough since the chunks do not recycled.
		srcChkPool := make(chan *chunk.Chunk, regionCnt*2)
		for i := 0; i < regionCnt*2; i++ {
			srcChkPool <- chunk.NewChunkWithCapacity(copCtx.FieldTypes(), 100)
		}

		ctx := context.Background()
		src := newTestSource(opTasks...)
		scanOp := ddl.NewTableScanOperator(ctx, sessPool, copCtx, srcChkPool, 3)
		sink := newTestSink[ddl.IndexRecordChunk]()

		operator.Compose[ddl.TableScanTask](src, scanOp)
		operator.Compose[ddl.IndexRecordChunk](scanOp, sink)

		pipeline := operator.NewAsyncPipeline(src, scanOp, sink)
		err = pipeline.Execute()
		require.NoError(t, err)
		err = pipeline.Close()
		require.NoError(t, err)

		results := sink.collect()
		cnt := 0
		for _, rs := range results {
			require.NoError(t, rs.Err)
			chkRowCnt := rs.Chunk.NumRows()
			cnt += chkRowCnt
			if chkRowCnt > 0 {
				chunkResults = append(chunkResults, rs)
			}
		}
		require.Equal(t, 10, cnt)
	}

	// Test IndexIngestOperator.
	{
		ctx := context.Background()
		var keys, values [][]byte
		onWrite := func(key, val []byte) {
			keys = append(keys, key)
			values = append(values, val)
		}

		srcChkPool := make(chan *chunk.Chunk, regionCnt*2)
		pTbl := tbl.(table.PhysicalTable)
		index := tables.NewIndex(pTbl.GetPhysicalID(), tbl.Meta(), idxInfo)
		mockEngine := ingest.NewMockEngineInfo(nil)
		mockEngine.SetHook(onWrite)

		src := newTestSource(chunkResults...)
		ingestOp := ddl.NewIndexIngestOperator(ctx, copCtx, sessPool, pTbl, index, mockEngine, srcChkPool, 3)
		sink := newTestSink[ddl.IndexWriteResult]()

		operator.Compose[ddl.IndexRecordChunk](src, ingestOp)
		operator.Compose[ddl.IndexWriteResult](ingestOp, sink)

		pipeline := operator.NewAsyncPipeline(src, ingestOp, sink)
		err = pipeline.Execute()
		require.NoError(t, err)
		err = pipeline.Close()
		require.NoError(t, err)

		results := sink.collect()
		for _, rs := range results {
			require.NoError(t, rs.Err)
		}
		require.Len(t, keys, 10)
		require.Len(t, values, 10)
		require.Len(t, sink.collect(), 10)
	}
}

func TestBackfillOperatorPipeline(t *testing.T) {
	store, dom := realtikvtest.CreateMockStoreAndDomainAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists op;")
	tk.MustExec("create database op;")
	tk.MustExec("use op;")
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)

	tk.MustExec("create table t(a int primary key, b int, index idx(b));")
	for i := 0; i < 10; i++ {
		tk.MustExec("insert into t values (?, ?)", i*10000, i)
	}
	regionCnt := 10
	tk.MustQuery(fmt.Sprintf("split table t between (0) and (100000) regions %d;", regionCnt)).
		Check(testkit.Rows(fmt.Sprintf("%d 1", regionCnt)))
	// Refresh the region cache.
	tk.MustQuery("select count(*) from t;").Check(testkit.Rows("10"))

	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr("op"), model.NewCIStr("t"))
	require.NoError(t, err)
	startKey := tbl.RecordPrefix()
	endKey := tbl.RecordPrefix().PrefixNext()

	tblInfo := tbl.Meta()
	idxInfo := tblInfo.FindIndexByName("idx")

	sessPool := newSessPoolForTest(t, store)

	ctx := context.Background()
	var keys, values [][]byte
	onWrite := func(key, val []byte) {
		keys = append(keys, key)
		values = append(values, val)
	}
	mockEngine := ingest.NewMockEngineInfo(nil)
	mockEngine.SetHook(onWrite)

	pipeline, err := ddl.NewAddIndexIngestPipeline(
		ctx, store,
		sessPool,
		mockEngine,
		tk.Session(),
		tbl.(table.PhysicalTable),
		idxInfo,
		startKey,
		endKey,
	)
	require.NoError(t, err)
	err = pipeline.Execute()
	require.NoError(t, err)
	err = pipeline.Close()
	require.NoError(t, err)
	require.Len(t, keys, 10)
	require.Len(t, values, 10)
}

type sessPoolForTest struct {
	pool *pools.ResourcePool
}

func newSessPoolForTest(t *testing.T, store kv.Storage) *sessPoolForTest {
	return &sessPoolForTest{
		pool: pools.NewResourcePool(func() (pools.Resource, error) {
			newTk := testkit.NewTestKit(t, store)
			return newTk.Session(), nil
		}, 8, 8, 0),
	}
}

func (p *sessPoolForTest) Get() (sessionctx.Context, error) {
	resource, err := p.pool.Get()
	if err != nil {
		return nil, err
	}
	return resource.(sessionctx.Context), nil
}

func (p *sessPoolForTest) Put(sctx sessionctx.Context) {
	p.pool.Put(sctx.(pools.Resource))
}

type testSink[T any] struct {
	errGroup  errgroup.Group
	ch        chan T
	collected []T
}

func newTestSink[T any]() *testSink[T] {
	return &testSink[T]{
		ch: make(chan T),
	}
}

func (s *testSink[T]) Open() error {
	s.errGroup.Go(func() error {
		for data := range s.ch {
			s.collected = append(s.collected, data)
		}
		return nil
	})
	return nil
}

func (s *testSink[T]) Close() error {
	return s.errGroup.Wait()
}

func (s *testSink[T]) SetSource(dataCh operator.DataChannel[T]) {
	s.ch = dataCh.Channel()
}

func (s *testSink[T]) String() string {
	return "testSink"
}

func (s *testSink[T]) collect() []T {
	return s.collected
}

type testSource[T any] struct {
	errGroup errgroup.Group
	ch       chan T
	toBeSent []T
}

func newTestSource[T any](toBeSent ...T) *testSource[T] {
	return &testSource[T]{
		ch:       make(chan T),
		toBeSent: toBeSent,
	}
}

func (s *testSource[T]) SetSink(sink operator.DataChannel[T]) {
	s.ch = sink.Channel()
}

func (s *testSource[T]) Open() error {
	s.errGroup.Go(func() error {
		for _, data := range s.toBeSent {
			s.ch <- data
		}
		close(s.ch)
		return nil
	})
	return nil
}

func (s *testSource[T]) Close() error {
	return s.errGroup.Wait()
}

func (s *testSource[T]) String() string {
	return "testSource"
}
