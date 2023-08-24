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

package ddl_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/disttask/operator"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

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

func TestTableScanTaskSource(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t values(1), (2), (3);")

	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	startKey := tbl.RecordPrefix()
	endKey := tbl.RecordPrefix().PrefixNext()

	ctx := context.Background()
	src := ddl.NewTableScanTaskSource(ctx, store, tbl.(table.PhysicalTable), startKey, endKey)
	sink := newTestSink[ddl.OpTaskTableScan]()

	operator.Compose[ddl.OpTaskTableScan](src, sink)

	pipeline := operator.NewAsyncPipeline(src, sink)
	err = pipeline.Execute()
	require.NoError(t, err)
	err = pipeline.Close()
	require.NoError(t, err)

	tasks := sink.collect()
	require.Len(t, tasks, 1)
	require.Equal(t, 0, tasks[0].ID)
	require.Equal(t, startKey, tasks[0].Start)
	require.Equal(t, endKey, tasks[0].End)
}

func test

func TestBackfillOperators(t *testing.T) {
}

func TestBackfillOperatorCancel(t *testing.T) {

}
