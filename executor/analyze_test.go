// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/types"
)

type recordSet struct {
	data   []types.Datum
	count  int
	cursor int
}

func (r *recordSet) Fields() ([]*ast.ResultField, error) {
	return nil, nil
}

func (r *recordSet) Next() (*ast.Row, error) {
	if r.cursor == r.count {
		return nil, nil
	}
	r.cursor++
	return &ast.Row{Data: []types.Datum{types.NewIntDatum(int64(r.cursor)), r.data[r.cursor-1]}}, nil
}

func (r *recordSet) Close() error {
	r.cursor = 0
	return nil
}

func (s *testSuite) TestCollectSamplesAndEstimateNDVs(c *C) {
	count := 10000
	rs := &recordSet{
		data:   make([]types.Datum, count),
		count:  count,
		cursor: 0,
	}
	pkInfo := &model.ColumnInfo{
		ID: 1,
	}
	start := 1000 // 1000 values is null
	for i := start; i < rs.count; i++ {
		rs.data[i].SetInt64(int64(i))
	}
	for i := start; i < rs.count; i += 3 {
		rs.data[i].SetInt64(rs.data[i].GetInt64() + 1)
	}
	for i := start; i < rs.count; i += 5 {
		rs.data[i].SetInt64(rs.data[i].GetInt64() + 2)
	}

	collectors, pkBuilder, err := executor.CollectSamplesAndEstimateNDVs(mock.NewContext(), rs, 1, pkInfo)
	c.Assert(err, IsNil)
	c.Assert(collectors[0].NullCount+collectors[0].Count, Equals, int64(rs.count))
	c.Assert(collectors[0].Sketch.NDV(), Equals, int64(6624))
	c.Assert(int64(pkBuilder.Count), Equals, int64(rs.count))
	c.Assert(pkBuilder.Hist.NDV, Equals, int64(rs.count))
}
