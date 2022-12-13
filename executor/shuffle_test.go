// Copyright 2020 PingCAP, Inc.
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

package executor

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
)

var _ = SerialSuites(&testPartitionSuite{})

type testPartitionSuite struct {
}

func (s *testPartitionSuite) TestPartitionRangeSplitter(c *C) {
	ctx := mock.NewContext()
	concurrency := 2

	tp := &types.FieldType{Tp: mysql.TypeVarchar}
	col0 := &expression.Column{
		RetType: tp,
		Index:   0,
	}
	byItems := []expression.Expression{col0}

	input := chunk.New([]*types.FieldType{tp}, 1024, 1024)
	input.Reset()
	input.Column(0).AppendString("a")
	input.Column(0).AppendString("a")
	input.Column(0).AppendString("a")
	input.Column(0).AppendString("a")
	input.Column(0).AppendString("c")
	input.Column(0).AppendString("c")
	input.Column(0).AppendString("b")
	input.Column(0).AppendString("b")
	input.Column(0).AppendString("b")
	input.Column(0).AppendString("q")
	input.Column(0).AppendString("eee")
	input.Column(0).AppendString("eee")
	input.Column(0).AppendString("ddd")

	expected := []int{0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 0, 0, 1}
	obtained := make([]int, 0)

	splitter := buildPartitionRangeSplitter(ctx, concurrency, byItems)
	obtained, err := splitter.split(ctx, input, obtained)
	c.Assert(err, IsNil)
	c.Assert(len(obtained), Equals, len(expected))
	for i := 0; i < len(obtained); i++ {
		c.Assert(obtained[i], Equals, expected[i])
	}
}
