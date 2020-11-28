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
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
)

func (s *testSuite) TestPartitionRangeSplitter(c *C) {
	ctx := mock.NewContext()
	concurrency := 2
	byItems := []expression.Expression{}
	var input *chunk.Chunk
	output := make([]int, 0)

	splitter := executor.BuildPartitionRangeSplitter(ctx, concurrency, byItems)
	output, err := splitter.Split(ctx, input, output)
	if err != nil {

	}
}
