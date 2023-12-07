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

package sortexec

import (
	"context"

	"github.com/pingcap/tidb/pkg/util/chunk"
)

// ParallelSortExec represents parallel sorting executor.
type ParallelSortExec struct {
	SortBase
}

// Close implements the Executor Close interface.
func (p *ParallelSortExec) Close() error {
	p.closeBase()
	return nil
}

// Open implements the Executor Open interface.
func (p *ParallelSortExec) Open(ctx context.Context) error {
	p.openBase()
	return nil
}

// Next implements the Executor Next interface.
// TODO rewrite the procedure
// Sort constructs the result following these step:
//  1. Read as mush as rows into memory.
//  2. If memory quota is triggered, sort these rows in memory and put them into disk as partition 1, then reset
//     the memory quota trigger and return to step 1
//  3. If memory quota is not triggered and child is consumed, sort these rows in memory as partition N.
//  4. Merge sort if the count of partitions is larger than 1. If there is only one partition in step 4, it works
//     just like in-memory sort before.
func (p *ParallelSortExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if !p.fetched {
		p.initCompareFuncs()
		p.buildKeyColumns()
		err := p.fetchRowChunks(ctx)
		if err != nil {
			return err
		}
		p.fetched = true
	}
	return nil
}

func (s *ParallelSortExec) fetchRowChunks(ctx context.Context) error {
	return nil
}
