// Copyright 2024 PingCAP, Inc.
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

package staticrecordset

import (
	"context"

	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/resourcegroup"
	"github.com/pingcap/tidb/pkg/session/cursor"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

var _ sqlexec.RecordSet = &cursorRecordSet{}

// cursorRecordSet wraps a cursor handle with a record set, to close the cursor handle automatically
// when the record set is closed
type cursorRecordSet struct {
	cursor    cursor.Handle
	recordSet sqlexec.RecordSet
	// runawayChecker performs the final runaway deadline check when the cursor is
	// closed. The detached server-side cursor path finishes without going through
	// `ExecStmt.FinishExecuteStmt`, so this is where its post-execution check runs.
	// It may be nil when resource control is disabled.
	runawayChecker resourcegroup.RunawayChecker
}

func (c *cursorRecordSet) Fields() []*resolve.ResultField {
	return c.recordSet.Fields()
}

func (c *cursorRecordSet) Next(ctx context.Context, req *chunk.Chunk) error {
	return c.recordSet.Next(ctx, req)
}

func (c *cursorRecordSet) NewChunk(alloc chunk.Allocator) *chunk.Chunk {
	return c.recordSet.NewChunk(alloc)
}

func (c *cursorRecordSet) Close() error {
	// Final runaway deadline check for the detached cursor. It is a no-op if an
	// in-flight path already marked the query during fetches.
	if c.runawayChecker != nil {
		c.runawayChecker.AfterExecutor()
	}
	c.cursor.Close()
	return c.recordSet.Close()
}

// GetExecutor4Test exports the internal executor for test purpose.
func (c *cursorRecordSet) GetExecutor4Test() any {
	return c.recordSet.(interface{ GetExecutor4Test() any }).GetExecutor4Test()
}

// WrapRecordSetWithCursor wraps a record set with a cursor handle. The cursor handle will be closed
// automatically when the record set is closed. The runawayChecker (which may be nil) runs the final
// runaway deadline check on close, since the detached cursor path does not reach
// `ExecStmt.FinishExecuteStmt`.
func WrapRecordSetWithCursor(cursor cursor.Handle, recordSet sqlexec.RecordSet, runawayChecker resourcegroup.RunawayChecker) sqlexec.RecordSet {
	return &cursorRecordSet{cursor: cursor, recordSet: recordSet, runawayChecker: runawayChecker}
}
