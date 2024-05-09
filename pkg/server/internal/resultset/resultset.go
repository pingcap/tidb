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

package resultset

import (
	"context"
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/server/internal/column"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

// ResultSet is the result set of an query.
type ResultSet interface {
	Columns() []*column.Info
	NewChunk(chunk.Allocator) *chunk.Chunk
	Next(context.Context, *chunk.Chunk) error
	Close()
	// IsClosed checks whether the result set is closed.
	IsClosed() bool
	FieldTypes() []*types.FieldType
	SetPreparedStmt(stmt *core.PlanCacheStmt)
	Finish() error
}

var _ ResultSet = &tidbResultSet{}

// New creates a new result set
func New(recordSet sqlexec.RecordSet, preparedStmt *core.PlanCacheStmt) ResultSet {
	return &tidbResultSet{
		recordSet:    recordSet,
		preparedStmt: preparedStmt,
	}
}

type tidbResultSet struct {
	recordSet    sqlexec.RecordSet
	preparedStmt *core.PlanCacheStmt
	columns      []*column.Info
	closed       int32
}

func (trs *tidbResultSet) NewChunk(alloc chunk.Allocator) *chunk.Chunk {
	return trs.recordSet.NewChunk(alloc)
}

func (trs *tidbResultSet) Next(ctx context.Context, req *chunk.Chunk) error {
	return trs.recordSet.Next(ctx, req)
}

func (trs *tidbResultSet) Finish() error {
	if x, ok := trs.recordSet.(interface{ Finish() error }); ok {
		return x.Finish()
	}
	return nil
}

func (trs *tidbResultSet) Close() {
	if !atomic.CompareAndSwapInt32(&trs.closed, 0, 1) {
		return
	}
	terror.Call(trs.recordSet.Close)
	trs.recordSet = nil
}

// IsClosed implements ResultSet.IsClosed interface.
func (trs *tidbResultSet) IsClosed() bool {
	return atomic.LoadInt32(&trs.closed) == 1
}

// OnFetchReturned implements FetchNotifier#OnFetchReturned
func (trs *tidbResultSet) OnFetchReturned() {
	if cl, ok := trs.recordSet.(FetchNotifier); ok {
		cl.OnFetchReturned()
	}
}

// Columns implements ResultSet.Columns interface.
func (trs *tidbResultSet) Columns() []*column.Info {
	if trs.columns != nil {
		return trs.columns
	}
	// for prepare statement, try to get cached columnInfo array
	if trs.preparedStmt != nil {
		ps := trs.preparedStmt
		if colInfos, ok := ps.PointGet.ColumnInfos.([]*column.Info); ok {
			trs.columns = colInfos
		}
	}
	if trs.columns == nil {
		fields := trs.recordSet.Fields()
		for _, v := range fields {
			trs.columns = append(trs.columns, column.ConvertColumnInfo(v))
		}
		if trs.preparedStmt != nil {
			// if Info struct has allocated object,
			// here maybe we need deep copy Info to do caching
			trs.preparedStmt.PointGet.ColumnInfos = trs.columns
		}
	}
	return trs.columns
}

// FieldTypes implements ResultSet.FieldTypes interface.
func (trs *tidbResultSet) FieldTypes() []*types.FieldType {
	fts := make([]*types.FieldType, 0, len(trs.recordSet.Fields()))
	for _, f := range trs.recordSet.Fields() {
		fts = append(fts, &f.Column.FieldType)
	}
	return fts
}

// SetPreparedStmt implements ResultSet.SetPreparedStmt interface.
func (trs *tidbResultSet) SetPreparedStmt(stmt *core.PlanCacheStmt) {
	trs.preparedStmt = stmt
}
