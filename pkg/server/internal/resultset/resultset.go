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
	"sync"
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/server/internal/column"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	clientutil "github.com/tikv/client-go/v2/util"
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
	TryDetach() (ResultSet, bool, error)
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
	cursorRUV2   *CursorRUV2Tracker
	columns      []*column.Info
	// finishLock is a mutex used to synchronize access to the `NewChunk`, `Next`, `Finish` and `Close` functions of the adapter.
	// It ensures that only one goroutine can access the result set lifecycle at a time, preventing race conditions.
	// When we terminate the current SQL externally (e.g., kill query), an additional goroutine would be used to call the `Finish` function.
	finishLock sync.Mutex
	closed     int32
}

// CursorRUV2Tracker synchronizes cursor response bytes used by RU v3.
type CursorRUV2Tracker struct {
	metrics   *execdetails.RUV2Metrics
	ruDetails *clientutil.RUDetails
	mu        sync.Mutex
}

// NewCursorRUV2Tracker creates a tracker for cursor response bytes.
func NewCursorRUV2Tracker(
	metrics *execdetails.RUV2Metrics,
	ruDetails *clientutil.RUDetails,
) *CursorRUV2Tracker {
	if metrics == nil || ruDetails == nil || metrics.Bypass() {
		return nil
	}
	tracker := &CursorRUV2Tracker{
		metrics:   metrics,
		ruDetails: ruDetails,
	}
	execdetails.SyncRUV2MetricsFromRUDetails(tracker.metrics, tracker.ruDetails)
	return tracker
}

func (t *CursorRUV2Tracker) reportDelta() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	execdetails.SyncRUV2MetricsFromRUDetails(t.metrics, t.ruDetails)
}

type cursorRUV2Trackable interface {
	setCursorRUV2Tracker(*CursorRUV2Tracker)
	reportCursorRUV2Delta()
}

// AttachCursorRUV2Tracker binds a cursor tracker to the result set if supported.
func AttachCursorRUV2Tracker(rs ResultSet, tracker *CursorRUV2Tracker) {
	if trackable, ok := rs.(cursorRUV2Trackable); ok {
		trackable.setCursorRUV2Tracker(tracker)
	}
}

// ReportCursorRUV2Delta synchronizes pending cursor response bytes for RU v3 if supported.
func ReportCursorRUV2Delta(rs ResultSet) {
	if trackable, ok := rs.(cursorRUV2Trackable); ok {
		trackable.reportCursorRUV2Delta()
	}
}

func (trs *tidbResultSet) NewChunk(alloc chunk.Allocator) *chunk.Chunk {
	trs.finishLock.Lock()
	defer trs.finishLock.Unlock()
	return trs.recordSet.NewChunk(alloc)
}

func (trs *tidbResultSet) Next(ctx context.Context, req *chunk.Chunk) error {
	trs.finishLock.Lock()
	defer trs.finishLock.Unlock()
	return trs.recordSet.Next(ctx, req)
}

func (trs *tidbResultSet) Finish() error {
	if trs.finishLock.TryLock() {
		defer trs.finishLock.Unlock()
		if x, ok := trs.recordSet.(interface{ Finish() error }); ok {
			return x.Finish()
		}
	}
	return nil
}

func (trs *tidbResultSet) Close() {
	trs.finishLock.Lock()
	defer trs.finishLock.Unlock()
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

func (trs *tidbResultSet) setCursorRUV2Tracker(tracker *CursorRUV2Tracker) {
	trs.cursorRUV2 = tracker
}

func (trs *tidbResultSet) reportCursorRUV2Delta() {
	if trs.cursorRUV2 != nil {
		trs.cursorRUV2.reportDelta()
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

// TryDetach creates a new `ResultSet` which doesn't depend on the current session context.
func (trs *tidbResultSet) TryDetach() (ResultSet, bool, error) {
	detachableRecordSet, ok := trs.recordSet.(sqlexec.DetachableRecordSet)
	if !ok {
		return nil, false, nil
	}

	recordSet, detached, err := detachableRecordSet.TryDetach()
	if !detached || err != nil {
		return nil, detached, err
	}

	return &tidbResultSet{
		recordSet:    recordSet,
		preparedStmt: trs.preparedStmt,
		columns:      trs.columns,
	}, true, nil
}
