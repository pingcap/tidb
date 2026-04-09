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
	"github.com/pingcap/tidb/pkg/resourcegroup"
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
	// finishLock is a mutex used to synchronize access to the `Next`,`Finish` and `Close` functions of the adapter.
	// It ensures that only one goroutine can access the `Next`,`Finish` and `Close` functions at a time, preventing race conditions.
	// When we terminate the current SQL externally (e.g., kill query), an additional goroutine would be used to call the `Finish` function.
	finishLock sync.Mutex
	closed     int32
}

// CursorRUV2Tracker keeps reporting state for server-side cursor fetches.
type CursorRUV2Tracker struct {
	reporter          resourcegroup.ConsumptionReporter
	metrics           *execdetails.RUV2Metrics
	ruDetails         *clientutil.RUDetails
	resourceGroupName string
	weights           execdetails.RUV2Weights
	reportedTiDBRU    float64
	reportedTiKVRUV2  float64
	reportedTiFlashRU float64
	mu                sync.Mutex
}

// NewCursorRUV2Tracker creates a tracker that reports cursor fetch deltas.
func NewCursorRUV2Tracker(
	reporter resourcegroup.ConsumptionReporter,
	resourceGroupName string,
	metrics *execdetails.RUV2Metrics,
	ruDetails *clientutil.RUDetails,
	weights execdetails.RUV2Weights,
) *CursorRUV2Tracker {
	if metrics == nil && ruDetails == nil {
		return nil
	}
	if metrics != nil && metrics.Bypass() {
		return nil
	}
	tracker := &CursorRUV2Tracker{
		reporter:          reporter,
		resourceGroupName: resourceGroupName,
		metrics:           metrics,
		ruDetails:         ruDetails,
		weights:           weights,
	}
	if metrics != nil {
		tracker.reportedTiDBRU = metrics.CalculateRUValues(weights)
	}
	if ruDetails != nil {
		tracker.reportedTiKVRUV2 = ruDetails.TiKVRUV2()
		tracker.reportedTiFlashRU = ruDetails.TiflashRU()
	}
	return tracker
}

func (t *CursorRUV2Tracker) addResultChunkCells(delta int64) {
	if t == nil || t.metrics == nil || delta <= 0 {
		return
	}
	t.metrics.AddResultChunkCells(delta)
}

func (t *CursorRUV2Tracker) reportDelta() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	var currentTiDBRU float64
	if t.metrics != nil {
		currentTiDBRU = t.metrics.CalculateRUValues(t.weights)
	}
	currentTiKVRUV2 := t.reportedTiKVRUV2
	currentTiFlashRU := t.reportedTiFlashRU
	if t.ruDetails != nil {
		currentTiKVRUV2 = t.ruDetails.TiKVRUV2()
		currentTiFlashRU = t.ruDetails.TiflashRU()
	}

	if t.reporter != nil && len(t.resourceGroupName) > 0 {
		deltaTiKVRUV2 := currentTiKVRUV2 - t.reportedTiKVRUV2
		deltaTiDBRU := currentTiDBRU - t.reportedTiDBRU
		deltaTiFlashRU := currentTiFlashRU - t.reportedTiFlashRU
		if deltaTiKVRUV2 > 0 || deltaTiDBRU > 0 || deltaTiFlashRU > 0 {
			t.reporter.ReportRUV2Consumption(
				t.resourceGroupName,
				max(deltaTiKVRUV2, 0),
				max(deltaTiDBRU, 0),
				max(deltaTiFlashRU, 0),
			)
		}
	}

	t.reportedTiDBRU = currentTiDBRU
	t.reportedTiKVRUV2 = currentTiKVRUV2
	t.reportedTiFlashRU = currentTiFlashRU
}

type cursorRUV2Trackable interface {
	setCursorRUV2Tracker(*CursorRUV2Tracker)
	reportCursorRUV2Delta(resultChunkCellsDelta int64)
}

// AttachCursorRUV2Tracker binds a cursor tracker to the result set if supported.
func AttachCursorRUV2Tracker(rs ResultSet, tracker *CursorRUV2Tracker) {
	if trackable, ok := rs.(cursorRUV2Trackable); ok {
		trackable.setCursorRUV2Tracker(tracker)
	}
}

// ReportCursorRUV2Delta reports any pending cursor RUv2 delta if supported.
// resultChunkCellsDelta is added to the cursor tracker before reporting.
func ReportCursorRUV2Delta(rs ResultSet, resultChunkCellsDelta int64) {
	if trackable, ok := rs.(cursorRUV2Trackable); ok {
		trackable.reportCursorRUV2Delta(resultChunkCellsDelta)
	}
}

func (trs *tidbResultSet) NewChunk(alloc chunk.Allocator) *chunk.Chunk {
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

func (trs *tidbResultSet) reportCursorRUV2Delta(resultChunkCellsDelta int64) {
	if trs.cursorRUV2 != nil {
		trs.cursorRUV2.addResultChunkCells(resultChunkCellsDelta)
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
