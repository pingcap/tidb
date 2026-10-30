// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package runaway

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	mockctx "github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
)

type stubSessionPool struct {
	resource pools.Resource
}

func (p *stubSessionPool) Get() (pools.Resource, error) {
	return p.resource, nil
}

func (*stubSessionPool) Put(pools.Resource) {}

func (*stubSessionPool) Close() {}

type stubRestrictedSession struct {
	*mockctx.Context
	rows []chunk.Row
	err  error

	sql  string
	args []any
}

func newStubRestrictedSession(rows []chunk.Row) *stubRestrictedSession {
	return &stubRestrictedSession{
		Context: mockctx.NewContext(),
		rows:    rows,
	}
}

func (*stubRestrictedSession) Close() {}

func (s *stubRestrictedSession) GetRestrictedSQLExecutor() sqlexec.RestrictedSQLExecutor {
	return s
}

func (*stubRestrictedSession) ParseWithParams(context.Context, string, ...any) (ast.StmtNode, error) {
	return nil, errors.New("unexpected ParseWithParams call")
}

func (*stubRestrictedSession) ExecRestrictedStmt(context.Context, ast.StmtNode, ...sqlexec.OptionFuncAlias) ([]chunk.Row, []*resolve.ResultField, error) {
	return nil, nil, errors.New("unexpected ExecRestrictedStmt call")
}

func (s *stubRestrictedSession) ExecRestrictedSQL(_ context.Context, _ []sqlexec.OptionFuncAlias, sql string, args ...any) ([]chunk.Row, []*resolve.ResultField, error) {
	s.sql = sql
	s.args = append([]any(nil), args...)
	return s.rows, nil, s.err
}

// makeWatchRows generates n watch rows with start_time starting from baseTime,
// incremented by step per row. step=0 gives identical timestamps.
func makeWatchRows(n int, baseTime time.Time, step time.Duration) []chunk.Row {
	rows := make([]chunk.Row, n)
	for i := range rows {
		st := baseTime.Add(time.Duration(i) * step)
		rows[i] = newWatchRow(int64(i+1), "rg", st, nil)
	}
	return rows
}

// makeWatchDoneRows generates n watch_done rows with done_time starting from
// baseDoneTime, incremented by step per row. start_time is fixed.
func makeWatchDoneRows(n int, baseDoneTime time.Time, step time.Duration) []chunk.Row {
	fixedStart := baseDoneTime.Add(-time.Hour)
	rows := make([]chunk.Row, n)
	for i := range rows {
		dt := baseDoneTime.Add(time.Duration(i) * step)
		rows[i] = newWatchDoneRow(int64(i+1), int64(i+1), "rg", fixedStart, nil, dt)
	}
	return rows
}

func newInvalidWatchDoneRow(doneID int64) chunk.Row {
	return buildRow(
		types.NewIntDatum(doneID),
		types.NewIntDatum(doneID),
		types.NewStringDatum("rg"),
		types.NewTimeDatum(types.ZeroDatetime),
		types.NewDatum(nil),
		types.NewIntDatum(1),
		types.NewStringDatum("select 1"),
		types.NewStringDatum("manual"),
		types.NewIntDatum(2),
		types.NewStringDatum("rg_dst"),
		types.NewStringDatum("watch-rule"),
		timeDatum(time.Date(2026, 4, 14, 12, 0, 0, 0, time.UTC)),
	)
}

// scanTestCase parameterizes watch vs watch_done so each test function
// covers one code path without duplicating the two-table variant.
type scanTestCase struct {
	name        string
	makeReader  func(time.Time) *systemTableReader
	makeRows    func(int, time.Time, time.Duration) []chunk.Row
	makeInvalid func(int64) chunk.Row
	setup       func(*syncer, *systemTableReader)
	scan        func(*syncer) ([]*QuarantineRecord, error)
}

var scanCases = []scanTestCase{
	{
		name:        "watch",
		makeReader:  newTestWatchReader,
		makeRows:    makeWatchRows,
		makeInvalid: func(id int64) chunk.Row { return newInvalidWatchRow(id, "rg") },
		setup:       func(s *syncer, r *systemTableReader) { s.newWatchReader = r },
		scan:        func(s *syncer) ([]*QuarantineRecord, error) { return s.getNewWatchRecords() },
	},
	{
		name:        "watch_done",
		makeReader:  newTestWatchDoneReader,
		makeRows:    makeWatchDoneRows,
		makeInvalid: newInvalidWatchDoneRow,
		setup:       func(s *syncer, r *systemTableReader) { s.deletionWatchReader = r },
		scan:        func(s *syncer) ([]*QuarantineRecord, error) { return s.getNewWatchDoneRecords() },
	},
}

func newTestWatchReader(checkpoint time.Time) *systemTableReader {
	r := newSystemTableReader(
		runawayWatchFullTableName, "start_time", watchColStartTime, watchRecordColumns,
	)
	r.CheckPoint = checkpoint
	return r
}

func newTestWatchDoneReader(checkpoint time.Time) *systemTableReader {
	r := newSystemTableReader(
		runawayWatchDoneFullTableName, "done_time", watchDoneColDoneTime, watchDoneRecordColumns,
	)
	r.CheckPoint = checkpoint
	return r
}

func timeDatum(t time.Time) types.Datum {
	return types.NewTimeDatum(types.NewTime(types.FromGoTime(t.UTC()), mysql.TypeDatetime, 6))
}

func buildRow(datums ...types.Datum) chunk.Row {
	return chunk.MutRowFromDatums(datums).ToRow()
}

func newWatchRow(id int64, groupName string, startTime time.Time, endTime *time.Time) chunk.Row {
	endDatum := types.NewDatum(nil)
	if endTime != nil {
		endDatum = timeDatum(*endTime)
	}
	return buildRow(
		types.NewIntDatum(id),
		types.NewStringDatum(groupName),
		timeDatum(startTime),
		endDatum,
		types.NewIntDatum(1),
		types.NewStringDatum("select 1"),
		types.NewStringDatum("manual"),
		types.NewIntDatum(2),
		types.NewStringDatum("rg_dst"),
		types.NewStringDatum("watch-rule"),
	)
}

func newInvalidWatchRow(id int64, groupName string) chunk.Row {
	return buildRow(
		types.NewIntDatum(id),
		types.NewStringDatum(groupName),
		types.NewTimeDatum(types.ZeroDatetime),
		types.NewDatum(nil),
		types.NewIntDatum(1),
		types.NewStringDatum("select 1"),
		types.NewStringDatum("manual"),
		types.NewIntDatum(2),
		types.NewStringDatum("rg_dst"),
		types.NewStringDatum("watch-rule"),
	)
}

func newWatchDoneRow(doneID, recordID int64, groupName string, startTime time.Time, endTime *time.Time, doneTime time.Time) chunk.Row {
	endDatum := types.NewDatum(nil)
	if endTime != nil {
		endDatum = timeDatum(*endTime)
	}
	return buildRow(
		types.NewIntDatum(doneID),
		types.NewIntDatum(recordID),
		types.NewStringDatum(groupName),
		timeDatum(startTime),
		endDatum,
		types.NewIntDatum(1),
		types.NewStringDatum("select 1"),
		types.NewStringDatum("manual"),
		types.NewIntDatum(2),
		types.NewStringDatum("rg_dst"),
		types.NewStringDatum("watch-rule"),
		timeDatum(doneTime),
	)
}

func TestGenSelectStmts(t *testing.T) {
	checkpoint := time.Date(2026, 4, 14, 9, 59, 0, 0, time.UTC)
	upperBound := checkpoint.Add(10 * time.Second)
	reader := newTestWatchReader(checkpoint)
	reader.UpperBound = upperBound

	sql, params := reader.genSelectStmt()
	require.Equal(t, "select * from mysql.tidb_runaway_watch where start_time >= %? and start_time < %? order by start_time limit %?", sql)
	require.Equal(t, []any{checkpoint, upperBound, watchSyncBatchLimit}, params)

	sqlGenFn := reader.genSelectByIDStmt(42)
	sql, params = sqlGenFn()
	require.Equal(t, "select * from mysql.tidb_runaway_watch where id = %?", sql)
	require.Equal(t, []any{int64(42)}, params)

	sqlGenFn = reader.genSelectByGroupStmt("rg_bulk")
	sql, params = sqlGenFn()
	require.Equal(t, "select * from mysql.tidb_runaway_watch where resource_group_name = %?", sql)
	require.Equal(t, []any{"rg_bulk"}, params)
}

func TestWatchAdvanceCheckpoint(t *testing.T) {
	// issue:67754
	startTime := time.Date(2026, 4, 14, 10, 0, 0, 123000000, time.UTC)
	endTime := startTime.Add(5 * time.Minute)
	initialCheckpoint := time.Date(2026, 4, 14, 9, 59, 0, 0, time.UTC)
	session := newStubRestrictedSession([]chunk.Row{newWatchRow(30005, "rg_bulk", startTime, &endTime)})
	reader := newTestWatchReader(initialCheckpoint)
	s := &syncer{
		sysSessionPool: &stubSessionPool{resource: session},
		newWatchReader: reader,
	}

	before := time.Now().UTC()
	records, err := s.getNewWatchRecords()
	after := time.Now().UTC()

	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, int64(30005), records[0].ID)
	require.Equal(t, "rg_bulk", records[0].ResourceGroupName)
	require.Equal(t, endTime, records[0].EndTime)
	require.Contains(t, session.sql, "where start_time >= %? and start_time < %? order by start_time limit %?")
	require.Len(t, session.args, 3)

	checkpointArg, ok := session.args[0].(time.Time)
	require.True(t, ok)
	require.True(t, checkpointArg.Equal(initialCheckpoint))
	upperBoundArg, ok := session.args[1].(time.Time)
	require.True(t, ok)
	require.False(t, upperBoundArg.Before(before))
	require.False(t, upperBoundArg.After(after))
	require.Equal(t, watchSyncBatchLimit, session.args[2])
	// Partial batch (1 < 2048): checkpoint advances to UpperBound - overlap
	require.True(t, reader.CheckPoint.Equal(upperBoundArg.Add(-watchSyncOverlap)))
}

func TestWatchHoldCheckpoint(t *testing.T) {
	initialCheckpoint := time.Date(2026, 4, 14, 9, 59, 0, 0, time.UTC)

	t.Run("empty result", func(t *testing.T) {
		session := newStubRestrictedSession(nil)
		reader := newTestWatchReader(initialCheckpoint)
		s := &syncer{
			sysSessionPool: &stubSessionPool{resource: session},
			newWatchReader: reader,
		}

		records, err := s.getNewWatchRecords()
		require.NoError(t, err)
		require.Empty(t, records)
		require.True(t, reader.CheckPoint.Equal(initialCheckpoint))
	})

	t.Run("all rows dropped during decode", func(t *testing.T) {
		session := newStubRestrictedSession([]chunk.Row{newInvalidWatchRow(30006, "rg_bulk")})
		reader := newTestWatchReader(initialCheckpoint)
		s := &syncer{
			sysSessionPool: &stubSessionPool{resource: session},
			newWatchReader: reader,
		}

		records, err := s.getNewWatchRecords()
		require.NoError(t, err)
		require.Empty(t, records)
		require.True(t, reader.CheckPoint.Equal(initialCheckpoint))
	})

	t.Run("sql error", func(t *testing.T) {
		session := newStubRestrictedSession(nil)
		session.err = errors.New("read failed")
		reader := newTestWatchReader(initialCheckpoint)
		s := &syncer{
			sysSessionPool: &stubSessionPool{resource: session},
			newWatchReader: reader,
		}

		records, err := s.getNewWatchRecords()
		require.ErrorContains(t, err, "read failed")
		require.Nil(t, records)
		require.True(t, reader.CheckPoint.Equal(initialCheckpoint))
	})
}

func TestWatchDoneAdvanceCheckpoint(t *testing.T) {
	startTime := time.Date(2026, 4, 14, 10, 0, 0, 123000000, time.UTC)
	endTime := startTime.Add(5 * time.Minute)
	doneTime := time.Date(2026, 4, 14, 10, 1, 0, 123000000, time.UTC)
	initialCheckpoint := time.Date(2026, 4, 14, 9, 59, 0, 0, time.UTC)
	session := newStubRestrictedSession([]chunk.Row{newWatchDoneRow(1, 30005, "rg_bulk", startTime, &endTime, doneTime)})
	reader := newTestWatchDoneReader(initialCheckpoint)
	s := &syncer{
		sysSessionPool:      &stubSessionPool{resource: session},
		deletionWatchReader: reader,
	}

	before := time.Now().UTC()
	records, err := s.getNewWatchDoneRecords()
	after := time.Now().UTC()

	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, int64(30005), records[0].ID)
	require.Equal(t, endTime, records[0].EndTime)
	require.Contains(t, session.sql, "where done_time >= %? and done_time < %? order by done_time limit %?")
	require.Len(t, session.args, 3)

	checkpointArg, ok := session.args[0].(time.Time)
	require.True(t, ok)
	require.True(t, checkpointArg.Equal(initialCheckpoint))
	upperBoundArg, ok := session.args[1].(time.Time)
	require.True(t, ok)
	require.False(t, upperBoundArg.Before(before))
	require.False(t, upperBoundArg.After(after))
	require.Equal(t, watchSyncBatchLimit, session.args[2])
	require.True(t, reader.CheckPoint.Equal(upperBoundArg.Add(-watchSyncOverlap)))
}

func TestWatchDoneHoldCheckpoint(t *testing.T) {
	initialCheckpoint := time.Date(2026, 4, 14, 9, 59, 0, 0, time.UTC)

	t.Run("empty result", func(t *testing.T) {
		session := newStubRestrictedSession(nil)
		reader := newTestWatchDoneReader(initialCheckpoint)
		s := &syncer{
			sysSessionPool:      &stubSessionPool{resource: session},
			deletionWatchReader: reader,
		}

		records, err := s.getNewWatchDoneRecords()
		require.NoError(t, err)
		require.Empty(t, records)
		require.True(t, reader.CheckPoint.Equal(initialCheckpoint))
	})

	t.Run("sql error", func(t *testing.T) {
		session := newStubRestrictedSession(nil)
		session.err = errors.New("read failed")
		reader := newTestWatchDoneReader(initialCheckpoint)
		s := &syncer{
			sysSessionPool:      &stubSessionPool{resource: session},
			deletionWatchReader: reader,
		}

		records, err := s.getNewWatchDoneRecords()
		require.ErrorContains(t, err, "read failed")
		require.Nil(t, records)
		require.True(t, reader.CheckPoint.Equal(initialCheckpoint))
	})
}

func TestScanFullBatch(t *testing.T) {
	baseTime := time.Date(2026, 4, 14, 10, 0, 0, 0, time.UTC)
	initialCP := baseTime.Add(-time.Minute)

	for _, tc := range scanCases {
		t.Run(tc.name, func(t *testing.T) {
			rows := tc.makeRows(watchSyncBatchLimit, baseTime, time.Microsecond)
			session := newStubRestrictedSession(rows)
			reader := tc.makeReader(initialCP)
			s := &syncer{sysSessionPool: &stubSessionPool{resource: session}}
			tc.setup(s, reader)

			before := time.Now().UTC()
			records, err := tc.scan(s)
			after := time.Now().UTC()

			require.NoError(t, err)
			require.Len(t, records, watchSyncBatchLimit)

			lastKeyTime := baseTime.Add(time.Duration(watchSyncBatchLimit-1) * time.Microsecond)
			require.True(t, reader.CheckPoint.Equal(lastKeyTime),
				"full batch: checkpoint must be last row's key time, got %v want %v", reader.CheckPoint, lastKeyTime)
			require.True(t, reader.lastScanKeyTime.Equal(lastKeyTime))
			// Must differ from the partial-batch path.
			require.False(t, reader.CheckPoint.Equal(reader.UpperBound.Add(-watchSyncOverlap)))
			require.False(t, reader.UpperBound.Before(before))
			require.False(t, reader.UpperBound.After(after))
		})
	}
}

func TestScanFullBatchSameKey(t *testing.T) {
	baseTime := time.Date(2026, 4, 14, 10, 0, 0, 0, time.UTC)

	for _, tc := range scanCases {
		t.Run(tc.name, func(t *testing.T) {
			rows := tc.makeRows(watchSyncBatchLimit, baseTime, 0)
			session := newStubRestrictedSession(rows)
			reader := tc.makeReader(baseTime) // CheckPoint == all rows' key time
			s := &syncer{sysSessionPool: &stubSessionPool{resource: session}}
			tc.setup(s, reader)

			records, err := tc.scan(s)

			require.NoError(t, err)
			require.Len(t, records, watchSyncBatchLimit)
			require.True(t, reader.lastScanKeyTime.Equal(baseTime))
			// Livelock guard: must fall back to UpperBound - overlap.
			require.True(t, reader.CheckPoint.Equal(reader.UpperBound.Add(-watchSyncOverlap)),
				"same-key full batch: checkpoint must fall back to UpperBound-overlap")
		})
	}
}

func TestScanPagination(t *testing.T) {
	baseTime := time.Date(2026, 4, 14, 10, 0, 0, 0, time.UTC)
	initialCP := baseTime.Add(-time.Minute)
	page2Count := 500

	for _, tc := range scanCases {
		t.Run(tc.name, func(t *testing.T) {
			page1 := tc.makeRows(watchSyncBatchLimit, baseTime, time.Microsecond)
			page2Start := baseTime.Add(time.Duration(watchSyncBatchLimit) * time.Microsecond)
			page2 := tc.makeRows(page2Count, page2Start, time.Microsecond)

			session := newStubRestrictedSession(page1)
			reader := tc.makeReader(initialCP)
			s := &syncer{sysSessionPool: &stubSessionPool{resource: session}}
			tc.setup(s, reader)

			// Page 1: full batch.
			records1, err := tc.scan(s)
			require.NoError(t, err)
			require.Len(t, records1, watchSyncBatchLimit)
			cp1 := reader.CheckPoint
			lastPage1Key := baseTime.Add(time.Duration(watchSyncBatchLimit-1) * time.Microsecond)
			require.True(t, cp1.Equal(lastPage1Key))

			// Page 2: partial batch.
			session.rows = page2
			records2, err := tc.scan(s)
			require.NoError(t, err)
			require.Len(t, records2, page2Count)
			cp2 := reader.CheckPoint

			require.True(t, cp2.After(cp1), "checkpoint must advance monotonically")
			require.True(t, cp2.Equal(reader.UpperBound.Add(-watchSyncOverlap)))
		})
	}
}

func TestScanInvalidTail(t *testing.T) {
	baseTime := time.Date(2026, 4, 14, 10, 0, 0, 0, time.UTC)
	initialCP := baseTime.Add(-time.Minute)
	validCount := watchSyncBatchLimit - 2

	for _, tc := range scanCases {
		t.Run(tc.name, func(t *testing.T) {
			valid := tc.makeRows(validCount, baseTime, time.Microsecond)
			rows := append(valid,
				tc.makeInvalid(int64(validCount+1)),
				tc.makeInvalid(int64(validCount+2)),
			)
			require.Len(t, rows, watchSyncBatchLimit) // raw rows hit LIMIT

			session := newStubRestrictedSession(rows)
			reader := tc.makeReader(initialCP)
			s := &syncer{sysSessionPool: &stubSessionPool{resource: session}}
			tc.setup(s, reader)

			records, err := tc.scan(s)

			require.NoError(t, err)
			require.Len(t, records, validCount) // invalid rows dropped
			// len(records) < watchSyncBatchLimit → partial-batch path.
			require.True(t, reader.CheckPoint.Equal(reader.UpperBound.Add(-watchSyncOverlap)))
			// lastScanKeyTime reflects the last *valid* row.
			lastValidKey := baseTime.Add(time.Duration(validCount-1) * time.Microsecond)
			require.True(t, reader.lastScanKeyTime.Equal(lastValidKey))
		})
	}
}

// TestPointQueryPreservesScanState guards the invariant that
// getWatchRecordByID / getWatchRecordByGroup leave scan-cursor state
// (lastScanKeyTime, CheckPoint, UpperBound) untouched, so manual
// RemoveRunawayWatch* calls between sync ticks can't perturb the cursor.
func TestPointQueryPreservesScanState(t *testing.T) {
	startTime := time.Date(2026, 4, 14, 10, 0, 0, 123000000, time.UTC)
	endTime := startTime.Add(5 * time.Minute)
	initialCheckpoint := time.Date(2026, 4, 14, 9, 59, 0, 0, time.UTC)
	initialUpperBound := time.Date(2026, 4, 14, 9, 59, 30, 0, time.UTC)
	initialLastScanKey := time.Date(2026, 4, 14, 9, 58, 45, 0, time.UTC)

	newReader := func() *systemTableReader {
		r := newTestWatchReader(initialCheckpoint)
		r.UpperBound = initialUpperBound
		r.lastScanKeyTime = initialLastScanKey
		return r
	}

	cases := []struct {
		name string
		call func(*syncer) ([]*QuarantineRecord, error)
	}{
		{"by_id", func(s *syncer) ([]*QuarantineRecord, error) { return s.getWatchRecordByID(30005) }},
		{"by_group", func(s *syncer) ([]*QuarantineRecord, error) { return s.getWatchRecordByGroup("rg_bulk") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			session := newStubRestrictedSession([]chunk.Row{newWatchRow(30005, "rg_bulk", startTime, &endTime)})
			reader := newReader()
			s := &syncer{sysSessionPool: &stubSessionPool{resource: session}, newWatchReader: reader}

			records, err := tc.call(s)
			require.NoError(t, err)
			require.Len(t, records, 1)

			require.True(t, reader.CheckPoint.Equal(initialCheckpoint), "point query must not advance CheckPoint")
			require.True(t, reader.UpperBound.Equal(initialUpperBound), "point query must not rewrite UpperBound")
			require.True(t, reader.lastScanKeyTime.Equal(initialLastScanKey), "point query must not touch lastScanKeyTime")
		})
	}
}
