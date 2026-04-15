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
	reader := &systemTableReader{
		TableName:  getRunawayWatchTableName(),
		KeyCol:     "start_time",
		CheckPoint: checkpoint,
		UpperBound: upperBound,
	}

	sql, params := reader.genSelectStmt()
	require.Equal(t, "select * from mysql.tidb_runaway_watch where start_time >= %? and start_time < %? order by start_time", sql)
	require.Equal(t, []any{checkpoint, upperBound}, params)

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
	reader := &systemTableReader{
		TableName:  getRunawayWatchTableName(),
		KeyCol:     "start_time",
		CheckPoint: initialCheckpoint,
	}
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
	require.Contains(t, session.sql, "where start_time >= %? and start_time < %? order by start_time")
	require.Len(t, session.args, 2)

	checkpointArg, ok := session.args[0].(time.Time)
	require.True(t, ok)
	require.True(t, checkpointArg.Equal(initialCheckpoint))
	upperBoundArg, ok := session.args[1].(time.Time)
	require.True(t, ok)
	require.False(t, upperBoundArg.Before(before))
	require.False(t, upperBoundArg.After(after))
	require.True(t, reader.CheckPoint.Equal(upperBoundArg.Add(-watchSyncOverlap)))
}

func TestWatchHoldCheckpoint(t *testing.T) {
	initialCheckpoint := time.Date(2026, 4, 14, 9, 59, 0, 0, time.UTC)

	t.Run("empty result", func(t *testing.T) {
		session := newStubRestrictedSession(nil)
		reader := &systemTableReader{
			TableName:  getRunawayWatchTableName(),
			KeyCol:     "start_time",
			CheckPoint: initialCheckpoint,
		}
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
		reader := &systemTableReader{
			TableName:  getRunawayWatchTableName(),
			KeyCol:     "start_time",
			CheckPoint: initialCheckpoint,
		}
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
		reader := &systemTableReader{
			TableName:  getRunawayWatchTableName(),
			KeyCol:     "start_time",
			CheckPoint: initialCheckpoint,
		}
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
	reader := &systemTableReader{
		TableName:  getRunawayWatchDoneTableName(),
		KeyCol:     "done_time",
		CheckPoint: initialCheckpoint,
	}
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
	require.Contains(t, session.sql, "where done_time >= %? and done_time < %? order by done_time")
	require.Len(t, session.args, 2)

	checkpointArg, ok := session.args[0].(time.Time)
	require.True(t, ok)
	require.True(t, checkpointArg.Equal(initialCheckpoint))
	upperBoundArg, ok := session.args[1].(time.Time)
	require.True(t, ok)
	require.False(t, upperBoundArg.Before(before))
	require.False(t, upperBoundArg.After(after))
	require.True(t, reader.CheckPoint.Equal(upperBoundArg.Add(-watchSyncOverlap)))
}

func TestWatchDoneHoldCheckpoint(t *testing.T) {
	initialCheckpoint := time.Date(2026, 4, 14, 9, 59, 0, 0, time.UTC)

	t.Run("empty result", func(t *testing.T) {
		session := newStubRestrictedSession(nil)
		reader := &systemTableReader{
			TableName:  getRunawayWatchDoneTableName(),
			KeyCol:     "done_time",
			CheckPoint: initialCheckpoint,
		}
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
		reader := &systemTableReader{
			TableName:  getRunawayWatchDoneTableName(),
			KeyCol:     "done_time",
			CheckPoint: initialCheckpoint,
		}
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
