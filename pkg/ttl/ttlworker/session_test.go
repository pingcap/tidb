// Copyright 2022 PingCAP, Inc.
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

package ttlworker

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/infoschema"
	infoschemactx "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/session/syssession"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/ttl/cache"
	"github.com/pingcap/tidb/pkg/ttl/session"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

var idAllocator atomic.Int64

func newMockTTLTbl(t *testing.T, name string) *cache.PhysicalTable {
	tblInfo := &model.TableInfo{
		ID:   idAllocator.Add(1),
		Name: ast.NewCIStr(name),
		Columns: []*model.ColumnInfo{
			{
				ID:        1,
				Name:      ast.NewCIStr("time"),
				Offset:    0,
				FieldType: *types.NewFieldType(mysql.TypeDatetime),
				State:     model.StatePublic,
			},
		},
		TTLInfo: &model.TTLInfo{
			ColumnName:       ast.NewCIStr("time"),
			IntervalExprStr:  "1",
			IntervalTimeUnit: int(ast.TimeUnitSecond),
			Enable:           true,
			JobInterval:      "1h",
		},
		State: model.StatePublic,
	}

	tbl, err := cache.NewPhysicalTable(ast.NewCIStr("test"), tblInfo, ast.NewCIStr(""))
	require.NoError(t, err)
	return tbl
}

func newMockInfoSchema(tbl ...*model.TableInfo) infoschema.InfoSchema {
	return infoschema.MockInfoSchema(tbl)
}

func newMockInfoSchemaWithVer(ver int64, tbl ...*model.TableInfo) infoschema.InfoSchema {
	return infoschema.MockInfoSchemaWithSchemaVer(tbl, ver)
}

type mockRows struct {
	t          *testing.T
	fieldTypes []*types.FieldType
	*chunk.Chunk
}

func newMockRows(t *testing.T, fieldTypes ...*types.FieldType) *mockRows {
	return &mockRows{
		t:          t,
		fieldTypes: fieldTypes,
		Chunk:      chunk.NewChunkWithCapacity(fieldTypes, 8),
	}
}

func (r *mockRows) Append(row ...any) *mockRows {
	require.Equal(r.t, len(r.fieldTypes), len(row))
	for i, ft := range r.fieldTypes {
		tp := ft.GetType()
		switch tp {
		case mysql.TypeTimestamp, mysql.TypeDate, mysql.TypeDatetime:
			tm, ok := row[i].(time.Time)
			require.True(r.t, ok)
			r.AppendTime(i, types.NewTime(types.FromGoTime(tm), tp, types.DefaultFsp))
		case mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
			val, ok := row[i].(int)
			require.True(r.t, ok)
			r.AppendInt64(i, int64(val))
		case mysql.TypeString:
			val, ok := row[i].(string)
			require.True(r.t, ok)
			r.AppendString(i, val)
		default:
			require.FailNow(r.t, "unsupported tp %v", tp)
		}
	}
	return r
}

func (r *mockRows) Rows() []chunk.Row {
	rows := make([]chunk.Row, r.NumRows())
	for i := range r.NumRows() {
		rows[i] = r.GetRow(i)
	}
	return rows
}

type mockSessionPool struct {
	syssession.Pool
	t           *testing.T
	se          *mockSession
	lastSession *mockSession
	inuse       atomic.Int64
}

func (p *mockSessionPool) WithSession(fn func(*syssession.Session) error) error {
	se := *(p.se)
	p.lastSession = &se
	p.inuse.Add(1)
	defer p.inuse.Add(-1)
	s, err := syssession.NewSessionForTest(p.lastSession)
	if err != nil {
		return err
	}
	p.lastSession.inPool = false
	defer func() {
		p.lastSession.inPool = true
	}()
	return fn(s)
}

func (p *mockSessionPool) AssertNoSessionInUse() {
	require.Equal(p.t, int64(0), p.inuse.Load())
}

func (p *mockSessionPool) Close() {}

func newMockSessionPool(t *testing.T, tbl ...*cache.PhysicalTable) *mockSessionPool {
	return &mockSessionPool{
		t:  t,
		se: newMockSession(t, tbl...),
	}
}

type mockSession struct {
	t *testing.T
	sessionctx.Context
	sessionVars       *variable.SessionVars
	globalTimeZone    *time.Location
	sessionInfoSchema infoschema.InfoSchema
	executeSQL        func(ctx context.Context, sql string, args ...any) ([]chunk.Row, error)
	rows              []chunk.Row
	execErr           error
	inPool            bool
	closed            bool
	commitErr         error
	killed            chan struct{}
}

type failAfterExecuteSession struct {
	session.Session
	failSQL  string
	failAt   int
	seen     int
	avoided  bool
	executed []string
}

func (s *failAfterExecuteSession) ExecuteSQL(ctx context.Context, sql string, args ...any) ([]chunk.Row, error) {
	rows, err := s.Session.ExecuteSQL(ctx, sql, args...)
	s.executed = append(s.executed, sql)
	if err != nil || !strings.EqualFold(sql, s.failSQL) {
		return rows, err
	}
	s.seen++
	if s.seen == s.failAt {
		return nil, errors.New("injected session error")
	}
	return rows, nil
}

func (s *failAfterExecuteSession) AvoidReuse() {
	s.avoided = true
}

type prepareSessionMock struct {
	*mockSession
	timeZone             string
	isolationReadEngines string
	avoided              bool
}

func newPrepareSessionMock(t *testing.T, timeZone string) *prepareSessionMock {
	s := &prepareSessionMock{
		mockSession:          newMockSession(t),
		timeZone:             timeZone,
		isolationReadEngines: "tikv",
	}
	s.sessionVars.RetryLimit = 7
	s.sessionVars.Enable1PC = false
	s.sessionVars.EnableAsyncCommit = false
	s.setTimeZone(timeZone)
	s.setIsolationReadEngines("tikv")
	return s
}

func (s *prepareSessionMock) setTimeZone(timeZone string) {
	s.timeZone = timeZone
	switch timeZone {
	case "UTC":
		s.sessionVars.TimeZone = time.UTC
	case "SYSTEM":
		s.sessionVars.TimeZone = time.Local
	case "+08:00":
		s.sessionVars.TimeZone = time.FixedZone("+08:00", 8*60*60)
	default:
		loc, err := time.LoadLocation(timeZone)
		require.NoError(s.t, err)
		s.sessionVars.TimeZone = loc
	}
}

func (s *prepareSessionMock) setIsolationReadEngines(value string) {
	s.isolationReadEngines = value
	s.sessionVars.IsolationReadEngines = make(map[kv.StoreType]struct{})
	for _, engine := range strings.Split(value, ",") {
		switch strings.TrimSpace(engine) {
		case "tidb":
			s.sessionVars.IsolationReadEngines[kv.TiDB] = struct{}{}
		case "tikv":
			s.sessionVars.IsolationReadEngines[kv.TiKV] = struct{}{}
		case "tiflash":
			s.sessionVars.IsolationReadEngines[kv.TiFlash] = struct{}{}
		}
	}
}

func (s *prepareSessionMock) ExecuteSQL(_ context.Context, sql string, args ...any) ([]chunk.Row, error) {
	lowerSQL := strings.ToLower(sql)
	switch lowerSQL {
	case "select @@time_zone":
		return newMockRows(s.t, types.NewFieldType(mysql.TypeString)).Append(s.timeZone).Rows(), nil
	case "select @@tidb_isolation_read_engines":
		return newMockRows(s.t, types.NewFieldType(mysql.TypeString)).Append(s.isolationReadEngines).Rows(), nil
	case "set tidb_enable_1pc=on":
		s.sessionVars.Enable1PC = true
	case "set tidb_enable_1pc=off":
		s.sessionVars.Enable1PC = false
	case "set tidb_enable_async_commit=on":
		s.sessionVars.EnableAsyncCommit = true
	case "set tidb_enable_async_commit=off":
		s.sessionVars.EnableAsyncCommit = false
	case "set @@time_zone='utc'":
		s.setTimeZone("UTC")
	case "set @@time_zone=%?":
		s.setTimeZone(args[0].(string))
	case "set tidb_isolation_read_engines='tikv,tiflash,tidb'":
		s.setIsolationReadEngines("tikv,tiflash,tidb")
	case "set tidb_isolation_read_engines=%?":
		s.setIsolationReadEngines(args[0].(string))
	case "rollback":
		return nil, nil
	default:
		const retryPrefix = "set tidb_retry_limit="
		if !strings.HasPrefix(lowerSQL, retryPrefix) {
			return nil, errors.New("unexpected SQL: " + sql)
		}
		value, err := strconv.ParseInt(strings.TrimPrefix(lowerSQL, retryPrefix), 10, 64)
		if err != nil {
			return nil, err
		}
		s.sessionVars.RetryLimit = value
	}
	return nil, nil
}

func (s *prepareSessionMock) AvoidReuse() {
	s.avoided = true
}

func newMockSession(t *testing.T, tbl ...*cache.PhysicalTable) *mockSession {
	tbls := make([]*model.TableInfo, len(tbl))
	for i, ttlTbl := range tbl {
		tbls[i] = ttlTbl.TableInfo
	}
	sessVars := variable.NewSessionVars(nil)
	sessVars.TimeZone = time.UTC
	return &mockSession{
		t:                 t,
		sessionInfoSchema: newMockInfoSchema(tbls...),
		sessionVars:       sessVars,
		globalTimeZone:    time.UTC,
		killed:            make(chan struct{}),
	}
}

func (s *mockSession) GetStore() kv.Storage {
	return nil
}

func (s *mockSession) GetLatestInfoSchema() infoschemactx.MetaOnlyInfoSchema {
	return s.sessionInfoSchema
}

func (s *mockSession) GetLatestISWithoutSessExt() infoschemactx.MetaOnlyInfoSchema {
	return s.GetLatestInfoSchema()
}

func (s *mockSession) SessionInfoSchema() infoschemactx.MetaOnlyInfoSchema {
	require.False(s.t, s.inPool)
	require.False(s.t, s.closed)
	return s.sessionInfoSchema
}

func (s *mockSession) GetSessionVars() *variable.SessionVars {
	require.False(s.t, s.inPool)
	require.False(s.t, s.closed)
	return s.sessionVars
}

func (s *mockSession) ExecuteSQL(ctx context.Context, sql string, args ...any) ([]chunk.Row, error) {
	require.False(s.t, s.inPool)
	require.False(s.t, s.closed)
	if strings.HasPrefix(strings.ToUpper(sql), "SELECT FROM_UNIXTIME") {
		panic("not supported")
	}

	if strings.ToUpper(sql) == "SELECT @@TIME_ZONE" {
		panic("not supported")
	}

	if strings.HasPrefix(strings.ToUpper(sql), "SET ") {
		return nil, nil
	}

	if s.executeSQL != nil {
		return s.executeSQL(ctx, sql, args...)
	}
	return s.rows, s.execErr
}

func (s *mockSession) RunInTxn(_ context.Context, fn func() error, _ session.TxnMode) error {
	require.False(s.t, s.inPool)
	require.False(s.t, s.closed)
	if err := fn(); err != nil {
		return err
	}
	return s.commitErr
}

// GlobalTimeZone returns the global timezone
func (s *mockSession) GlobalTimeZone(_ context.Context) (*time.Location, error) {
	return s.globalTimeZone, nil
}

// KillStmt kills the current statement execution
func (s *mockSession) KillStmt() {
	close(s.killed)
}

func (s *mockSession) Close() {
	require.False(s.t, s.closed)
	s.closed = true
}

func (s *mockSession) Now() time.Time {
	tz := s.sessionVars.TimeZone
	if tz != nil {
		tz = time.UTC
	}
	return time.Now().In(tz)
}

func (s *mockSession) AvoidReuse() {}

func TestExecuteSQLWithCheck(t *testing.T) {
	ctx := context.TODO()
	tbl := newMockTTLTbl(t, "t1")
	s := newMockSession(t, tbl)
	s.execErr = errors.New("mockErr")
	s.rows = newMockRows(t, types.NewFieldType(mysql.TypeInt24)).Append(12).Rows()
	tblSe := newTableSession(s, tbl, time.UnixMilli(0).In(time.UTC))

	rows, shouldRetry, err := tblSe.ExecuteSQLWithCheck(ctx, "select 1")
	require.EqualError(t, err, "mockErr")
	require.True(t, shouldRetry)
	require.Nil(t, rows)

	s.sessionInfoSchema = newMockInfoSchema()
	rows, shouldRetry, err = tblSe.ExecuteSQLWithCheck(ctx, "select 1")
	require.EqualError(t, err, "table 'test.t1' meta changed, should abort current job: [schema:1146]Table 'test.t1' doesn't exist")
	require.False(t, shouldRetry)
	require.Nil(t, rows)

	s.sessionInfoSchema = newMockInfoSchema(tbl.TableInfo)
	s.execErr = nil
	rows, shouldRetry, err = tblSe.ExecuteSQLWithCheck(ctx, "select 1")
	require.NoError(t, err)
	require.False(t, shouldRetry)
	require.Equal(t, 1, len(rows))
	require.Equal(t, int64(12), rows[0].GetInt64(0))

	s.commitErr = errors.New("mockCommitErr")
	rows, shouldRetry, err = tblSe.ExecuteSQLWithCheck(ctx, "select 1")
	require.EqualError(t, err, "mockCommitErr")
	require.True(t, shouldRetry)
	require.Nil(t, rows)
}

func TestPrepareSessionUsesUTCAndRestoresState(t *testing.T) {
	for _, timeZone := range []string{"SYSTEM", "+08:00", "Asia/Shanghai"} {
		t.Run(timeZone, func(t *testing.T) {
			se := newPrepareSessionMock(t, timeZone)

			restore, err := prepareSession(se)
			require.NoError(t, err)
			require.Equal(t, "UTC", se.timeZone)
			require.Equal(t, int64(0), se.sessionVars.RetryLimit)
			require.True(t, se.sessionVars.Enable1PC)
			require.True(t, se.sessionVars.EnableAsyncCommit)
			require.Contains(t, se.GetSessionVars().IsolationReadEngines, kv.TiDB)
			require.Contains(t, se.GetSessionVars().IsolationReadEngines, kv.TiKV)
			require.Contains(t, se.GetSessionVars().IsolationReadEngines, kv.TiFlash)

			require.NoError(t, restore())
			require.Equal(t, timeZone, se.timeZone)
			require.Equal(t, int64(7), se.sessionVars.RetryLimit)
			require.False(t, se.sessionVars.Enable1PC)
			require.False(t, se.sessionVars.EnableAsyncCommit)
			require.Len(t, se.GetSessionVars().IsolationReadEngines, 1)
			require.Contains(t, se.GetSessionVars().IsolationReadEngines, kv.TiKV)
			require.False(t, se.avoided)
		})
	}
}

func TestPrepareSessionFailureCannotPollutePool(t *testing.T) {
	setupSQLs := []string{
		"set tidb_retry_limit=0",
		"set tidb_enable_1pc=ON",
		"set tidb_enable_async_commit=ON",
		"ROLLBACK",
		"select @@time_zone",
		"set @@time_zone='UTC'",
		"select @@tidb_isolation_read_engines",
		"set tidb_isolation_read_engines='tikv,tiflash,tidb'",
	}
	for _, failSQL := range setupSQLs {
		t.Run(failSQL, func(t *testing.T) {
			base := newPrepareSessionMock(t, "Asia/Shanghai")
			se := &failAfterExecuteSession{Session: base, failSQL: failSQL, failAt: 1}

			restore, err := prepareSession(se)
			require.Nil(t, restore)
			require.ErrorContains(t, err, "injected session error")
			require.True(t, se.avoided)
			// The failing statement is applied before its injected error. Cleanup
			// still restores every variable whose setup may have taken effect.
			require.Equal(t, "Asia/Shanghai", base.timeZone)
			require.Equal(t, int64(7), base.sessionVars.RetryLimit)
			require.False(t, base.sessionVars.Enable1PC)
			require.False(t, base.sessionVars.EnableAsyncCommit)
			require.Len(t, base.GetSessionVars().IsolationReadEngines, 1)
			require.Contains(t, base.GetSessionVars().IsolationReadEngines, kv.TiKV)
		})
	}
}

func TestPrepareSessionRestoreFailureContinuesCleanup(t *testing.T) {
	restoreSQLs := []string{
		"set tidb_retry_limit=7",
		"set tidb_enable_1pc=OFF",
		"set tidb_enable_async_commit=OFF",
		"set @@time_zone=%?",
		"set tidb_isolation_read_engines=%?",
	}
	for _, failSQL := range restoreSQLs {
		t.Run(failSQL, func(t *testing.T) {
			base := newPrepareSessionMock(t, "Asia/Shanghai")
			se := &failAfterExecuteSession{Session: base}
			restore, err := prepareSession(se)
			require.NoError(t, err)
			se.failSQL = failSQL
			se.failAt = 1
			err = restore()
			require.Error(t, err)
			require.True(t, se.avoided)
			// Restoration never returns early: all five restore statements run.
			for _, sql := range restoreSQLs {
				require.Contains(t, se.executed, sql)
			}
			require.Equal(t, "Asia/Shanghai", base.timeZone)
			require.Equal(t, int64(7), base.sessionVars.RetryLimit)
			require.False(t, base.sessionVars.Enable1PC)
			require.False(t, base.sessionVars.EnableAsyncCommit)
			require.Len(t, base.GetSessionVars().IsolationReadEngines, 1)
			require.Contains(t, base.GetSessionVars().IsolationReadEngines, kv.TiKV)
		})
	}
}

func TestNewScanSessionRestoresStateAndDiscardsPartialSetup(t *testing.T) {
	for _, original := range []bool{false, true} {
		t.Run(fmt.Sprintf("restore internal scan flag %t", original), func(t *testing.T) {
			se := newMockSession(t)
			se.sessionVars.InternalSQLScanUserTable = original
			_, restore, err := NewScanSession(context.Background(), se, nil, time.Time{})
			require.NoError(t, err)
			require.True(t, se.sessionVars.InternalSQLScanUserTable)
			require.NoError(t, restore())
			require.Equal(t, original, se.sessionVars.InternalSQLScanUserTable)
		})
	}

	for _, failSQL := range []string{
		"set @@tidb_distsql_scan_concurrency=1",
		"set @@tidb_enable_paging=OFF",
	} {
		t.Run("setup failure "+failSQL, func(t *testing.T) {
			se := &failAfterExecuteSession{
				Session: newMockSession(t),
				failSQL: failSQL,
				failAt:  1,
			}
			_, restore, err := NewScanSession(context.Background(), se, nil, time.Time{})
			require.Nil(t, restore)
			require.ErrorContains(t, err, "injected session error")
			require.True(t, se.avoided)
		})
	}

	t.Run("restore failure continues cleanup", func(t *testing.T) {
		se := &failAfterExecuteSession{
			Session: newMockSession(t),
			failSQL: "set @@tidb_distsql_scan_concurrency=%?",
			failAt:  1,
		}
		_, restore, err := NewScanSession(context.Background(), se, nil, time.Time{})
		require.NoError(t, err)
		require.ErrorContains(t, restore(), "injected session error")
		require.True(t, se.avoided)
		require.Contains(t, se.executed, "set @@tidb_enable_paging=%?")
	})
}

func TestValidateTTLWork(t *testing.T) {
	ctx := context.TODO()
	tbl := newMockTTLTbl(t, "t1")
	expire := time.UnixMilli(0).In(time.UTC)

	s := newMockSession(t, tbl)
	s.execErr = errors.New("mockErr")
	ctx = cache.SetMockExpireTime(ctx, time.UnixMilli(0).In(time.UTC))

	// test table dropped
	s.sessionInfoSchema = newMockInfoSchema()
	err := validateTTLWork(ctx, s, tbl, expire)
	require.EqualError(t, err, "[schema:1146]Table 'test.t1' doesn't exist")

	// test TTL option removed
	tbl2 := tbl.TableInfo.Clone()
	tbl2.TTLInfo = nil
	s.sessionInfoSchema = newMockInfoSchema(tbl2)
	err = validateTTLWork(ctx, s, tbl, expire)
	require.EqualError(t, err, "table 'test.t1' is not a ttl table")

	// test table state not public
	tbl2 = tbl.TableInfo.Clone()
	tbl2.State = model.StateDeleteOnly
	s.sessionInfoSchema = newMockInfoSchema(tbl2)
	err = validateTTLWork(ctx, s, tbl, expire)
	require.EqualError(t, err, "table 'test.t1' is not a public table")

	// test table name changed
	tbl2 = tbl.TableInfo.Clone()
	tbl2.Name = ast.NewCIStr("testcc")
	s.sessionInfoSchema = newMockInfoSchema(tbl2)
	err = validateTTLWork(ctx, s, tbl, expire)
	require.EqualError(t, err, "[schema:1146]Table 'test.t1' doesn't exist")

	// test table id changed
	tbl2 = tbl.TableInfo.Clone()
	tbl2.ID = 123
	s.sessionInfoSchema = newMockInfoSchema(tbl2)
	err = validateTTLWork(ctx, s, tbl, expire)
	require.EqualError(t, err, "table id changed")

	// test time column name changed
	tbl2 = tbl.TableInfo.Clone()
	tbl2.Columns[0] = tbl2.Columns[0].Clone()
	tbl2.Columns[0].Name = ast.NewCIStr("time2")
	tbl2.TTLInfo.ColumnName = ast.NewCIStr("time2")
	s.sessionInfoSchema = newMockInfoSchema(tbl2)
	err = validateTTLWork(ctx, s, tbl, expire)
	require.EqualError(t, err, "time column name changed")

	// test interval changed and expire time before previous
	tbl2 = tbl.TableInfo.Clone()
	tbl2.TTLInfo.IntervalExprStr = "10"
	s.sessionInfoSchema = newMockInfoSchema(tbl2)
	ctx = cache.SetMockExpireTime(ctx, time.UnixMilli(-1))
	err = validateTTLWork(ctx, s, tbl, expire)
	require.EqualError(t, err, "expire interval changed")

	tbl2 = tbl.TableInfo.Clone()
	tbl2.TTLInfo.IntervalTimeUnit = int(ast.TimeUnitDay)
	ctx = cache.SetMockExpireTime(ctx, time.UnixMilli(-1))
	s.sessionInfoSchema = newMockInfoSchema(tbl2)
	err = validateTTLWork(ctx, s, tbl, expire)
	require.EqualError(t, err, "expire interval changed")

	// test for safe meta change
	tbl2 = tbl.TableInfo.Clone()
	tbl2.Columns[0] = tbl2.Columns[0].Clone()
	tbl2.Columns[0].ID += 10
	tbl2.Columns[0].FieldType = *types.NewFieldType(mysql.TypeDate)
	tbl2.TTLInfo.IntervalExprStr = "100"
	ctx = cache.SetMockExpireTime(ctx, time.UnixMilli(1000))
	s.sessionInfoSchema = newMockInfoSchema(tbl2)
	err = validateTTLWork(ctx, s, tbl, expire)
	require.NoError(t, err)

	// test table partition name changed
	tp := tbl.TableInfo.Clone()
	tp.Partition = &model.PartitionInfo{
		Definitions: []model.PartitionDefinition{
			{ID: 1023, Name: ast.NewCIStr("p0")},
		},
	}
	tbl, err = cache.NewPhysicalTable(ast.NewCIStr("test"), tp, ast.NewCIStr("p0"))
	require.NoError(t, err)
	tbl2 = tp.Clone()
	tbl2.Partition = tp.Partition.Clone()
	tbl2.Partition.Definitions[0].Name = ast.NewCIStr("p1")
	s.sessionInfoSchema = newMockInfoSchema(tbl2)
	err = validateTTLWork(ctx, s, tbl, expire)
	require.EqualError(t, err, "partition 'p0' is not found in ttl table 'test.t1'")

	// test table partition id changed
	tbl2 = tp.Clone()
	tbl2.Partition = tp.Partition.Clone()
	tbl2.Partition.Definitions[0].ID += 100
	s.sessionInfoSchema = newMockInfoSchema(tbl2)
	err = validateTTLWork(ctx, s, tbl, expire)
	require.EqualError(t, err, "physical id changed")
}
