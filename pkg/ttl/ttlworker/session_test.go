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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/infoschema"
	infoschemactx "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
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
		Name: model.NewCIStr(name),
		Columns: []*model.ColumnInfo{
			{
				ID:        1,
				Name:      model.NewCIStr("time"),
				Offset:    0,
				FieldType: *types.NewFieldType(mysql.TypeDatetime),
				State:     model.StatePublic,
			},
		},
		TTLInfo: &model.TTLInfo{
			ColumnName:       model.NewCIStr("time"),
			IntervalExprStr:  "1",
			IntervalTimeUnit: int(ast.TimeUnitSecond),
			Enable:           true,
			JobInterval:      "1h",
		},
		State: model.StatePublic,
	}

	tbl, err := cache.NewPhysicalTable(model.NewCIStr("test"), tblInfo, model.NewCIStr(""))
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
	for i := 0; i < r.NumRows(); i++ {
		rows[i] = r.GetRow(i)
	}
	return rows
}

type mockSessionPool struct {
	t           *testing.T
	se          *mockSession
	lastSession *mockSession
}

func (p *mockSessionPool) Get() (pools.Resource, error) {
	se := *(p.se)
	p.lastSession = &se
	return p.lastSession, nil
}

func (p *mockSessionPool) Put(pools.Resource) {}

func newMockSessionPool(t *testing.T, tbl ...*cache.PhysicalTable) *mockSessionPool {
	return &mockSessionPool{
		se: newMockSession(t, tbl...),
	}
}

type mockSession struct {
	t *testing.T
	sessionctx.Context
	sessionVars        *variable.SessionVars
	sessionInfoSchema  infoschema.InfoSchema
	executeSQL         func(ctx context.Context, sql string, args ...any) ([]chunk.Row, error)
	rows               []chunk.Row
	execErr            error
	resetTimeZoneCalls int
	closed             bool
	commitErr          error
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
	}
}

func (s *mockSession) GetDomainInfoSchema() infoschemactx.MetaOnlyInfoSchema {
	return s.sessionInfoSchema
}

func (s *mockSession) SessionInfoSchema() infoschema.InfoSchema {
	require.False(s.t, s.closed)
	return s.sessionInfoSchema
}

func (s *mockSession) GetSessionVars() *variable.SessionVars {
	require.False(s.t, s.closed)
	return s.sessionVars
}

func (s *mockSession) ExecuteSQL(ctx context.Context, sql string, args ...any) ([]chunk.Row, error) {
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
	require.False(s.t, s.closed)
	if err := fn(); err != nil {
		return err
	}
	return s.commitErr
}

func (s *mockSession) ResetWithGlobalTimeZone(_ context.Context) (err error) {
	require.False(s.t, s.closed)
	s.resetTimeZoneCalls++
	return nil
}

// GlobalTimeZone returns the global timezone
func (s *mockSession) GlobalTimeZone(_ context.Context) (*time.Location, error) {
	return time.Local, nil
}

func (s *mockSession) Close() {
	s.closed = true
}

func (s *mockSession) Now() time.Time {
	tz := s.sessionVars.TimeZone
	if tz != nil {
		tz = time.UTC
	}
	return time.Now().In(tz)
}

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
	require.Equal(t, 1, s.resetTimeZoneCalls)

	s.sessionInfoSchema = newMockInfoSchema()
	rows, shouldRetry, err = tblSe.ExecuteSQLWithCheck(ctx, "select 1")
	require.EqualError(t, err, "table 'test.t1' meta changed, should abort current job: [schema:1146]Table 'test.t1' doesn't exist")
	require.False(t, shouldRetry)
	require.Nil(t, rows)
	require.Equal(t, 2, s.resetTimeZoneCalls)

	s.sessionInfoSchema = newMockInfoSchema(tbl.TableInfo)
	s.execErr = nil
	rows, shouldRetry, err = tblSe.ExecuteSQLWithCheck(ctx, "select 1")
	require.NoError(t, err)
	require.False(t, shouldRetry)
	require.Equal(t, 1, len(rows))
	require.Equal(t, int64(12), rows[0].GetInt64(0))
	require.Equal(t, 3, s.resetTimeZoneCalls)

	s.commitErr = errors.New("mockCommitErr")
	rows, shouldRetry, err = tblSe.ExecuteSQLWithCheck(ctx, "select 1")
	require.EqualError(t, err, "mockCommitErr")
	require.True(t, shouldRetry)
	require.Nil(t, rows)
	require.Equal(t, 4, s.resetTimeZoneCalls)
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
	tbl2.Name = model.NewCIStr("testcc")
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
	tbl2.Columns[0].Name = model.NewCIStr("time2")
	tbl2.TTLInfo.ColumnName = model.NewCIStr("time2")
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
			{ID: 1023, Name: model.NewCIStr("p0")},
		},
	}
	tbl, err = cache.NewPhysicalTable(model.NewCIStr("test"), tp, model.NewCIStr("p0"))
	require.NoError(t, err)
	tbl2 = tp.Clone()
	tbl2.Partition = tp.Partition.Clone()
	tbl2.Partition.Definitions[0].Name = model.NewCIStr("p1")
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
