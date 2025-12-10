// Copyright 2025 PingCAP, Inc.
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

package testkit

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/types"
)

// TestKitFactory creates a new *TestKit backed by the caller's store.
// Each invocation must return a fresh TestKit instance suitable for exclusive use
// by a single database/sql connection.
type TestKitFactory func() *TestKit

// NewTestKitSQLDB returns a *sql.DB whose operations are executed via TestKit sessions.
// It allows importsdk to run against in-process TiDB components inside integration tests
// without opening a real MySQL connection.
func NewTestKitSQLDB(factory TestKitFactory) *sql.DB {
	connector := &testKitConnector{factory: factory}
	return sql.OpenDB(connector)
}

type testKitConnector struct {
	factory TestKitFactory
}

func (c *testKitConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if c.factory == nil {
		return nil, errors.New("nil testkit factory")
	}
	tk := c.factory()
	if tk == nil {
		return nil, errors.New("testkit factory returned nil")
	}
	return &testKitConn{tk: tk}, nil
}

func (c *testKitConnector) Driver() driver.Driver {
	return testKitDriver{}
}

type testKitDriver struct{}

func (testKitDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("use testKitConnector via sql.OpenDB")
}

type testKitConn struct {
	tk *TestKit
}

var (
	_ driver.Conn              = (*testKitConn)(nil)
	_ driver.ExecerContext     = (*testKitConn)(nil)
	_ driver.QueryerContext    = (*testKitConn)(nil)
	_ driver.ConnBeginTx       = (*testKitConn)(nil)
	_ driver.Pinger            = (*testKitConn)(nil)
	_ driver.SessionResetter   = (*testKitConn)(nil)
	_ driver.NamedValueChecker = (*testKitConn)(nil)
)

func (c *testKitConn) Prepare(string) (driver.Stmt, error) {
	return nil, driver.ErrSkip
}

func (c *testKitConn) Close() error {
	if c.tk != nil {
		c.tk.Session().Close()
		c.tk = nil
	}
	return nil
}

func (c *testKitConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *testKitConn) BeginTx(ctx context.Context, _ driver.TxOptions) (driver.Tx, error) {
	if err := sessiontxn.NewTxn(ctx, c.tk.Session()); err != nil {
		return nil, err
	}
	return &testKitTx{tk: c.tk, ctx: ctx}, nil
}

type testKitTx struct {
	tk  *TestKit
	ctx context.Context
}

func (tx *testKitTx) Commit() error {
	return tx.tk.Session().CommitTxn(tx.ctx)
}

func (tx *testKitTx) Rollback() error {
	tx.tk.Session().RollbackTxn(tx.ctx)
	return nil
}

func (c *testKitConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	rs, err := c.tk.ExecWithContext(ctx, query, namedValueArgs(args)...)
	if err != nil {
		return nil, err
	}
	if rs != nil {
		if err := rs.Close(); err != nil {
			return nil, err
		}
	}
	sc := c.tk.Session().GetSessionVars().StmtCtx
	return testKitResult{
		lastInsertID: int64(sc.LastInsertID),
		rowsAffected: int64(sc.AffectedRows()),
	}, nil
}

func (c *testKitConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	rs, err := c.tk.ExecWithContext(ctx, query, namedValueArgs(args)...)
	if err != nil {
		return nil, err
	}
	if rs == nil {
		return &testKitRows{}, nil
	}
	fields := rs.Fields()
	columns := columnNamesFromFields(fields)
	rows, err := session.GetRows4Test(ctx, c.tk.Session(), rs)
	if err != nil {
		return nil, err
	}
	if err := rs.Close(); err != nil {
		return nil, err
	}
	loc := c.tk.Session().GetSessionVars().Location()
	data := make([][]driver.Value, len(rows))
	for i, row := range rows {
		values := make([]driver.Value, len(fields))
		for j := range fields {
			if row.IsNull(j) {
				values[j] = nil
				continue
			}
			d := row.GetDatum(j, &fields[j].Column.FieldType)
			val, err := datumToDriverValue(d, loc)
			if err != nil {
				return nil, err
			}
			values[j] = val
		}
		data[i] = values
	}
	return &testKitRows{columns: columns, data: data}, nil
}

func (c *testKitConn) Ping(ctx context.Context) error {
	rs, err := c.tk.ExecWithContext(ctx, "SELECT 1")
	if err != nil {
		return err
	}
	if rs != nil {
		return rs.Close()
	}
	return nil
}

func (c *testKitConn) ResetSession(ctx context.Context) error {
	txn, err := c.tk.Session().Txn(false)
	if err != nil {
		return err
	}
	if txn.Valid() {
		c.tk.Session().RollbackTxn(ctx)
	}
	return nil
}

func (c *testKitConn) CheckNamedValue(value *driver.NamedValue) error {
	return nil
}

func namedValueArgs(args []driver.NamedValue) []any {
	if len(args) == 0 {
		return nil
	}
	values := make([]any, len(args))
	for i, nv := range args {
		values[i] = nv.Value
	}
	return values
}

type testKitResult struct {
	lastInsertID int64
	rowsAffected int64
}

func (r testKitResult) LastInsertId() (int64, error) { return r.lastInsertID, nil }
func (r testKitResult) RowsAffected() (int64, error) { return r.rowsAffected, nil }

type testKitRows struct {
	columns []string
	data    [][]driver.Value
	idx     int32
}

func (r *testKitRows) Columns() []string { return r.columns }
func (r *testKitRows) Close() error {
	r.data = nil
	return nil
}

func (r *testKitRows) Next(dest []driver.Value) error {
	i := int(atomic.LoadInt32(&r.idx))
	if i >= len(r.data) {
		return io.EOF
	}
	row := r.data[i]
	copy(dest, row)
	for j := len(row); j < len(dest); j++ {
		dest[j] = nil
	}
	atomic.AddInt32(&r.idx, 1)
	return nil
}

func columnNamesFromFields(fields []*resolve.ResultField) []string {
	columns := make([]string, len(fields))
	for i, f := range fields {
		name := f.ColumnAsName.O
		if name == "" && f.Column != nil {
			name = f.Column.Name.O
		}
		if name == "" {
			name = fmt.Sprintf("col_%d", i)
		}
		columns[i] = name
	}
	return columns
}

func datumToDriverValue(d types.Datum, loc *time.Location) (driver.Value, error) {
	switch d.Kind() {
	case types.KindNull:
		return nil, nil
	case types.KindInt64:
		return d.GetInt64(), nil
	case types.KindUint64:
		return d.GetUint64(), nil
	case types.KindFloat32:
		return d.GetFloat32(), nil
	case types.KindFloat64:
		return d.GetFloat64(), nil
	case types.KindString:
		return d.GetString(), nil
	case types.KindBytes:
		return cloneBytes(d.GetBytes()), nil
	case types.KindBinaryLiteral, types.KindMysqlBit:
		return cloneBytes(d.GetBinaryLiteral()), nil
	case types.KindMysqlDecimal:
		return d.GetMysqlDecimal().String(), nil
	case types.KindMysqlDuration:
		return d.GetMysqlDuration().Duration, nil
	case types.KindMysqlTime:
		if loc == nil {
			loc = time.Local
		}
		goTime, err := d.GetMysqlTime().GoTime(loc)
		if err != nil {
			return nil, err
		}
		return goTime, nil
	case types.KindMysqlEnum:
		return d.GetMysqlEnum().String(), nil
	case types.KindMysqlSet:
		return d.GetMysqlSet().String(), nil
	case types.KindMysqlJSON:
		return d.GetMysqlJSON().String(), nil
	default:
		return d.GetValue(), nil
	}
}

func cloneBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}
