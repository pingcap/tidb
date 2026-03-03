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
	"fmt"
	"io"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/session/sessionapi"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

var (
	tkMapMu sync.RWMutex
	tkMap   = make(map[string]*TestKit)
	tkIDSeq int64
)

func init() {
	sql.Register("testkit", &testKitDriver{})
}

// CreateMockDB creates a *sql.DB that uses the TestKit's store to create sessions.
func CreateMockDB(tk *TestKit) *sql.DB {
	id := strconv.FormatInt(atomic.AddInt64(&tkIDSeq, 1), 10)
	tkMapMu.Lock()
	tkMap[id] = tk
	tkMapMu.Unlock()

	db, err := sql.Open("testkit", id)
	if err != nil {
		panic(err)
	}
	return db
}

type testKitDriver struct{}

func (d *testKitDriver) Open(name string) (driver.Conn, error) {
	tkMapMu.RLock()
	tk, ok := tkMap[name]
	tkMapMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("testkit not found for %s", name)
	}
	se := NewSession(tk.t, tk.store)
	return &testKitConn{se: se}, nil
}

type testKitConn struct {
	se sessionapi.Session
}

func (c *testKitConn) Prepare(query string) (driver.Stmt, error) {
	return &testKitStmt{c: c, query: query}, nil
}

func (c *testKitConn) Close() error {
	c.se.Close()
	return nil
}

func (c *testKitConn) Begin() (driver.Tx, error) {
	_, err := c.Exec("BEGIN", nil)
	if err != nil {
		return nil, err
	}
	return &testKitTxn{c: c}, nil
}

func (c *testKitConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	return nil, driver.ErrSkip
}

func (c *testKitConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	qArgs := make([]any, len(args))
	for i, a := range args {
		qArgs[i] = a.Value
	}

	rs, err := c.execute(ctx, query, qArgs)
	if err != nil {
		return nil, err
	}
	if rs != nil {
		if err := rs.Close(); err != nil {
			return nil, err
		}
	}
	return &testKitResult{
		lastInsertID: int64(c.se.LastInsertID()),
		rowsAffected: int64(c.se.AffectedRows()),
	}, nil
}

func (c *testKitConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	qArgs := make([]any, len(args))
	for i, a := range args {
		qArgs[i] = a.Value
	}

	rs, err := c.execute(ctx, query, qArgs)
	if err != nil {
		return nil, err
	}
	if rs == nil {
		return nil, nil
	}
	return &testKitRows{rs: rs}, nil
}

func (c *testKitConn) execute(ctx context.Context, sql string, args []any) (sqlexec.RecordSet, error) {
	// Set the command value to ComQuery, so that the process info can be updated correctly
	c.se.SetCommandValue(mysql.ComQuery)
	defer c.se.SetCommandValue(mysql.ComSleep)

	if len(args) == 0 {
		rss, err := c.se.Execute(ctx, sql)
		if err != nil {
			return nil, err
		}
		if len(rss) == 0 {
			return nil, nil
		}
		return rss[0], nil
	}

	stmtID, _, _, err := c.se.PrepareStmt(sql)
	if err != nil {
		return nil, errors.Trace(err)
	}
	params := expression.Args2Expressions4Test(args...)
	rs, err := c.se.ExecutePreparedStmt(ctx, stmtID, params)
	if err != nil {
		return rs, errors.Trace(err)
	}
	err = c.se.DropPreparedStmt(stmtID)
	if err != nil {
		return rs, errors.Trace(err)
	}
	return rs, nil
}

type testKitStmt struct {
	c     *testKitConn
	query string
}

func (s *testKitStmt) Close() error {
	return nil
}

func (s *testKitStmt) NumInput() int {
	return -1
}

func (s *testKitStmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, driver.ErrSkip
}

func (s *testKitStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.c.ExecContext(ctx, s.query, args)
}

func (s *testKitStmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, driver.ErrSkip
}

func (s *testKitStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.c.QueryContext(ctx, s.query, args)
}

type testKitResult struct {
	lastInsertID int64
	rowsAffected int64
}

func (r *testKitResult) LastInsertId() (int64, error) {
	return r.lastInsertID, nil
}

func (r *testKitResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

type testKitRows struct {
	rs    sqlexec.RecordSet
	chunk *chunk.Chunk
	it    *chunk.Iterator4Chunk
}

func (r *testKitRows) Columns() []string {
	fields := r.rs.Fields()
	cols := make([]string, len(fields))
	for i, f := range fields {
		cols[i] = f.Column.Name.O
	}
	return cols
}

func (r *testKitRows) Close() error {
	return r.rs.Close()
}

func (r *testKitRows) Next(dest []driver.Value) error {
	if r.chunk == nil {
		r.chunk = r.rs.NewChunk(nil)
	}

	var row chunk.Row
	if r.it == nil {
		err := r.rs.Next(context.Background(), r.chunk)
		if err != nil {
			return err
		}
		if r.chunk.NumRows() == 0 {
			return io.EOF
		}
		r.it = chunk.NewIterator4Chunk(r.chunk)
		row = r.it.Begin()
	} else {
		row = r.it.Next()
		if row.IsEmpty() {
			err := r.rs.Next(context.Background(), r.chunk)
			if err != nil {
				return err
			}
			if r.chunk.NumRows() == 0 {
				return io.EOF
			}
			r.it = chunk.NewIterator4Chunk(r.chunk)
			row = r.it.Begin()
		}
	}

	for i := range row.Len() {
		d := row.GetDatum(i, &r.rs.Fields()[i].Column.FieldType)
		// Handle NULL
		if d.IsNull() {
			dest[i] = nil
		} else {
			// Convert to appropriate type if needed, or just return string/bytes/int/float
			// driver.Value allows int64, float64, bool, []byte, string, time.Time
			// Datum.GetValue() returns interface{} which might be compatible.
			v := d.GetValue()
			switch x := v.(type) {
			case []byte:
				dest[i] = x
			case string:
				dest[i] = x
			case int64:
				dest[i] = x
			case uint64:
				dest[i] = x
			case float64:
				dest[i] = x
			case float32:
				dest[i] = x
			case types.Time:
				dest[i] = x.String()
			case types.Duration:
				dest[i] = x.String()
			case *types.MyDecimal:
				dest[i] = x.String()
			default:
				dest[i] = x
			}
		}
	}
	return nil
}

type testKitTxn struct {
	c *testKitConn
}

func (t *testKitTxn) Commit() error {
	_, err := t.c.Exec("COMMIT", nil)
	return err
}

func (t *testKitTxn) Rollback() error {
	_, err := t.c.Exec("ROLLBACK", nil)
	return err
}
