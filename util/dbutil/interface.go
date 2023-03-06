// Copyright 2022 PingCAP, Inc.
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

package dbutil

import (
	"context"
	"database/sql"
)

// check compatibility
var (
	_ DBExecutor = &sql.DB{}
	_ DBExecutor = &sql.Conn{}
)

// QueryExecutor is a interface for execute Query from a database.
//
// in generate the implement should be *sql.DB or *sql.Conn
type QueryExecutor interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

// DBExecutor is a interface for execute read and write statements from a database.
//
// in generate the implement should be *sql.DB or *sql.Conn
type DBExecutor interface {
	QueryExecutor
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}
