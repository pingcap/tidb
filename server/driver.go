// Copyright 2015 PingCAP, Inc.
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

package server

import (
	"context"
	"crypto/tls"

<<<<<<< HEAD
	"github.com/pingcap/tidb/types"
=======
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/extension"
	"github.com/pingcap/tidb/sessionctx/variable"
>>>>>>> 5f8002fec7 (session, com_stmt: store and restore the params for cursor fetch (#41441))
	"github.com/pingcap/tidb/util/chunk"
)

// IDriver opens IContext.
type IDriver interface {
	// OpenCtx opens an IContext with connection id, client capability, collation, dbname and optionally the tls state.
	OpenCtx(connID uint64, capability uint32, collation uint8, dbname string, tlsState *tls.ConnectionState) (*TiDBContext, error)
}

// PreparedStatementCtx stores the context generated in `execute` statement for a prepared statement
// subsequent stmt fetching could restore the session variables from this context
type PreparedStatementCtx struct {
	// Params is the params used in `execute` statement
	Params variable.PreparedParams
	// TODO: store and restore variables, but be careful that we'll also need to restore the variables after FETCH
	// a cleaner way to solve this problem is to always reading params from a statement scope (but not session scope)
	// context. But switching in/out related context is simpler on current code base, and the affected radius is more
	// controllable.
}

// PreparedStatement is the interface to use a prepared statement.
type PreparedStatement interface {
	// ID returns statement ID
	ID() int

	// Execute executes the statement.
	Execute(context.Context, []types.Datum) (ResultSet, error)

	// AppendParam appends parameter to the statement.
	AppendParam(paramID int, data []byte) error

	// NumParams returns number of parameters.
	NumParams() int

	// BoundParams returns bound parameters.
	BoundParams() [][]byte

	// SetParamsType sets type for parameters.
	SetParamsType([]byte)

	// GetParamsType returns the type for parameters.
	GetParamsType() []byte

	// StoreResultSet stores ResultSet for subsequent stmt fetching
	StoreResultSet(rs ResultSet)

	// GetResultSet gets ResultSet associated this statement
	GetResultSet() ResultSet

	// StorePreparedCtx stores context in `execute` statement for subsequent stmt fetching
	StorePreparedCtx(ctx *PreparedStatementCtx)

	// GetPreparedParams gets the prepared params associated this statement
	GetPreparedCtx() *PreparedStatementCtx

	// Reset removes all bound parameters.
	Reset()

	// Close closes the statement.
	Close() error
}

// ResultSet is the result set of an query.
type ResultSet interface {
	Columns() []*ColumnInfo
	NewChunk() *chunk.Chunk
	Next(context.Context, *chunk.Chunk) error
	StoreFetchedRows(rows []chunk.Row)
	GetFetchedRows() []chunk.Row
	Close() error
}

// fetchNotifier represents notifier will be called in COM_FETCH.
type fetchNotifier interface {
	// OnFetchReturned be called when COM_FETCH returns.
	// it will be used in server-side cursor.
	OnFetchReturned()
}
