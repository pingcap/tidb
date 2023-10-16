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

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/server/internal/resultset"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// IDriver opens IContext.
type IDriver interface {
	// OpenCtx opens an IContext with connection id, client capability, collation, dbname and optionally the tls state.
	OpenCtx(connID uint64, capability uint32, collation uint8, dbname string, tlsState *tls.ConnectionState, extensions *extension.SessionExtensions) (*TiDBContext, error)
}

// PreparedStatement is the interface to use a prepared statement.
type PreparedStatement interface {
	// ID returns statement ID
	ID() int

	// Execute executes the statement.
	Execute(context.Context, []expression.Expression) (resultset.ResultSet, error)

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
	StoreResultSet(rs resultset.CursorResultSet)

	// GetResultSet gets ResultSet associated this statement
	GetResultSet() resultset.CursorResultSet

	// Reset removes all bound parameters and opened resultSet/rowContainer.
	Reset() error

	// Close closes the statement.
	Close() error

	// GetCursorActive returns whether the statement has active cursor
	GetCursorActive() bool

	// SetCursorActive sets whether the statement has active cursor
	SetCursorActive(active bool)

	// StoreRowContainer stores a row container into the prepared statement. The `rowContainer` is used to be closed at
	// appropriate time. It's actually not used to read, because an iterator of it has been stored in the result set.
	StoreRowContainer(container *chunk.RowContainer)

	// GetRowContainer returns the row container of the statement
	GetRowContainer() *chunk.RowContainer
}
