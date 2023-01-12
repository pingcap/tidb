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
	"time"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/extension"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/chunk"
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
	Execute(context.Context, []expression.Expression) (ResultSet, error)

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

	// Reset removes all bound parameters.
	Reset()

	// Close closes the statement.
	Close() error
}

// ResultSet is the result set of an query.
type ResultSet interface {
	Columns() []*ColumnInfo
	NewChunk(chunk.Allocator) *chunk.Chunk
	Next(context.Context, *chunk.Chunk) error
	StoreFetchedRows(rows []chunk.Row)
	GetFetchedRows() []chunk.Row
	Close() error
	// IsClosed checks whether the result set is closed.
	IsClosed() bool
}

// fetchNotifier represents notifier will be called in COM_FETCH.
type fetchNotifier interface {
	// OnFetchReturned be called when COM_FETCH returns.
	// it will be used in server-side cursor.
	OnFetchReturned()
}

type GetResult struct {
	columns   []*ColumnInfo
	Id        int
	chunks    []*chunk.Chunk
	rows      []chunk.Row
	CloseBool bool
}

func (cache *GetResult) Columns() []*ColumnInfo {
	return cache.columns
}

func (cache *GetResult) Next(ctx context.Context, req *chunk.Chunk) error {
	cache.chunks[cache.Id].CopyChunk(req)
	cache.Id++
	return nil
}

func (cache *GetResult) StoreFetchedRows(rows []chunk.Row) {
	cache.rows = rows
}

func (cache *GetResult) GetFetchedRows() []chunk.Row {
	return cache.rows
}

func (cache *GetResult) Close() error {
	cache.Id = 0
	cache.CloseBool = true
	return nil
}

func (cache *GetResult) IsClosed() bool {
	return cache.CloseBool
}
func (cache *GetResult) NewChunk(chunk.Allocator) *chunk.Chunk {
	chunk := cache.chunks[cache.Id].CopyConstruct()
	chunk.Reset()
	return chunk
}

func newCacheResult(rs ResultSet) *variable.CacheResult {
	cols := rs.Columns()
	tmpcols := make([]*variable.TmpcolumnInfo, 0, len(cols))
	for _, col := range cols {
		tmpcols = append(tmpcols, col.Tranfer())
	}
	ca := &variable.CacheResult{
		Id:             0,
		Columns:        tmpcols,
		Rows:           rs.GetFetchedRows(),
		CloseBool:      false,
		LastUpdateTime: time.Now(),
	}
	return ca
}

func tryNewResultSet(rs ResultSet, stmt ast.StmtNode) *variable.CacheResult {
	switch stmt.(type) {
	case *ast.SelectStmt:
		return newCacheResult(rs)
	default:
		return nil
	}
}

func newGetResult(cs *variable.CacheResult) *GetResult {
	colus := make([]*ColumnInfo, 0, len(cs.Columns))
	for _, column := range cs.Columns {
		colus = append(colus, tmpcolumnInfotransfer(column))
	}
	getRes := &GetResult{
		columns:   colus,
		Id:        0,
		chunks:    cs.Chunks,
		CloseBool: false,
		rows:      cs.Rows,
	}
	return getRes
}
