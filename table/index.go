// Copyright 2016 PingCAP, Inc.
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

package table

import (
	"context"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
)

// IndexIterator is the interface for iterator of index data on KV store.
type IndexIterator interface {
	Next() (k []types.Datum, h kv.Handle, err error)
	Close()
}

// CreateIdxOpt contains the options will be used when creating an index.
type CreateIdxOpt struct {
	Ctx             context.Context
	Untouched       bool // If true, the index key/value is no need to commit.
	IgnoreAssertion bool
}

// CreateIdxOptFunc is defined for the Create() method of Index interface.
// Here is a blog post about how to use this pattern:
// https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis
type CreateIdxOptFunc func(*CreateIdxOpt)

// IndexIsUntouched uses to indicate the index kv is untouched.
var IndexIsUntouched CreateIdxOptFunc = func(opt *CreateIdxOpt) {
	opt.Untouched = true
}

// WithIgnoreAssertion uses to indicate the process can ignore assertion.
var WithIgnoreAssertion = func(opt *CreateIdxOpt) {
	opt.IgnoreAssertion = true
}

// WithCtx returns a CreateIdxFunc.
// This option is used to pass context.Context.
func WithCtx(ctx context.Context) CreateIdxOptFunc {
	return func(opt *CreateIdxOpt) {
		opt.Ctx = ctx
	}
}

// Index is the interface for index data on KV store.
type Index interface {
	// Meta returns IndexInfo.
	Meta() *model.IndexInfo
	// Create supports insert into statement.
	Create(ctx sessionctx.Context, txn kv.Transaction, indexedValues []types.Datum, h kv.Handle, handleRestoreData []types.Datum, opts ...CreateIdxOptFunc) (kv.Handle, error)
	// Delete supports delete from statement.
	Delete(sc *stmtctx.StatementContext, txn kv.Transaction, indexedValues []types.Datum, h kv.Handle) error
	// Exist supports check index exists or not.
	Exist(sc *stmtctx.StatementContext, txn kv.Transaction, indexedValues []types.Datum, h kv.Handle) (bool, kv.Handle, error)
	// GenIndexKey generates an index key.
	GenIndexKey(sc *stmtctx.StatementContext, indexedValues []types.Datum, h kv.Handle, buf []byte) (key []byte, distinct bool, err error)
	// FetchValues fetched index column values in a row.
	// Param columns is a reused buffer, if it is not nil, FetchValues will fill the index values in it,
	// and return the buffer, if it is nil, FetchValues will allocate the buffer instead.
	FetchValues(row []types.Datum, columns []types.Datum) ([]types.Datum, error)
}
