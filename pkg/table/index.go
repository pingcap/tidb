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
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/table/briefapi"
	"github.com/pingcap/tidb/pkg/types"
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
	FromBackFill    bool
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

// FromBackfill indicates that the index is created by DDL backfill worker.
// In the backfill-merge process, the index KVs from DML will be redirected to
// the temp index. On the other hand, the index KVs from DDL backfill worker should
// never be redirected to the temp index.
var FromBackfill = func(opt *CreateIdxOpt) {
	opt.FromBackFill = true
}

// WithCtx returns a CreateIdxFunc.
// This option is used to pass context.Context.
func WithCtx(ctx context.Context) CreateIdxOptFunc {
	return func(opt *CreateIdxOpt) {
		opt.Ctx = ctx
	}
}

// Index is the alias of briefapi.Index
type Index = briefapi.Index

// IndexMutator is the interface for index mutating
type IndexMutator interface {
	briefapi.Index
	// Create supports insert into statement.
	Create(ctx MutateContext, txn kv.Transaction, indexedValues []types.Datum, h kv.Handle, handleRestoreData []types.Datum, opts ...CreateIdxOptFunc) (kv.Handle, error)
	// Delete supports delete from statement.
	Delete(ctx MutateContext, txn kv.Transaction, indexedValues []types.Datum, h kv.Handle) error
}
