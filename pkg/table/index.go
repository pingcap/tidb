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
	"time"

	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
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

// Index is the interface for index data on KV store.
type Index interface {
	// Meta returns IndexInfo.
	Meta() *model.IndexInfo
	// TableMeta returns TableInfo
	TableMeta() *model.TableInfo
	// Create supports insert into statement.
	Create(ctx MutateContext, txn kv.Transaction, indexedValues []types.Datum, h kv.Handle, handleRestoreData []types.Datum, opts ...CreateIdxOptFunc) (kv.Handle, error)
	// Delete supports delete from statement.
	Delete(ctx MutateContext, txn kv.Transaction, indexedValues []types.Datum, h kv.Handle) error
	// GenIndexKVIter generate index key and value for multi-valued index, use iterator to reduce the memory allocation.
	GenIndexKVIter(ec errctx.Context, loc *time.Location, indexedValue []types.Datum, h kv.Handle, handleRestoreData []types.Datum) IndexKVGenerator
	// Exist supports check index exists or not.
	Exist(ec errctx.Context, loc *time.Location, txn kv.Transaction, indexedValues []types.Datum, h kv.Handle) (bool, kv.Handle, error)
	// GenIndexKey generates an index key. If the index is a multi-valued index, use GenIndexKVIter instead.
	GenIndexKey(ec errctx.Context, loc *time.Location, indexedValues []types.Datum, h kv.Handle, buf []byte) (key []byte, distinct bool, err error)
	// GenIndexValue generates an index value.
	GenIndexValue(ec errctx.Context, loc *time.Location, distinct bool, indexedValues []types.Datum, h kv.Handle, restoredData []types.Datum, buf []byte) ([]byte, error)
	// FetchValues fetched index column values in a row.
	// Param columns is a reused buffer, if it is not nil, FetchValues will fill the index values in it,
	// and return the buffer, if it is nil, FetchValues will allocate the buffer instead.
	FetchValues(row []types.Datum, columns []types.Datum) ([]types.Datum, error)
}

// IndexKVGenerator generates kv for an index.
// It could be also used for generating multi-value indexes.
type IndexKVGenerator struct {
	index             Index
	ec                errctx.Context
	loc               *time.Location
	handle            kv.Handle
	handleRestoreData []types.Datum

	isMultiValue bool
	// Only used by multi-value index.
	allIdxVals [][]types.Datum
	i          int
	// Only used by non multi-value index.
	idxVals []types.Datum
}

// NewMultiValueIndexKVGenerator creates a new IndexKVGenerator for multi-value indexes.
func NewMultiValueIndexKVGenerator(
	index Index,
	ec errctx.Context,
	loc *time.Location,
	handle kv.Handle,
	handleRestoredData []types.Datum,
	mvIndexData [][]types.Datum,
) IndexKVGenerator {
	return IndexKVGenerator{
		index:             index,
		ec:                ec,
		loc:               loc,
		handle:            handle,
		handleRestoreData: handleRestoredData,
		isMultiValue:      true,
		allIdxVals:        mvIndexData,
		i:                 0,
	}
}

// NewPlainIndexKVGenerator creates a new IndexKVGenerator for non multi-value indexes.
func NewPlainIndexKVGenerator(
	index Index,
	ec errctx.Context,
	loc *time.Location,
	handle kv.Handle,
	handleRestoredData []types.Datum,
	idxData []types.Datum,
) IndexKVGenerator {
	return IndexKVGenerator{
		index:             index,
		ec:                ec,
		loc:               loc,
		handle:            handle,
		handleRestoreData: handleRestoredData,
		isMultiValue:      false,
		idxVals:           idxData,
	}
}

// Next returns the next index key and value.
// For non multi-value indexes, there is only one index kv.
func (iter *IndexKVGenerator) Next(keyBuf, valBuf []byte) ([]byte, []byte, bool, error) {
	var val []types.Datum
	if iter.isMultiValue {
		val = iter.allIdxVals[iter.i]
	} else {
		val = iter.idxVals
	}
	key, distinct, err := iter.index.GenIndexKey(iter.ec, iter.loc, val, iter.handle, keyBuf)
	if err != nil {
		return nil, nil, false, err
	}
	idxVal, err := iter.index.GenIndexValue(iter.ec, iter.loc, distinct, val, iter.handle, iter.handleRestoreData, valBuf)
	if err != nil {
		return nil, nil, false, err
	}
	iter.i++
	return key, idxVal, distinct, err
}

// Valid returns true if the generator is not exhausted.
func (iter *IndexKVGenerator) Valid() bool {
	if iter.isMultiValue {
		return iter.i < len(iter.allIdxVals)
	}
	return iter.i == 0
}
