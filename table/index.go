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
// See the License for the specific language governing permissions and
// limitations under the License.

package table

import (
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/util/types"
)

// IndexIterator is the interface for iterator of index data on KV store.
type IndexIterator interface {
	Next() (k []types.Datum, h int64, err error)
	Close()
}

// Index is the interface for index data on KV store.
type Index interface {
	// Meta returns IndexInfo.
	Meta() *model.IndexInfo
	// Create supports insert into statement.
	Create(rm kv.RetrieverMutator, indexedValues []types.Datum, h int64) (int64, error)
	// Delete supports delete from statement.
	Delete(m kv.Mutator, indexedValues []types.Datum, h int64) error
	// Drop supports drop table, drop index statements.
	Drop(rm kv.RetrieverMutator) error
	// Exist supports check index exists or not.
	Exist(rm kv.RetrieverMutator, indexedValues []types.Datum, h int64) (bool, int64, error)
	// GenIndexKey generates an index key.
	GenIndexKey(indexedValues []types.Datum, h int64) (key []byte, distinct bool, err error)
	// Seek supports where clause.
	Seek(r kv.Retriever, indexedValues []types.Datum) (iter IndexIterator, hit bool, err error)
	// SeekFirst supports aggregate min and ascend order by.
	SeekFirst(r kv.Retriever) (iter IndexIterator, err error)
	// FetchValues fetched index column values in a row.
	FetchValues(row []types.Datum) (columns []types.Datum, err error)
}
