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

package executor

import (
	"github.com/pingcap/tidb/util/chunk"
)

// TODO: Consider using a fix-sized hash table.
type rowHashTable map[uint64][]chunk.RowPtr

func newRowHashTable() rowHashTable {
	t := make(rowHashTable)
	return t
}

func (t rowHashTable) Put(key uint64, rowPtr chunk.RowPtr) {
	t[key] = append(t[key], rowPtr)
}

func (t rowHashTable) Get(key uint64) []chunk.RowPtr {
	return t[key]
}

func (t rowHashTable) Len() int { return len(t) }
