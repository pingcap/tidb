// Copyright 2024 PingCAP, Inc.
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

package syncload

import "github.com/pingcap/tidb/pkg/statistics"

type batchContext struct {
	tables map[int]*tableStatsBatch
}

func newBatchContext() *batchContext {
	return &batchContext{
		tables: make(map[int]*tableStatsBatch, batchSize),
	}
}

type tableStatsBatch struct {
	indexs  map[int]*statistics.Index
	columns map[int]*statistics.Column
}

func newtableStatsBatch() *tableStatsBatch {
	return &tableStatsBatch{
		indexs:  make(map[int]*statistics.Index),
		columns: make(map[int]*statistics.Column),
	}
}
