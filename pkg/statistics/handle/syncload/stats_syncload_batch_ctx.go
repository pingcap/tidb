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

import (
	"github.com/pingcap/tidb/pkg/statistics"
)

type BatchSyncLoadKey struct {
	histID int64
	tid    int64
}
type batchContext struct {
	tables       map[int64]*tableStatsBatch
	fullLoad     map[BatchSyncLoadKey]bool
	statsWrapper map[BatchSyncLoadKey]statsWrapper
	tableInfo    map[int64]*statistics.Table
}

func newBatchContext() *batchContext {
	return &batchContext{
		tables: make(map[int64]*tableStatsBatch, batchSize),
	}
}

func (b *batchContext) SetFullLoad(tid, hists int64, fullLoad bool) {
	key := BatchSyncLoadKey{histID: hists, tid: tid}
	oldvalue, ok := b.fullLoad[key]
	if ok {
		b.fullLoad[key] = oldvalue || fullLoad
	} else {
		b.fullLoad[key] = fullLoad
	}
}

func (b *batchContext) GetFullLoad(tid, hists int64) (fullLoad bool) {
	key := BatchSyncLoadKey{histID: hists, tid: tid}
	v, ok := b.fullLoad[key]
	return v && ok
}

func (b *batchContext) SetStatsWrapper(tid, hists int64, wrapper statsWrapper) {
	key := BatchSyncLoadKey{histID: hists, tid: tid}
	b.statsWrapper[key] = wrapper
}

func (b *batchContext) GetStatsWrapper(tid, hists int64) (statsWrapper, bool) {
	key := BatchSyncLoadKey{histID: hists, tid: tid}
	result, ok := b.statsWrapper[key]
	return result, ok
}

type tableStatsBatch struct {
	indexs  map[int64]*statistics.Index
	columns map[int64]*statistics.Column
}

func newtableStatsBatch() *tableStatsBatch {
	return &tableStatsBatch{
		indexs:  make(map[int64]*statistics.Index),
		columns: make(map[int64]*statistics.Column),
	}
}
