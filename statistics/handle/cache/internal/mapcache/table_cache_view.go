// Copyright 2023 PingCAP, Inc.
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

package mapcache

import (
	"github.com/pingcap/tidb/statistics"
)

type tableCacheView struct {
	tableCache *tableCache
}

func newTableCacheView() *tableCacheView {
	c := newTableCache()
	return &tableCacheView{tableCache: c}
}

func (v *tableCacheView) Clone() *tableCacheView {
	if v == nil {
		panic("cannot clone a nil view")
	}
	v.tableCache.AddRef()
	newV := &tableCacheView{
		tableCache: v.tableCache,
	}
	return newV
}

func (v *tableCacheView) Release() {
	if v == nil {
		panic("cannot release a nil view")
	}
	v.tableCache.DecRef()
}

func (v *tableCacheView) share() bool {
	return v.tableCache.ref.Load() > 1
}

func (v *tableCacheView) Get(k int64) (*statistics.Table, bool) {
	return v.tableCache.Get(k)
}

func (v *tableCacheView) Put(k int64, tbl *statistics.Table) int64 {
	if v.share() {
		defer v.tableCache.DecRef()
		v.tableCache = v.tableCache.Clone()
	}
	return v.tableCache.Put(k, tbl)
}

func (v *tableCacheView) Del(k int64) int64 {
	item, ok := v.tableCache.Item[k]
	if !ok {
		return 0
	}
	if v.share() {
		defer v.tableCache.DecRef()
		v.tableCache = v.tableCache.Clone()
	}
	delete(v.tableCache.Item, k)
	return item.cost
}

func (v *tableCacheView) Iterate(f func(k int64, v *statistics.Table)) {
	v.tableCache.Iterate(f)
}

func (v *tableCacheView) Len() int {
	return v.tableCache.Len()
}

func (v *tableCacheView) FreshMemUsage() int64 {
	return v.tableCache.FreshMemUsage()
}
