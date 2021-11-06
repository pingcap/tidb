// Copyright 2021 PingCAP, Inc.
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

package tables

import (
	"fmt"
	"sync"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/tikv/client-go/v2/tikv"
)

var _ table.Table = &cachedTable{}
var _ table.CachedTable = &cachedTable{}

type cachedTable struct {
	TableCommon
	kv.MemBuffer
	mu sync.RWMutex
}

func (c *cachedTable) TryGetMemcache(ts uint64) (kv.MemBuffer, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.isReadFromCache(ts) {
		return c.MemBuffer, true
	}
	return nil, false
}
func (c *cachedTable) isReadFromCache(ts uint64) bool {
	// If first read cache table. directly return false, the backend goroutine will help us update the lock information
	// and read the data from the original table at the same time
	// TODO : Use lease and ts judge whether it is readable.
	// TODO : If the cache is not readable. MemBuffer become invalid.
	return c.MemBuffer != nil
}

// NewCachedTable creates a new CachedTable Instance
func NewCachedTable(tbl *TableCommon) (table.Table, error) {
	return &cachedTable{
		TableCommon: *tbl,
	}, nil
}

func (c *cachedTable) loadDataFromOriginalTable(ctx sessionctx.Context) error {
	prefix := tablecodec.GenTablePrefix(c.tableID)
	txn, err := ctx.Txn(true)
	if err != nil {
		return err
	}
	buffTxn, err := ctx.GetStore().BeginWithOption(tikv.DefaultStartTSOption().SetStartTS(0))
	if err != nil {
		return err
	}
	buffer := buffTxn.GetMemBuffer()
	it, err := txn.Iter(prefix, prefix.PrefixNext())
	if err != nil {
		return err
	}
	defer it.Close()
	if !it.Valid() {
		return nil
	}
	for it.Valid() && it.Key().HasPrefix(prefix) {
		value := it.Value()
		err = buffer.Set(it.Key(), value)
		if err != nil {
			return err
		}
		err = it.Next()
		if err != nil {
			return err
		}
	}
	c.mu.Lock()
	c.MemBuffer = buffer
	c.mu.Unlock()
	return nil
}

func (c *cachedTable) UpdateLockForRead(ctx sessionctx.Context, ts uint64) error {
	// Now only the data is re-load here, and the lock information is not updated. any read-lock information update will in the next pr.
	err := c.loadDataFromOriginalTable(ctx)
	if err != nil {
		return fmt.Errorf("reload data error")
	}
	return nil
}
