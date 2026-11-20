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

package chunk

import (
	"io"
	"sync"
)

// readerWithCachePool caches ReaderWithCache objects to reduce allocation
// pressure during high-frequency spilling read operations.
var readerWithCachePool = sync.Pool{
	New: func() interface{} {
		return &ReaderWithCache{}
	},
}

// getPooledReaderWithCache retrieves a ReaderWithCache from the pool and
// initialises it with the given parameters.
func getPooledReaderWithCache(r io.ReaderAt, cache []byte, cacheOff int64) *ReaderWithCache {
	rwc := readerWithCachePool.Get().(*ReaderWithCache)
	rwc.r = r
	rwc.cacheOff = cacheOff
	rwc.cache = cache
	return rwc
}

// putPooledReaderWithCache clears references and returns a ReaderWithCache to
// the pool so it can be reused.
func putPooledReaderWithCache(rwc *ReaderWithCache) {
	rwc.r = nil
	rwc.cache = nil
	readerWithCachePool.Put(rwc)
}
