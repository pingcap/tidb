// Copyright 2025 PingCAP, Inc.
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

package executor

import (
	"context"

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/execdetails"
)

// CachedResultExec wraps an existing executor and adds result set caching
// for cached table queries. On Open, it checks the result cache; on hit it
// serves chunks directly from memory. On miss it delegates to the original
// executor and collects the result for cache back-fill on Close.
type CachedResultExec struct {
	exec.BaseExecutor

	original    exec.Executor
	cachedTable table.CachedTable
	cacheKey    table.ResultCacheKey
	paramBytes  []byte

	// cache hit state
	hitCache     bool
	cachedChunks []*chunk.Chunk
	chunkIdx     int

	// cache miss state: collect results for back-fill
	collecting      bool
	collectedChunks []*chunk.Chunk
	resultSchema    []*types.FieldType
}

// Open checks the result cache before opening the wrapped executor.
func (e *CachedResultExec) Open(ctx context.Context) error {
	// Reset state in case Open is called multiple times.
	e.hitCache = false
	e.cachedChunks = nil
	e.chunkIdx = 0
	e.collecting = false
	e.collectedChunks = nil
	e.resultSchema = nil

	chunks, fieldTypes, ok := e.cachedTable.GetCachedResult(e.cacheKey, e.paramBytes)
	if ok && schemaMatch(fieldTypes, e.RetFieldTypes()) {
		e.hitCache = true
		e.cachedChunks = chunks
		e.chunkIdx = 0
		metrics.ResultCacheHitCounter.Inc()
		e.Ctx().GetSessionVars().StmtCtx.ReadFromResultCache = true
		// Register runtime stats for EXPLAIN ANALYZE.
		if coll := e.Ctx().GetSessionVars().StmtCtx.RuntimeStatsColl; coll != nil {
			var cachedRows int64
			for _, chk := range chunks {
				cachedRows += int64(chk.NumRows())
			}
			coll.RegisterStats(e.ID(), &execdetails.ResultCacheRuntimeStats{
				HitCache:   true,
				CachedRows: cachedRows,
			})
		}
		return nil
	}

	// Cache miss — open the original executor.
	metrics.ResultCacheMissCounter.Inc()
	if coll := e.Ctx().GetSessionVars().StmtCtx.RuntimeStatsColl; coll != nil {
		coll.RegisterStats(e.ID(), &execdetails.ResultCacheRuntimeStats{HitCache: false})
	}
	if err := e.original.Open(ctx); err != nil {
		return err
	}
	e.collecting = true
	e.resultSchema = e.RetFieldTypes()
	return nil
}

// Next returns the next chunk of results, either from cache or the original executor.
func (e *CachedResultExec) Next(ctx context.Context, req *chunk.Chunk) error {
	if e.hitCache {
		return e.nextFromCache(req)
	}

	err := e.original.Next(ctx, req)
	if err != nil {
		e.collecting = false
		return err
	}

	if e.collecting && req.NumRows() > 0 {
		// Deep copy: the session reuses the chunk memory between Next calls.
		copied := req.CopyConstruct()
		e.collectedChunks = append(e.collectedChunks, copied)
	}

	return nil
}

// Close back-fills the result cache on miss and closes the original executor.
func (e *CachedResultExec) Close() error {
	if e.hitCache {
		// Original executor was never opened.
		return nil
	}

	if err := e.original.Close(); err != nil {
		e.collecting = false
		return err
	}

	// Back-fill the cache only after the wrapped executor closes successfully.
	if e.collecting {
		e.cachedTable.PutCachedResult(e.cacheKey, e.paramBytes, e.collectedChunks, e.resultSchema)
	}

	return nil
}

// nextFromCache serves chunks from the cached result set.
func (e *CachedResultExec) nextFromCache(req *chunk.Chunk) error {
	req.Reset()
	if e.chunkIdx >= len(e.cachedChunks) {
		return nil // EOF
	}
	src := e.cachedChunks[e.chunkIdx]
	e.chunkIdx++
	// Copy into req so we don't hand out shared cache memory to the session.
	req.Append(src, 0, src.NumRows())
	return nil
}

// schemaMatch returns true when the cached field types are compatible with
// the current executor's output schema. This guards against schema changes
// (e.g. DDL altering a column type) that would make a cached result invalid.
func schemaMatch(cached, current []*types.FieldType) bool {
	if len(cached) != len(current) {
		return false
	}
	for i := range cached {
		if cached[i] == nil || current[i] == nil {
			if cached[i] != current[i] {
				return false
			}
			continue
		}
		if !cached[i].Equal(current[i]) {
			return false
		}
	}
	return true
}
