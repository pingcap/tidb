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

package context

import (
	"sync"

	"github.com/pingcap/errors"
)

// PlanCacheType is the flag of plan cache
type PlanCacheType int

const (
	// DefaultNoCache no cache
	DefaultNoCache PlanCacheType = iota
	// SessionPrepared session prepared plan cache
	SessionPrepared
	// SessionNonPrepared session non-prepared plan cache
	SessionNonPrepared
)

// PlanCacheTracker PlanCacheTracker `PlanCacheTracker`. `PlanCacheTracker` is thread-safe.
type PlanCacheTracker struct {
	mu sync.Mutex

	useCache             bool
	cacheType            PlanCacheType
	planCacheUnqualified string
	forcePlanCache       bool // force the optimizer to use plan cache even if there is risky optimization, see #49736.
	alwaysWarnSkipCache  bool

	warnHandler WarnAppender
}

// WarnSkipPlanCache output the reason why this query can't hit the plan cache.
func (h *PlanCacheTracker) WarnSkipPlanCache(reason string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.cacheType == DefaultNoCache {
		return
	}
	h.warnSkipPlanCache(reason)
}

// SetSkipPlanCache sets to skip plan cache.
func (h *PlanCacheTracker) SetSkipPlanCache(reason string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.useCache {
		return
	}

	if h.forcePlanCache {
		h.warnHandler.AppendWarning(errors.NewNoStackErrorf("force plan-cache: may use risky cached plan: %s", reason))
		return
	}

	h.useCache = false
	h.warnSkipPlanCache(reason)
}

func (h *PlanCacheTracker) warnSkipPlanCache(reason string) {
	h.planCacheUnqualified = reason
	switch h.cacheType {
	case DefaultNoCache:
		h.warnHandler.AppendWarning(errors.NewNoStackError("unknown cache type"))
	case SessionPrepared:
		h.warnHandler.AppendWarning(errors.NewNoStackErrorf("skip prepared plan-cache: %s", reason))
	case SessionNonPrepared:
		if h.alwaysWarnSkipCache {
			// use "plan_cache" rather than types.ExplainFormatPlanCache to avoid import cycle
			h.warnHandler.AppendWarning(errors.NewNoStackErrorf("skip non-prepared plan-cache: %s", reason))
		}
	}
}

// SetAlwaysWarnSkipCache sets whether to always warn when skip plan cache. By default, for `SessionNonPrepared`, we don't warn
// when skip plan cache. But in some cases, we want to warn even for `SessionNonPrepared`.
func (h *PlanCacheTracker) SetAlwaysWarnSkipCache(alwaysWarnSkipCache bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.alwaysWarnSkipCache = alwaysWarnSkipCache
}

// SetCacheType sets the cache type.
func (h *PlanCacheTracker) SetCacheType(cacheType PlanCacheType) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.cacheType = cacheType
}

// SetForcePlanCache sets whether to force the optimizer to use plan cache even if there is risky optimization.
func (h *PlanCacheTracker) SetForcePlanCache(forcePlanCache bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.forcePlanCache = forcePlanCache
}

// EnablePlanCache sets to use plan cache.
func (h *PlanCacheTracker) EnablePlanCache() {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.useCache = true
}

// UseCache returns whether to use plan cache.
func (h *PlanCacheTracker) UseCache() bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.useCache
}

// PlanCacheUnqualified returns the reason of why the plan cache is unqualified
func (h *PlanCacheTracker) PlanCacheUnqualified() string {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.planCacheUnqualified
}

// NewPlanCacheTracker creates a new PlanCacheTracker.
func NewPlanCacheTracker(warnHandler WarnAppender) PlanCacheTracker {
	return PlanCacheTracker{
		warnHandler: warnHandler,
	}
}

// RangeFallbackHandler is used to handle range fallback.
// If there are too many ranges, it'll fallback and add a warning.
// RangeFallbackHandler is thread-safe.
type RangeFallbackHandler struct {
	planCacheTracker *PlanCacheTracker

	warnHandler                WarnAppender
	reportRangeFallbackWarning sync.Once
}

// RecordRangeFallback records the range fallback event.
func (h *RangeFallbackHandler) RecordRangeFallback(rangeMaxSize int64) {
	// If range fallback happens, it means ether the query is unreasonable(for example, several long IN lists) or tidb_opt_range_max_size is too small
	// and the generated plan is probably suboptimal. In that case we don't put it into plan cache.
	h.planCacheTracker.SetSkipPlanCache("in-list is too long")
	h.reportRangeFallbackWarning.Do(func() {
		h.warnHandler.AppendWarning(errors.NewNoStackErrorf("Memory capacity of %v bytes for 'tidb_opt_range_max_size' exceeded when building ranges. Less accurate ranges such as full range are chosen", rangeMaxSize))
	})
}

// NewRangeFallbackHandler creates a new RangeFallbackHandler.
func NewRangeFallbackHandler(planCacheTracker *PlanCacheTracker, warnHandler WarnAppender) RangeFallbackHandler {
	return RangeFallbackHandler{
		planCacheTracker: planCacheTracker,
		warnHandler:      warnHandler,
	}
}
