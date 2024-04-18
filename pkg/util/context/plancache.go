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

// PlanCacheTracker PlanCacheTrackerf `PlanCacheTracker`.
type PlanCacheTracker struct {
	UseCache             bool
	CacheType            PlanCacheType
	PlanCacheUnqualified string
	ForcePlanCache       bool // force the optimizer to use plan cache even if there is risky optimization, see #49736.
	alwaysWarnSkipCache  bool

	warnHandler WarnHandler
}

// ForceSetSkipPlanCache forces to skip plan cache.
func (h *PlanCacheTracker) ForceSetSkipPlanCache(err error) {
	if h.CacheType == DefaultNoCache {
		return
	}
	h.setSkipPlanCache(err)
}

// SetSkipPlanCache sets to skip plan cache.
func (h *PlanCacheTracker) SetSkipPlanCache(err error) {
	if !h.UseCache {
		return
	}

	if h.ForcePlanCache {
		h.warnHandler.AppendWarning(errors.NewNoStackErrorf("force plan-cache: may use risky cached plan: %s", err.Error()))
		return
	}

	h.setSkipPlanCache(err)
}

func (h *PlanCacheTracker) setSkipPlanCache(reason error) {
	h.UseCache = false
	h.PlanCacheUnqualified = reason.Error()

	switch h.CacheType {
	case DefaultNoCache:
		h.warnHandler.AppendWarning(errors.NewNoStackError("unknown cache type"))
	case SessionPrepared:
		h.warnHandler.AppendWarning(errors.NewNoStackErrorf("skip prepared plan-cache: %s", reason.Error()))
	case SessionNonPrepared:
		if h.alwaysWarnSkipCache {
			// use "plan_cache" rather than types.ExplainFormatPlanCache to avoid import cycle
			h.warnHandler.AppendWarning(errors.NewNoStackErrorf("skip non-prepared plan-cache: %s", reason.Error()))
		}
	}
}

// SetAlwaysWarnSkipCache sets whether to always warn when skip plan cache. By default, for `SessionNonPrepared`, we don't warn
// when skip plan cache. But in some cases, we want to warn even for `SessionNonPrepared`.
func (h *PlanCacheTracker) SetAlwaysWarnSkipCache(alwaysWarnSkipCache bool) {
	h.alwaysWarnSkipCache = alwaysWarnSkipCache
}

// NewPlanCacheTracker creates a new PlanCacheTracker.
func NewPlanCacheTracker(warnHandler WarnHandler) PlanCacheTracker {
	return PlanCacheTracker{
		warnHandler: warnHandler,
	}
}

// RangeFallbackHandler is used to handle range fallback.
// If there are too many ranges, it'll fallback and add a warning.
type RangeFallbackHandler struct {
	PlanCacheTracker

	warnHandler                WarnHandler
	reportRangeFallbackWarning sync.Once
}

// RecordRangeFallback records the range fallback event.
func (h *RangeFallbackHandler) RecordRangeFallback(rangeMaxSize int64) {
	// If range fallback happens, it means ether the query is unreasonable(for example, several long IN lists) or tidb_opt_range_max_size is too small
	// and the generated plan is probably suboptimal. In that case we don't put it into plan cache.
	if h.UseCache {
		h.SetSkipPlanCache(errors.NewNoStackError("in-list is too long"))
	}
	h.reportRangeFallbackWarning.Do(func() {
		h.warnHandler.AppendWarning(errors.NewNoStackErrorf("Memory capacity of %v bytes for 'tidb_opt_range_max_size' exceeded when building ranges. Less accurate ranges such as full range are chosen", rangeMaxSize))
	})
}

// NewRangeFallbackHandler creates a new RangeFallbackHandler.
func NewRangeFallbackHandler(warnHandler WarnHandler) RangeFallbackHandler {
	return RangeFallbackHandler{
		PlanCacheTracker: NewPlanCacheTracker(warnHandler),
		warnHandler:      warnHandler,
	}
}
