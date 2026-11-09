// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

package stmtsummary

// Keep these names in sync with the legacy statement-summary reader.
const (
	PlanCacheMissUnknownCountStr                  = "PLAN_CACHE_MISS_UNKNOWN_COUNT"
	PlanCacheMissCacheDisabledCountStr            = "PLAN_CACHE_MISS_CACHE_DISABLED_COUNT"
	PlanCacheMissUnsupportedStatementTypeCountStr = "PLAN_CACHE_MISS_UNSUPPORTED_STATEMENT_TYPE_COUNT"
	PlanCacheMissNotCacheableCountStr             = "PLAN_CACHE_MISS_NOT_CACHEABLE_COUNT"
	PlanCacheMissParameterTypeMismatchCountStr    = "PLAN_CACHE_MISS_PARAMETER_TYPE_MISMATCH_COUNT"
	PlanCacheMissLookupMissCountStr               = "PLAN_CACHE_MISS_LOOKUP_MISS_COUNT"
	PlanCacheMissReuseRejectedCountStr            = "PLAN_CACHE_MISS_REUSE_REJECTED_COUNT"
	PlanCacheMissCacheInsertFailedCountStr        = "PLAN_CACHE_MISS_CACHE_INSERT_FAILED_COUNT"
)
