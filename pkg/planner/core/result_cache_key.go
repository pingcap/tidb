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

package core

import (
	"hash/fnv"

	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
)

// BuildResultCacheKey constructs a ResultCacheKey from the current session
// context. It uses the plan digest to identify the query shape and hashes
// prepared-statement parameter values (or non-prep plan cache literal values)
// so that the same query template with different arguments maps to different
// cache entries.
//
// Returns false when no plan digest is available (e.g. the plan has not been
// finalized yet).
func BuildResultCacheKey(sctx sessionctx.Context) (tables.ResultCacheKey, bool) {
	stmtCtx := sctx.GetSessionVars().StmtCtx

	// Obtain the plan digest. It is set after optimization.
	_, planDigest := stmtCtx.GetPlanDigest()
	if planDigest == nil {
		return tables.ResultCacheKey{}, false
	}

	var key tables.ResultCacheKey
	digestBytes := planDigest.Bytes()
	copy(key.PlanDigest[:], digestBytes)

	// Hash parameter values when present. For prepared statements these are the
	// EXECUTE parameters; for non-prepared plan cache they are the extracted
	// literal values.
	if params := sctx.GetSessionVars().PlanCacheParams.AllParamValues(); len(params) > 0 {
		key.ParamHash = hashParams(params)
	}

	return key, true
}

// HashParamsForTest is exported for testing only.
var HashParamsForTest = hashParams

// hashParams computes a 64-bit FNV-1a hash over a slice of Datum values.
// It uses codec.EncodeKey to produce a stable, type-aware byte representation
// of each parameter before feeding it to the hasher.
func hashParams(params []types.Datum) uint64 {
	h := fnv.New64a()
	for _, p := range params {
		b, err := codec.EncodeKey(nil, nil, p)
		if err != nil {
			// Encoding should not fail for normal parameter types.
			// On error, write a sentinel so different params still differ.
			h.Write([]byte{0xFF})
			continue
		}
		h.Write(b)
	}
	return h.Sum64()
}
