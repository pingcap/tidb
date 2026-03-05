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
	"encoding/binary"
	"hash/fnv"
	"time"

	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
)

// BuildResultCacheKey constructs a ResultCacheKey from the current session
// context. It uses the plan digest to identify the query shape and hashes
// prepared-statement parameter values (or non-prep plan cache literal values)
// so that the same query template with different arguments maps to different
// cache entries.
//
// The returned []byte contains the raw encoded parameter bytes used for
// secondary verification on cache hit (to guard against hash collisions).
//
// Returns false when no plan digest is available (e.g. the plan has not been
// finalized yet).
func BuildResultCacheKey(sctx sessionctx.Context) (table.ResultCacheKey, []byte, bool) {
	stmtCtx := sctx.GetSessionVars().StmtCtx

	// Obtain the plan digest. It is set after optimization.
	_, planDigest := stmtCtx.GetPlanDigest()
	if planDigest == nil {
		return table.ResultCacheKey{}, nil, false
	}

	var key table.ResultCacheKey
	digestBytes := planDigest.Bytes()
	copy(key.PlanDigest[:], digestBytes)

	var paramBytes []byte

	// Hash parameter values when present. For prepared statements these are the
	// EXECUTE parameters; for non-prepared plan cache they are the extracted
	// literal values.
	if params := sctx.GetSessionVars().PlanCacheParams.AllParamValues(); len(params) > 0 {
		var ok bool
		paramBytes, ok = encodeParams(sctx.GetSessionVars().Location(), params)
		if !ok {
			return table.ResultCacheKey{}, nil, false
		}
	} else if len(stmtCtx.OriginalSQL) > 0 {
		// PlanCacheParams is empty when non-prepared plan cache is disabled or
		// the query bypasses plan cache parameterization. Fall back to hashing
		// the original SQL text to distinguish queries with different literals.
		paramBytes = []byte(stmtCtx.OriginalSQL)
	}

	// Include the session timezone offset in the cache key so that the same
	// query executed under different timezones maps to different cache entries.
	// TIMESTAMP columns are stored in UTC and converted on read; cached result
	// sets contain the post-conversion values and must not be reused across
	// timezone changes.
	paramBytes = appendTZOffset(paramBytes, sctx.GetSessionVars().Location())

	key.ParamHash = hashBytes(paramBytes)
	return key, paramBytes, true
}

// HashParamsForTest is exported for testing only.
var HashParamsForTest = hashParams

// EncodeParamsForTest is exported for testing only.
var EncodeParamsForTest = encodeParamsForTest

// hashBytes computes a 64-bit FNV-1a hash over a byte slice.
func hashBytes(b []byte) uint64 {
	h := fnv.New64a()
	h.Write(b)
	return h.Sum64()
}

// encodeParams encodes a slice of Datum values into a single byte slice using
// codec.EncodeKey, concatenating the encoded bytes for each parameter.
func encodeParams(loc *time.Location, params []types.Datum) ([]byte, bool) {
	if loc == nil {
		loc = time.UTC
	}
	var buf []byte
	for _, p := range params {
		var err error
		buf, err = codec.EncodeKey(loc, buf, p)
		if err != nil {
			return nil, false
		}
	}
	return buf, true
}

func encodeParamsForTest(params []types.Datum) []byte {
	buf, _ := encodeParams(time.UTC, params)
	return buf
}

// hashParams computes a 64-bit FNV-1a hash over a slice of Datum values.
// It uses codec.EncodeKey to produce a stable, type-aware byte representation
// of each parameter before feeding it to the hasher.
func hashParams(params []types.Datum) uint64 {
	encoded, ok := encodeParams(time.UTC, params)
	if !ok {
		return 0
	}
	return hashBytes(encoded)
}

// appendTZOffset appends the timezone UTC offset (in seconds) to buf so that
// different session timezones produce distinct cache keys.
//
// It also appends the timezone name to distinguish locations that share the
// same UTC offset but have different DST rules (e.g. "UTC" vs "Europe/London").
func appendTZOffset(buf []byte, loc *time.Location) []byte {
	if loc == nil {
		loc = time.UTC
	}
	_, offset := time.Date(2000, 1, 1, 0, 0, 0, 0, loc).Zone()
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(int32(offset)))
	buf = append(buf, b[:]...)

	nameBytes := []byte(loc.String())
	binary.BigEndian.PutUint32(b[:], uint32(len(nameBytes)))
	buf = append(buf, b[:]...)
	return append(buf, nameBytes...)
}
