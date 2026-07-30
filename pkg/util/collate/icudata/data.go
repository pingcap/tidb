// Copyright 2026 PingCAP, Inc.
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

// Package icudata holds collation element tables extracted offline from ICU (the build-time-ICU
// approach). The tables are pure Go, so the shipped TiDB binary needs no ICU; only regenerating
// them does. Regenerate with: go run -tags icugen ./pkg/util/collate/icudata/icugen
//
// Each collation element is packed as (primary<<32 | secondary<<16 | tertiary), matching the
// format the multi-level key builder in package collate consumes.
//
// # Versioning and the REINDEX hazard
//
// Indexes on ICU-derived collations store sort-key bytes computed from these tables (the same
// hazard PostgreSQL manages via pg_collation.collversion). If a future change to this data alters
// any sort key, existing indexes become inconsistent and must be rebuilt. The governing rules:
//
//   - These tables are PINNED data. Do not regenerate them casually. Regenerating from a different
//     ICU version (see ICUVersion) may change weights.
//   - Any change that alters sort keys MUST be treated as a breaking change: it requires bumping the
//     TiDB version that ships it and rebuilding (ADMIN RECOVER / re-create) indexes on the affected
//     collations. It must never land silently within a patch release.
//   - TiDB does not yet track per-index collation-data versions (a full collversion mechanism is
//     future work); until it does, the policy above is enforced by review, and ICUVersion records
//     the provenance so a mismatch can be detected.
package icudata

// RootTable is the locale-neutral (ICU root) table over the BMP in compressed-sparse-row layout:
// the collation elements of rune r are CEData[Offset[r]:Offset[r+1]].
type RootTable struct {
	CEData []uint64
	Offset []uint32
}

// TailoringData is a per-locale delta over Root: rune overrides plus contractions (multi-rune
// sequences that collate as a unit, e.g. Danish "aa").
type TailoringData struct {
	Override            map[rune][]uint64
	Contraction         map[string][]uint64
	MaxContractionRunes int
}
