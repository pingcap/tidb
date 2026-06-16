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

package collate

import "github.com/pingcap/tidb/pkg/util/collate/icudata"

// sharedICURoot is the single ICU root table instance shared by every locale collation, so each
// locale stores only its delta (overrides + contractions), not a full copy of the root.
var sharedICURoot = csrRoot{t: &icudata.Root}

// icuLocaleTailorings maps a locale code to its generated tailoring data. This is the curated
// first-cut set; expanding it is just adding generated data and a charset.go entry.
var icuLocaleTailorings = map[string]icudata.TailoringData{
	"da": icudata.Danish,
	"de": icudata.German,
	"es": icudata.Spanish,
	"fr": icudata.French,
	"sv": icudata.Swedish,
}

// tidbOnlyCollationIDs holds collations that only TiDB can evaluate. The TiKV/TiFlash coprocessor
// does not implement them, so the planner must not push expressions using them down to storage.
var tidbOnlyCollationIDs = map[int]struct{}{}

// IsTiDBOnlyCollation reports whether the named collation can only be evaluated by TiDB (not by the
// TiKV/TiFlash coprocessor). The planner uses this to keep such expressions from being pushed down;
// index-ordered reads still work because the sort-key bytes are byte-comparable in storage.
func IsTiDBOnlyCollation(name string) bool {
	if name == "" {
		return false
	}
	_, ok := tidbOnlyCollationIDs[CollationName2ID(name)]
	return ok
}

func icuTailoringFor(d icudata.TailoringData) *icuTailoring {
	return &icuTailoring{
		root:                sharedICURoot,
		override:            d.Override,
		contraction:         d.Contraction,
		maxContractionRunes: d.MaxContractionRunes,
	}
}

// registerICULocaleCollations registers the customer-facing locale collations
// utf8mb4_<locale>_0900_as_ci_kn: per-locale ordering, accent-sensitive, case-insensitive
// (ks-level2), with numeric ordering (kn). Case-first (kf) is inert at level 2, so it is not set.
func registerICULocaleCollations() {
	for loc, data := range icuLocaleTailorings {
		name := "utf8mb4_" + loc + "_0900_as_ci_kn"
		c := &unicodeICUCollator{
			t:        icuTailoringFor(data),
			numeric:  true,
			strength: strengthSecondary,
		}
		id := CollationName2ID(name)
		newCollatorMap[name] = c
		newCollatorIDMap[id] = c
		tidbOnlyCollationIDs[id] = struct{}{}
	}
}
