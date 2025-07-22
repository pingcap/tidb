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
// See the License for the specific language governing permissions and
// limitations under the License.

package tidb

const (
	// FeatureIDTiDB represents the general TiDB-specific features.
	FeatureIDTiDB = ""
	// FeatureIDAutoRandom is the `auto_random` feature.
	FeatureIDAutoRandom = "auto_rand"
	// FeatureIDAutoIDCache is the `auto_id_cache` feature.
	FeatureIDAutoIDCache = "auto_id_cache"
	// FeatureIDAutoRandomBase is the `auto_random_base` feature.
	FeatureIDAutoRandomBase = "auto_rand_base"
	// FeatureIDClusteredIndex is the `clustered_index` feature.
	FeatureIDClusteredIndex = "clustered_index"
	// FeatureIDForceAutoInc is the `force auto_increment` feature.
	FeatureIDForceAutoInc = "force_inc"
	// FeatureIDPlacement is the `placement rule` feature.
	FeatureIDPlacement = "placement"
	// FeatureIDTTL is the `ttl` feature
	FeatureIDTTL = "ttl"
	// FeatureIDResourceGroup is the `resource group` feature.
	FeatureIDResourceGroup = "resource_group"
	// FeatureIDGlobalIndex is the `Global Index` feature.
	FeatureIDGlobalIndex = "global_index"
	// FeatureIDPresplit is the pre-split feature.
	FeatureIDPresplit = "pre_split"
)

var featureIDs = map[string]struct{}{
	FeatureIDAutoRandom:     {},
	FeatureIDAutoIDCache:    {},
	FeatureIDAutoRandomBase: {},
	FeatureIDClusteredIndex: {},
	FeatureIDForceAutoInc:   {},
	FeatureIDPlacement:      {},
	FeatureIDTTL:            {},
	FeatureIDGlobalIndex:    {},
	FeatureIDPresplit:       {},
}

// CanParseFeature is used to check if a feature can be parsed.
func CanParseFeature(fs ...string) bool {
	for _, f := range fs {
		if _, ok := featureIDs[f]; !ok {
			return false
		}
	}
	return true
}
