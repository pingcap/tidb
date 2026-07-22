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

//! TiDB parser feature IDs from `pkg/parser/tidb`.

/// General TiDB-specific syntax marker.
pub const FEATURE_ID_TIDB: &str = "";
/// `AUTO_RANDOM` syntax.
pub const FEATURE_ID_AUTO_RANDOM: &str = "auto_rand";
/// `AUTO_ID_CACHE` syntax.
pub const FEATURE_ID_AUTO_ID_CACHE: &str = "auto_id_cache";
/// `AUTO_RANDOM_BASE` syntax.
pub const FEATURE_ID_AUTO_RANDOM_BASE: &str = "auto_rand_base";
/// Clustered-index syntax.
pub const FEATURE_ID_CLUSTERED_INDEX: &str = "clustered_index";
/// Forced auto-increment syntax.
pub const FEATURE_ID_FORCE_AUTO_INC: &str = "force_inc";
/// Placement-rule syntax.
pub const FEATURE_ID_PLACEMENT: &str = "placement";
/// TTL syntax.
pub const FEATURE_ID_TTL: &str = "ttl";
/// Resource-group syntax; intentionally not in the parser allowlist.
pub const FEATURE_ID_RESOURCE_GROUP: &str = "resource_group";
/// Global-index syntax.
pub const FEATURE_ID_GLOBAL_INDEX: &str = "global_index";
/// Pre-split syntax.
pub const FEATURE_ID_PRESPLIT: &str = "pre_split";
/// Affinity syntax.
pub const FEATURE_ID_AFFINITY: &str = "affinity";
/// Region-split syntax.
pub const FEATURE_ID_SPLIT_REGION: &str = "region_split";

/// Returns whether every supplied feature is in the source parser's allowlist.
pub fn can_parse_feature(features: &[&str]) -> bool {
    features.iter().all(|feature| {
        matches!(
            *feature,
            FEATURE_ID_AUTO_RANDOM
                | FEATURE_ID_AUTO_ID_CACHE
                | FEATURE_ID_AUTO_RANDOM_BASE
                | FEATURE_ID_CLUSTERED_INDEX
                | FEATURE_ID_FORCE_AUTO_INC
                | FEATURE_ID_PLACEMENT
                | FEATURE_ID_TTL
                | FEATURE_ID_GLOBAL_INDEX
                | FEATURE_ID_PRESPLIT
                | FEATURE_ID_AFFINITY
                | FEATURE_ID_SPLIT_REGION
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_source_registry_and_variadic_semantics() {
        let supported = [
            FEATURE_ID_AUTO_RANDOM,
            FEATURE_ID_AUTO_ID_CACHE,
            FEATURE_ID_AUTO_RANDOM_BASE,
            FEATURE_ID_CLUSTERED_INDEX,
            FEATURE_ID_FORCE_AUTO_INC,
            FEATURE_ID_PLACEMENT,
            FEATURE_ID_TTL,
            FEATURE_ID_GLOBAL_INDEX,
            FEATURE_ID_PRESPLIT,
            FEATURE_ID_AFFINITY,
            FEATURE_ID_SPLIT_REGION,
        ];
        assert!(can_parse_feature(&[]));
        assert!(can_parse_feature(&supported));
        assert!(!can_parse_feature(&[FEATURE_ID_TIDB]));
        assert!(!can_parse_feature(&[FEATURE_ID_RESOURCE_GROUP]));
        assert!(!can_parse_feature(&[FEATURE_ID_TTL, "unknown"]));
    }
}
