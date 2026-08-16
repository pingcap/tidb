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

//! Go `common.go`: bundle/rule naming constants and store-label vocabulary.

/// Go `TiFlashRuleGroupID`: the rule group id of tiflash.
pub const TIFLASH_RULE_GROUP_ID: &str = "tiflash";
/// Go `BundleIDPrefix`: the bundle prefix of all rule bundles from TiDB DDL
/// statements.
pub const BUNDLE_ID_PREFIX: &str = "TiDB_DDL_";
/// Go `PDBundleID`: the bundle name of pd, the default bundle for all regions.
pub const PD_BUNDLE_ID: &str = "pd";

/// Go `DefaultKwd`: used to reset the default rule (remove bundle).
pub const DEFAULT_KWD: &str = "default";
/// Go `TiDBBundleRangePrefixForGlobal`: the bundle prefix of the system global
/// range.
pub const TIDB_BUNDLE_RANGE_PREFIX_FOR_GLOBAL: &str = "TiDB_GLOBAL";
/// Go `TiDBBundleRangePrefixForMeta`: the bundle prefix of the system meta
/// range.
pub const TIDB_BUNDLE_RANGE_PREFIX_FOR_META: &str = "TiDB_META";
/// Go `KeyRangeGlobal`: the key range for the system global range.
pub const KEY_RANGE_GLOBAL: &str = "global";
/// Go `KeyRangeMeta`: the key range for the system meta range.
pub const KEY_RANGE_META: &str = "meta";

/// Go `metaPrefix`: the one-byte prefix of every meta key.
pub(crate) const META_PREFIX: &[u8] = b"m";

/// Go `GroupID`: accepts a table ID or whatever integer, and encodes it into a
/// valid group ID for PD.
#[must_use]
pub fn group_id(id: i64) -> String {
    format!("{BUNDLE_ID_PREFIX}{id}")
}

/// Go `RuleIndexKeyRangeForGlobal`: the index for a rule of the whole system
/// range.
pub const RULE_INDEX_KEY_RANGE_FOR_GLOBAL: i64 = 20;
/// Go `RuleIndexKeyRangeForMeta`: the index for a rule of the system meta
/// range.
pub const RULE_INDEX_KEY_RANGE_FOR_META: i64 = 21;
/// Go `RuleIndexTable`: the index for a rule of a table.
pub const RULE_INDEX_TABLE: i64 = 40;
/// Go `RuleIndexPartition`: the index for a rule of a partition.
pub const RULE_INDEX_PARTITION: i64 = 80;
/// Go `RuleIndexTiFlash`: the index for a rule of TiFlash.
pub const RULE_INDEX_TIFLASH: i64 = 120;

/// Go `DCLabelKey`: the key of the label which represents the DC for a store.
pub const DC_LABEL_KEY: &str = "zone";
/// Go `EngineLabelKey`: the label that indicates the backend of a store
/// instance, tikv or tiflash.
pub const ENGINE_LABEL_KEY: &str = "engine";
/// Go `EngineLabelTiFlash`: the label value a TiFlash instance carries under
/// [`ENGINE_LABEL_KEY`].
pub const ENGINE_LABEL_TIFLASH: &str = "tiflash";
/// Go `EngineLabelTiKV`: the label value used in some tests, and possibly by
/// TiKV itself.
pub const ENGINE_LABEL_TIKV: &str = "tikv";
/// Go `EngineLabelTiFlashCompute`: the label of tiflash_compute nodes in
/// disaggregated TiFlash mode.
pub const ENGINE_LABEL_TIFLASH_COMPUTE: &str = "tiflash_compute";
/// Go `EngineRoleLabelKey`: the label that indicates whether the TiFlash
/// instance is a write node.
pub const ENGINE_ROLE_LABEL_KEY: &str = "engine_role";
/// Go `EngineRoleLabelWrite`: the disaggregated TiFlash write-node role value.
pub const ENGINE_ROLE_LABEL_WRITE: &str = "write";

#[cfg(test)]
mod tests {
    use super::group_id;

    /// Go `TestGroup` (`common_test.go`).
    #[test]
    fn test_group() {
        assert_eq!("TiDB_DDL_1", group_id(1));
        assert_eq!("TiDB_DDL_90", group_id(90));
        assert_eq!("TiDB_DDL_-1", group_id(-1));
    }
}
