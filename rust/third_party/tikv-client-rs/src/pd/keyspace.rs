// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! pd-client's keyspace GC configuration (`keyspace_client.go`).

use crate::proto::keyspacepb;

/// pd-client `KeyspaceConfigGCManagementType`.
pub const KEYSPACE_CONFIG_GC_MANAGEMENT_TYPE: &str = "gc_management_type";
/// pd-client `KeyspaceConfigGCManagementTypeUnified`.
pub const KEYSPACE_CONFIG_GC_MANAGEMENT_TYPE_UNIFIED: &str = "unified";
/// pd-client `KeyspaceConfigGCManagementTypeKeyspaceLevel`.
pub const KEYSPACE_CONFIG_GC_MANAGEMENT_TYPE_KEYSPACE_LEVEL: &str = "keyspace_level";
const KEYSPACE_CONFIG_SAFE_POINT_VERSION: &str = "safe_point_version";
const KEYSPACE_CONFIG_SAFE_POINT_VERSION_V2: &str = "v2";

/// pd-client `IsKeyspaceUsingKeyspaceLevelGC`: whether the keyspace runs its
/// own GC rather than the unified GC of the null keyspace.
pub fn is_keyspace_using_keyspace_level_gc(meta: Option<&keyspacepb::KeyspaceMeta>) -> bool {
    let Some(meta) = meta else {
        return false;
    };
    // gc_management_type is authoritative when present; safe_point_version
    // is the fallback for legacy keyspace metadata.
    if let Some(management_type) = meta.config.get(KEYSPACE_CONFIG_GC_MANAGEMENT_TYPE) {
        return management_type == KEYSPACE_CONFIG_GC_MANAGEMENT_TYPE_KEYSPACE_LEVEL;
    }
    meta.config
        .get(KEYSPACE_CONFIG_SAFE_POINT_VERSION)
        .is_some_and(|version| version == KEYSPACE_CONFIG_SAFE_POINT_VERSION_V2)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(config: &[(&str, &str)]) -> keyspacepb::KeyspaceMeta {
        keyspacepb::KeyspaceMeta {
            config: config
                .iter()
                .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                .collect(),
            ..keyspacepb::KeyspaceMeta::default()
        }
    }

    #[test]
    fn keyspace_level_gc_follows_pd_client() {
        assert!(!is_keyspace_using_keyspace_level_gc(None));
        assert!(!is_keyspace_using_keyspace_level_gc(Some(&meta(&[]))));
        assert!(is_keyspace_using_keyspace_level_gc(Some(&meta(&[(
            KEYSPACE_CONFIG_GC_MANAGEMENT_TYPE,
            KEYSPACE_CONFIG_GC_MANAGEMENT_TYPE_KEYSPACE_LEVEL
        )]))));
        assert!(!is_keyspace_using_keyspace_level_gc(Some(&meta(&[(
            KEYSPACE_CONFIG_GC_MANAGEMENT_TYPE,
            KEYSPACE_CONFIG_GC_MANAGEMENT_TYPE_UNIFIED
        )]))));
        // The legacy flag counts only without gc_management_type.
        assert!(is_keyspace_using_keyspace_level_gc(Some(&meta(&[(
            "safe_point_version",
            "v2"
        )]))));
        assert!(!is_keyspace_using_keyspace_level_gc(Some(&meta(&[
            (
                KEYSPACE_CONFIG_GC_MANAGEMENT_TYPE,
                KEYSPACE_CONFIG_GC_MANAGEMENT_TYPE_UNIFIED
            ),
            ("safe_point_version", "v2"),
        ]))));
    }
}
