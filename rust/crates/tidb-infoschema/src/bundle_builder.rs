// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `bundle_builder.go` and `bundleInfoBuilder.updateInfoSchemaBundlesV2`.
use crate::SessionExtendedInfoSchema;

struct PolicyGetter<'a>(&'a SessionExtendedInfoSchema);
impl tidb_placement::PolicyGetter for PolicyGetter<'_> {
    fn get_policy(
        &self,
        id: i64,
    ) -> Result<tidb_model::PolicyInfo, tidb_placement::PlacementError> {
        self.0
            .policies()
            .find(|p| p.id == id)
            .cloned()
            .ok_or_else(|| {
                tidb_placement::PlacementError::wrap(
                    tidb_placement::PlacementErrorKind::InvalidPlacementOptions,
                    format!("Cannot find placement policy with ID: {id}"),
                )
            })
    }
}
impl SessionExtendedInfoSchema {
    /// Go `bundleInfoBuilder.updateInfoSchemaBundlesV2`: ordinary tables need
    /// no load; the resident placement metadata supplies table and partition rules.
    pub(crate) fn update_info_schema_bundles_v2(&mut self) {
        let Some(v2) = self.v2() else {
            return;
        };
        let mut bundles = std::collections::HashMap::new();
        for (_, tables) in v2.list_tables_with_special_attribute(|t| {
            t.placement_policy_ref.is_some()
                || t.partition.as_ref().is_some_and(|p| {
                    p.read()
                        .definitions
                        .with_visible(|defs| defs.iter().any(|d| d.placement_policy_ref.is_some()))
                })
        }) {
            for table in tables {
                let table = table.read();
                // Go logs invalid stored placement metadata and continues building.
                if let Ok(Some(bundle)) =
                    tidb_placement::new_table_bundle(&PolicyGetter(self), &table)
                {
                    bundles.insert(table.id, bundle);
                }
                if let Some(partition) = &table.partition {
                    for def in partition.read().definitions.snapshot() {
                        if let Ok(Some(bundle)) =
                            tidb_placement::new_partition_bundle(&PolicyGetter(self), &def)
                        {
                            bundles.insert(def.id, bundle);
                        }
                    }
                }
            }
        }
        self.schema.rule_bundle_map = bundles;
    }

    /// Go `infoSchemaMisc.PlacementBundleByPhysicalTableID`.
    pub fn placement_bundle_by_physical_table_id(
        &self,
        id: i64,
    ) -> Option<&tidb_placement::Bundle> {
        self.schema.rule_bundle_map.get(&id)
    }

    /// Go `infoSchemaMisc.AllPlacementBundles`.
    pub fn all_placement_bundles(&self) -> impl Iterator<Item = &tidb_placement::Bundle> {
        self.schema.rule_bundle_map.values()
    }
}
