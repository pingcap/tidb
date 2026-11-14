// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `NewLoaderForCrossKS` and `NewCrossKSSyncer` isolation rules.
use tidb_infoschema::cluster_catalog::{
    list_tables, with_meta_reader, ClusterCatalogError, MetaSnapshot,
};
use tidb_infoschema::SessionExtendedInfoSchema;
use tidb_model::{GoShared, GoSharedPointerSlice, SchemaDiff};

/// Go `Loader.fetchAllSchemasWithTables`, `LoadWithTS` and `Builder.WithCrossKS`.
pub fn load_info_schema<S: MetaSnapshot>(
    snapshot: &mut S,
) -> Result<SessionExtendedInfoSchema, ClusterCatalogError> {
    let id = tidb_metadef::system::SYSTEM_DATABASE_ID;
    let version = with_meta_reader(snapshot, |meta| {
        meta.get_schema_version_with_non_empty_diff()
    })?;
    let database = with_meta_reader(snapshot, |meta| meta.database(id))?;
    let mut database = database.ok_or_else(|| {
        ClusterCatalogError::Sql(tidb_terror::MysqlError::unknown(
            "system database not found",
        ))
    })?;
    database.deprecated_tables = GoSharedPointerSlice::from_handles(
        list_tables(snapshot, id)?
            .into_iter()
            .map(|table| Some(GoShared::new(table)))
            .collect(),
    );
    let (policies, groups) = with_meta_reader(snapshot, |meta| {
        Ok((meta.policies()?, meta.resource_groups()?))
    })?;
    Ok(SessionExtendedInfoSchema::init_with_db_infos_for_cross_ks(
        vec![GoShared::new(database)],
        policies,
        groups.iter().map(|group| (**group).clone()).collect(),
        version,
    ))
}

/// Go `Loader.skipLoadingDiff`: only reserved table IDs concern a cross-KS loader.
pub fn skip_loading_diff(cross_ks: bool, diff: &SchemaDiff) -> bool {
    cross_ks
        && !tidb_metadef::is_reserved_id(diff.table_id)
        && !tidb_metadef::is_reserved_id(diff.old_table_id)
}

/// Go `Syncer.skipMDLCheck`: the cross-KS session cannot hold user-table MDL.
pub fn skip_mdl_check(cross_ks: bool, table_ids: &[i64]) -> bool {
    cross_ks && !table_ids.iter().copied().any(tidb_metadef::is_reserved_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_metadef::RESERVED_GLOBAL_ID_UPPER_BOUND as RESERVED;
    /// Go `TestLoaderSkipLoadingDiff`.
    #[test]
    fn test_loader_skip_loading_diff() {
        for (table, old, skipped) in [
            (0, 0, true),
            (100, 0, true),
            (0, 100, true),
            (100, 100, true),
            (RESERVED, 0, false),
            (0, RESERVED, false),
            (RESERVED, RESERVED, false),
        ] {
            let diff = SchemaDiff {
                table_id: table,
                old_table_id: old,
                ..Default::default()
            };
            assert!(!skip_loading_diff(false, &diff));
            assert_eq!(skip_loading_diff(true, &diff), skipped);
        }
    }
    /// Go `TestSyncerSkipMDLCheck`.
    #[test]
    fn test_syncer_skip_mdl_check() {
        for (tables, skipped) in [
            (vec![], true),
            (vec![123], true),
            (vec![123, 456], true),
            (vec![RESERVED], false),
            (vec![123, RESERVED], false),
        ] {
            assert!(!skip_mdl_check(false, &tables));
            assert_eq!(skip_mdl_check(true, &tables), skipped);
        }
    }
}
