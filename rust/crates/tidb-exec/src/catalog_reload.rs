// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Bringing a loaded catalog up to the cluster's current schema version.
//!
//! Go source of truth: `pkg/infoschema/issyncer/loader.go`
//! (`LoadWithTS` / `tryLoadSchemaDiffs`) and `pkg/infoschema/builder.go`
//! (`ApplyDiff`). A DDL writes `Diff:<version>` describing exactly what one
//! schema version changed, so a reader holding version `v` reaches version `w`
//! by replaying `Diff:v+1 .. Diff:w` rather than re-reading every database and
//! table.
//!
//! Three rules keep this honest, all of them Go's:
//!
//! * The version and every diff and every object are read from ONE snapshot,
//!   so the result is a single schema version and never a blend of two.
//! * A diff this module does not know how to apply never produces a partial
//!   guess. It falls back to a full load at the same snapshot, which is always
//!   correct, merely more expensive.
//! * The version to reach is the newest one whose diff is actually stored
//!   (Go `GetSchemaVersionWithNonEmptyDiff`): a version whose diff has not
//!   been written yet is not observable as a schema, so it is not adopted.

use std::fmt;

use tidb_meta::{key, value};
use tidb_model::action_type::ActionType;
use tidb_model::schema_diff::SchemaDiff;

use crate::cluster_catalog::{
    load_cluster_catalog, ClusterCatalog, ClusterCatalogError, LoadedDatabase, MetaSnapshot,
};

/// Go `issyncer.LoadSchemaDiffVersionGapThreshold`: past this many versions a
/// full load is cheaper (and less failure-prone) than replaying every diff.
pub const LOAD_SCHEMA_DIFF_VERSION_GAP_THRESHOLD: i64 = 100;

/// Why the diff path gave up and a full load was performed instead.
///
/// Kept as data rather than a log line so the reload thread can report it and
/// tests can assert on the exact cause.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FullReloadReason {
    /// The stored version is older than the loaded one, so there are no diffs
    /// forward to replay (a flashback, or reading an older snapshot).
    VersionWentBackwards {
        /// The loaded catalog's version.
        from: i64,
        /// The version the snapshot reports.
        to: i64,
    },
    /// Too many versions elapsed; Go stops replaying at the same threshold.
    TooManyDiffs {
        /// The loaded catalog's version.
        from: i64,
        /// The version the snapshot reports.
        to: i64,
    },
    /// The diff demands the whole schema map be rebuilt.
    RegenerateSchemaMap {
        /// The version whose diff demanded it.
        version: i64,
    },
    /// The diff's action is one this tier does not apply incrementally.
    UnsupportedAction {
        /// The version whose diff could not be applied.
        version: i64,
        /// The action the diff carried.
        action: ActionType,
    },
    /// The diff referenced an object the loaded catalog or the snapshot does
    /// not have, so replaying it would invent or lose state.
    MissingObject {
        /// The version whose diff could not be applied.
        version: i64,
        /// Exact description of what was missing.
        detail: String,
    },
}

impl fmt::Display for FullReloadReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::VersionWentBackwards { from, to } => write!(
                formatter,
                "stored schema version {to} is older than the loaded version {from}"
            ),
            Self::TooManyDiffs { from, to } => write!(
                formatter,
                "schema version moved {} steps, from {from} to {to}",
                to - from
            ),
            Self::RegenerateSchemaMap { version } => write!(
                formatter,
                "schema version {version} carries a regenerate-schema-map diff"
            ),
            Self::UnsupportedAction { version, action } => write!(
                formatter,
                "schema version {version} was produced by `{action}`, which this node cannot apply incrementally"
            ),
            Self::MissingObject { version, detail } => write!(
                formatter,
                "schema version {version} references {detail}"
            ),
        }
    }
}

/// What one reload pass did.
#[derive(Clone, Debug)]
pub enum ReloadedCatalog {
    /// The cluster is still at the loaded version; nothing was read further
    /// and the caller keeps the catalog it already has.
    Unchanged {
        /// The version both sides agree on.
        version: i64,
    },
    /// The loaded catalog was advanced by replaying diffs.
    Diffs {
        /// The new catalog.
        catalog: ClusterCatalog,
        /// How many non-empty diffs were applied.
        applied: usize,
    },
    /// The whole catalog was re-read at this snapshot.
    Full {
        /// The new catalog.
        catalog: ClusterCatalog,
        /// Why the diff path was not taken.
        reason: FullReloadReason,
    },
}

impl ReloadedCatalog {
    /// The catalog to publish, `None` when nothing changed.
    #[must_use]
    pub const fn catalog(&self) -> Option<&ClusterCatalog> {
        match self {
            Self::Unchanged { .. } => None,
            Self::Diffs { catalog, .. } | Self::Full { catalog, .. } => Some(catalog),
        }
    }

    /// The schema version in force after this pass.
    #[must_use]
    pub const fn version(&self) -> i64 {
        match self {
            Self::Unchanged { version } => *version,
            Self::Diffs { catalog, .. } | Self::Full { catalog, .. } => catalog.schema_version,
        }
    }
}

/// Reads the newest schema version whose diff is actually stored.
///
/// Go `Mutator.GetSchemaVersionWithNonEmptyDiff`: the version counter is bumped
/// by one transaction and the diff written by another, so a reader can observe
/// a version whose diff does not exist yet. Adopting that version would let a
/// later read of the same version see more, which is the inconsistency the
/// step-back avoids.
fn schema_version_with_non_empty_diff<S: MetaSnapshot>(
    snapshot: &mut S,
) -> Result<i64, ClusterCatalogError> {
    let version = match snapshot.get(&key::schema_version_kv_key())? {
        Some(stored) => value::parse_int_value(&stored)
            .map_err(|error| ClusterCatalogError::Decode(format!("SchemaVersionKey: {error}")))?,
        None => 0,
    };
    if version > 0 && read_schema_diff(snapshot, version)?.is_none() {
        return Ok(version - 1);
    }
    Ok(version)
}

fn read_schema_diff<S: MetaSnapshot>(
    snapshot: &mut S,
    version: i64,
) -> Result<Option<SchemaDiff>, ClusterCatalogError> {
    let Some(stored) = snapshot.get(&key::schema_diff_kv_key(version))? else {
        return Ok(None);
    };
    value::parse_schema_diff(&stored)
        .map_err(|error| ClusterCatalogError::Decode(format!("Diff:{version}: {error}")))
}

/// Brings `current` up to the snapshot's schema version.
///
/// Everything this reads — the version, the diffs, and any table or database
/// definition a diff points at — comes from the one `snapshot`, so the answer
/// is one schema version rather than a mixture.
pub fn reload_cluster_catalog<S: MetaSnapshot>(
    snapshot: &mut S,
    current: &ClusterCatalog,
) -> Result<ReloadedCatalog, ClusterCatalogError> {
    let needed = schema_version_with_non_empty_diff(snapshot)?;
    let loaded = current.schema_version;
    if needed == loaded {
        return Ok(ReloadedCatalog::Unchanged { version: needed });
    }

    let reason = if needed < loaded {
        FullReloadReason::VersionWentBackwards {
            from: loaded,
            to: needed,
        }
    } else if needed - loaded >= LOAD_SCHEMA_DIFF_VERSION_GAP_THRESHOLD {
        FullReloadReason::TooManyDiffs {
            from: loaded,
            to: needed,
        }
    } else {
        match apply_diff_range(snapshot, current, loaded, needed)? {
            Ok(applied) => return Ok(applied),
            Err(reason) => reason,
        }
    };

    let mut catalog = load_cluster_catalog(snapshot)?;
    // The diff whose absence set `needed` back one version is still absent at
    // this snapshot, so the full load's own version reading is stepped back
    // the same way rather than trusting the raw counter.
    catalog.schema_version = needed;
    Ok(ReloadedCatalog::Full { catalog, reason })
}

/// Replays `loaded+1 ..= needed`, or names the first diff that blocked it.
///
/// The catalog is only cloned once, and a refusal discards the partially
/// advanced copy: the caller never sees a half-applied catalog.
#[allow(clippy::type_complexity)]
fn apply_diff_range<S: MetaSnapshot>(
    snapshot: &mut S,
    current: &ClusterCatalog,
    loaded: i64,
    needed: i64,
) -> Result<Result<ReloadedCatalog, FullReloadReason>, ClusterCatalogError> {
    let mut catalog = current.clone();
    let mut applied = 0usize;
    for version in (loaded + 1)..=needed {
        let Some(diff) = read_schema_diff(snapshot, version)? else {
            // Go skips an empty diff: the version-bumping transaction committed
            // and the DDL's did not, so the version carries no change at all.
            catalog.schema_version = version;
            continue;
        };
        if diff.regenerate_schema_map {
            return Ok(Err(FullReloadReason::RegenerateSchemaMap { version }));
        }
        if let Err(reason) = apply_schema_diff(snapshot, &mut catalog, &diff)? {
            return Ok(Err(reason));
        }
        // Go `Builder.SetSchemaVersion(diff.Version)`; the stored diff's own
        // version field and its key agree, and the key is authoritative.
        catalog.schema_version = version;
        applied += 1;
    }
    Ok(Ok(ReloadedCatalog::Diffs { catalog, applied }))
}

/// Applies one diff, or names why it cannot be applied incrementally.
///
/// Go's `Builder.ApplyDiff` covers every action kind; this tier covers the
/// object-lifecycle subset its read path can actually serve, and refuses the
/// rest outright. A refusal is not a failure — it is the caller's signal to
/// take the full load, which is always correct.
fn apply_schema_diff<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &mut ClusterCatalog,
    diff: &SchemaDiff,
) -> Result<Result<(), FullReloadReason>, ClusterCatalogError> {
    let version = diff.version;
    match diff.action_type {
        ActionType::ACTION_CREATE_SCHEMA => {
            let Some(info) = read_database(snapshot, diff.schema_id)? else {
                return Ok(Err(missing(
                    version,
                    format!("database {}", diff.schema_id),
                )));
            };
            catalog.databases.retain(|db| db.info.id != info.id);
            catalog.databases.push(LoadedDatabase {
                info,
                tables: Vec::new(),
            });
        }
        ActionType::ACTION_DROP_SCHEMA => {
            catalog.databases.retain(|db| db.info.id != diff.schema_id);
        }
        // Go `getTableIDs` classifies a materialized view or view log create
        // exactly like `ActionCreateTable`: the diff's own table ID names the
        // new physical table (Go master `94a9cbedab`).
        ActionType::ACTION_CREATE_TABLE
        | ActionType::ACTION_CREATE_MATERIALIZED_VIEW
        | ActionType::ACTION_CREATE_MATERIALIZED_VIEW_LOG => {
            if let Err(reason) =
                create_table(snapshot, catalog, version, diff.schema_id, diff.table_id)?
            {
                return Ok(Err(reason));
            }
        }
        ActionType::ACTION_CREATE_TABLES => {
            // Go `applyCreateTables`: the diff's own table ID is unset and
            // every created table is listed as an affected option.
            for affected in diff.affected_options.iter_handles() {
                // Go dereferences each `*AffectedOption`; a stored null
                // element is corrupt metadata and panics rather than
                // being silently skipped.
                let affected = affected.expect("nil affected option in create-tables schema diff");
                let (schema_id, table_id) = {
                    let affected = affected.read();
                    (affected.schema_id, affected.table_id)
                };
                if let Err(reason) = create_table(snapshot, catalog, version, schema_id, table_id)?
                {
                    return Ok(Err(reason));
                }
            }
        }
        ActionType::ACTION_DROP_TABLE => {
            if let Err(reason) = drop_table(catalog, version, diff.schema_id, diff.table_id)? {
                return Ok(Err(reason));
            }
        }
        ActionType::ACTION_TRUNCATE_TABLE => {
            // The table keeps its name and gains a new ID; the old ID's data
            // is dropped. Order matters only for the degenerate equal-ID case,
            // which cannot happen, but dropping first keeps names unique.
            if let Err(reason) = drop_table(catalog, version, diff.schema_id, diff.old_table_id)? {
                return Ok(Err(reason));
            }
            if let Err(reason) =
                create_table(snapshot, catalog, version, diff.schema_id, diff.table_id)?
            {
                return Ok(Err(reason));
            }
        }
        // Go `getTableIDs`'s `default:` case again (RENAME_TABLE(S) is not
        // one of the special-cased action types in `getTableIDs`, so it
        // keeps its ID too), but `dropTableForUpdate`
        // (`builder.go:546-576`) special-cases these two: when the rename
        // crosses databases (`diff.OldSchemaID != diff.SchemaID`) the old
        // copy must be dropped from the OLD database, not the new one,
        // because `applyTableUpdate`'s single `dbInfo` is resolved from
        // `diff.SchemaID` (the new database) throughout. A same-database
        // rename needs no extra step: `create_table`'s dedup-by-ID `retain`
        // already replaces the old name with the freshly read one in place.
        // `ACTION_RENAME_TABLES`' own diff covers its first renamed table
        // the same way `ACTION_CREATE_TABLES` covers its own; the rest are
        // each one `AffectedOption`, with their own `old_schema_id`
        // (`schema_version.go:96-115`, one entry per table after the first).
        ActionType::ACTION_RENAME_TABLE | ActionType::ACTION_RENAME_TABLES => {
            if let Err(reason) = apply_rename_table(
                snapshot,
                catalog,
                version,
                diff.schema_id,
                diff.old_schema_id,
                diff.table_id,
            )? {
                return Ok(Err(reason));
            }
            for affected in diff.affected_options.iter_handles() {
                let affected = affected.expect("nil affected option in rename-tables schema diff");
                let (schema_id, table_id, old_schema_id) = {
                    let affected = affected.read();
                    (
                        affected.schema_id,
                        affected.table_id,
                        affected.old_schema_id,
                    )
                };
                if let Err(reason) = apply_rename_table(
                    snapshot,
                    catalog,
                    version,
                    schema_id,
                    old_schema_id,
                    table_id,
                )? {
                    return Ok(Err(reason));
                }
            }
        }
        // Go `getTableIDs`'s `default:` case (`oldTableID = newTableID =
        // diff.TableID`), reached through `ApplyDiff`'s own `default:` arm
        // (`applyDefaultAction` -> `applyTableUpdate`) for every action type
        // below: the table keeps its ID and its database, so re-reading its
        // one `TableInfo` and swapping it in for the old copy is exactly what
        // Go's `applyCreateTable` does for this case (`m.GetTable` then
        // replace), modulo the in-memory caches (auto-ID allocators,
        // placement bundles, masking-policy cache, `sortedTablesBuckets`)
        // this simpler catalog does not keep at all -- there is nothing here
        // for those Go side effects to go stale, so there is nothing to
        // replicate for them. Excluded on purpose: anything that changes a
        // table's ID (`CREATE_VIEW`, partition/multi-schema-change actions,
        // ...), which Go's `getTableIDs` special-cases and this tier does not
        // attempt to.
        ActionType::ACTION_ADD_COLUMN
        | ActionType::ACTION_DROP_COLUMN
        | ActionType::ACTION_ADD_COLUMNS
        | ActionType::ACTION_DROP_COLUMNS
        | ActionType::ACTION_MODIFY_COLUMN
        | ActionType::ACTION_ADD_INDEX
        | ActionType::ACTION_DROP_INDEX
        | ActionType::ACTION_RENAME_INDEX
        | ActionType::ACTION_ALTER_INDEX_VISIBILITY
        | ActionType::ACTION_ADD_PRIMARY_KEY
        | ActionType::ACTION_DROP_PRIMARY_KEY
        | ActionType::ACTION_SET_DEFAULT_VALUE
        | ActionType::ACTION_MODIFY_TABLE_COMMENT
        | ActionType::ACTION_MODIFY_TABLE_CHARSET_AND_COLLATE
        | ActionType::ACTION_SHARD_ROW_ID
        | ActionType::ACTION_REBASE_AUTO_ID
        | ActionType::ACTION_REBASE_AUTO_RANDOM_BASE
        | ActionType::ACTION_MODIFY_TABLE_AUTO_IDCACHE
        | ActionType::ACTION_ADD_FOREIGN_KEY
        | ActionType::ACTION_DROP_FOREIGN_KEY
        | ActionType::ACTION_ADD_CHECK_CONSTRAINT
        | ActionType::ACTION_DROP_CHECK_CONSTRAINT
        | ActionType::ACTION_ALTER_CHECK_CONSTRAINT
        | ActionType::ACTION_LOCK_TABLE
        | ActionType::ACTION_UNLOCK_TABLE
        | ActionType::ACTION_REPAIR_TABLE
        | ActionType::ACTION_SET_TI_FLASH_REPLICA
        | ActionType::ACTION_UPDATE_TI_FLASH_REPLICA_STATUS
        | ActionType::ACTION_ALTER_TABLE_ATTRIBUTES
        | ActionType::ACTION_ALTER_CACHE_TABLE
        | ActionType::ACTION_ALTER_NO_CACHE_TABLE
        | ActionType::ACTION_ALTER_TABLE_STATS_OPTIONS
        | ActionType::ACTION_ALTER_TTLINFO
        | ActionType::ACTION_ALTER_TTLREMOVE
        | ActionType::ACTION_ADD_COLUMNAR_INDEX
        | ActionType::ACTION_MODIFY_ENGINE_ATTRIBUTE
        | ActionType::ACTION_ALTER_TABLE_MODE
        | ActionType::ACTION_ALTER_TABLE_AFFINITY
        | ActionType::ACTION_ALTER_TABLE_SOFT_DELETE_INFO
        | ActionType::ACTION_ALTER_TABLE_SET_REGION_SPLIT_POLICY => {
            if let Err(reason) = apply_table_update(snapshot, catalog, version, diff)? {
                return Ok(Err(reason));
            }
        }
        action => return Ok(Err(FullReloadReason::UnsupportedAction { version, action })),
    }
    Ok(Ok(()))
}

const fn missing(version: i64, detail: String) -> FullReloadReason {
    FullReloadReason::MissingObject { version, detail }
}

fn read_database<S: MetaSnapshot>(
    snapshot: &mut S,
    db_id: i64,
) -> Result<Option<tidb_model::db::DBInfo>, ClusterCatalogError> {
    let Some(stored) = snapshot.get(&key::database_kv_key(db_id))? else {
        return Ok(None);
    };
    value::parse_db_info(&stored)
        .map(Some)
        .map_err(|error| ClusterCatalogError::Decode(format!("DBInfo {db_id}: {error}")))
}

fn create_table<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &mut ClusterCatalog,
    version: i64,
    db_id: i64,
    table_id: i64,
) -> Result<Result<(), FullReloadReason>, ClusterCatalogError> {
    let Some(database) = catalog.databases.iter_mut().find(|db| db.info.id == db_id) else {
        return Ok(Err(missing(version, format!("unknown database {db_id}"))));
    };
    let Some(stored) = snapshot.get(&key::table_kv_key(db_id, table_id))? else {
        return Ok(Err(missing(
            version,
            format!("table {table_id} in database {db_id}, which the snapshot does not store"),
        )));
    };
    let table = value::parse_table_info(&stored, db_id)
        .map_err(|error| ClusterCatalogError::Decode(format!("TableInfo {table_id}: {error}")))?;
    database.tables.retain(|existing| existing.id != table.id);
    database.tables.push(table);
    Ok(Ok(()))
}

/// Go `applyTableUpdate`'s same-ID case: reload the diff's own table, then
/// (Go `applyAffectedOpts`) do the same for every table an `AffectedOption`
/// names, in case a covered action type's diff ever lists more than one
/// table under a single schema version even though none of them changed ID
/// (`ACTION_MULTI_SCHEMA_CHANGE` itself is not one of this tier's covered
/// action types -- it stays on the `UnsupportedAction` path below).
fn apply_table_update<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &mut ClusterCatalog,
    version: i64,
    diff: &SchemaDiff,
) -> Result<Result<(), FullReloadReason>, ClusterCatalogError> {
    if let Err(reason) = create_table(snapshot, catalog, version, diff.schema_id, diff.table_id)? {
        return Ok(Err(reason));
    }
    for affected in diff.affected_options.iter_handles() {
        let affected = affected.expect("nil affected option in schema diff");
        let (schema_id, table_id) = {
            let affected = affected.read();
            (affected.schema_id, affected.table_id)
        };
        if let Err(reason) = create_table(snapshot, catalog, version, schema_id, table_id)? {
            return Ok(Err(reason));
        }
    }
    Ok(Ok(()))
}

fn drop_table(
    catalog: &mut ClusterCatalog,
    version: i64,
    db_id: i64,
    table_id: i64,
) -> Result<Result<(), FullReloadReason>, ClusterCatalogError> {
    let Some(database) = catalog.databases.iter_mut().find(|db| db.info.id == db_id) else {
        return Ok(Err(missing(version, format!("unknown database {db_id}"))));
    };
    database.tables.retain(|existing| existing.id != table_id);
    Ok(Ok(()))
}

/// One renamed table: Go `dropTableForUpdate`'s rename special case
/// (`builder.go:568-579`) plus the `applyCreateTable` every `getTableIDs`
/// case ends in. `old_schema_id` is `0` only for a stored diff predating
/// this field; treated the same as "same database" since there is no other
/// database to remove the stale copy from.
fn apply_rename_table<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &mut ClusterCatalog,
    version: i64,
    schema_id: i64,
    old_schema_id: i64,
    table_id: i64,
) -> Result<Result<(), FullReloadReason>, ClusterCatalogError> {
    if old_schema_id != 0 && old_schema_id != schema_id {
        if let Err(reason) = drop_table(catalog, version, old_schema_id, table_id)? {
            return Ok(Err(reason));
        }
    }
    create_table(snapshot, catalog, version, schema_id, table_id)
}
