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
//! * A diff that names an object the loaded catalog or the snapshot does not
//!   have (or already has) never produces a partial guess. It falls back to a
//!   full load at the same snapshot, which is always correct, merely more
//!   expensive -- where Go's applier would return the error its loader turns
//!   into a full load.
//! * The version to reach is the newest one whose diff is actually stored
//!   (Go `GetSchemaVersionWithNonEmptyDiff`): a version whose diff has not
//!   been written yet is not observable as a schema, so it is not adopted.

use std::fmt;

use tidb_meta::{key, value};
use tidb_model::action_type::ActionType;
use tidb_model::schema_diff::SchemaDiff;
use tidb_model::schema_state::SchemaState;

use crate::cluster_catalog::{
    load_cluster_catalog, load_database_tables, normalize_diff_loaded_table_info, ClusterCatalog,
    ClusterCatalogError, LoadedDatabase, MetaSnapshot,
};
use crate::schema_validator::RelatedSchemaChange;

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
    /// The diff would create an object the loaded catalog already has
    /// (Go `ErrDatabaseExists` from `applyRecoverSchema`), so replaying it
    /// would duplicate state.
    ConflictingObject {
        /// The version whose diff could not be applied.
        version: i64,
        /// Exact description of what already existed.
        detail: String,
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
            Self::ConflictingObject { version, detail } => {
                write!(formatter, "schema version {version} recreates {detail}")
            }
            Self::MissingObject { version, detail } => {
                write!(formatter, "schema version {version} references {detail}")
            }
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
        /// The physical tables the diffs changed and the action that changed
        /// each (Go `tryLoadSchemaDiffs`'s `RelatedSchemaChange`,
        /// `loader.go:378-401`), for the schema validator.
        changes: RelatedSchemaChange,
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
    let mut changes = RelatedSchemaChange::default();
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
        let ids = match apply_schema_diff(snapshot, &mut catalog, &diff)? {
            Ok(ids) => ids,
            Err(reason) => return Ok(Err(reason)),
        };
        // Go `Builder.SetSchemaVersion(diff.Version)`; the stored diff's own
        // version field and its key agree, and the key is authoritative.
        catalog.schema_version = version;
        applied += 1;
        // Go `canSkipSchemaCheckerDDL` (`loader.go:624-630`): a TiFlash
        // replica change never invalidates a transaction.
        if !matches!(
            diff.action_type,
            ActionType::ACTION_UPDATE_TI_FLASH_REPLICA_STATUS
                | ActionType::ACTION_SET_TI_FLASH_REPLICA
        ) {
            let action = u64::from(diff.action_type.0);
            changes
                .action_types
                .extend(std::iter::repeat_n(action, ids.len()));
            changes.phy_tbl_ids.extend(ids);
        }
    }
    Ok(Ok(ReloadedCatalog::Diffs {
        catalog,
        applied,
        changes,
    }))
}

/// One applied diff: the physical table IDs it touched (Go `ApplyDiff`'s
/// `[]int64`), or why it could not be applied incrementally.
type AppliedDiff = Result<Result<Vec<i64>, FullReloadReason>, ClusterCatalogError>;

/// Applies one diff, or names why it cannot be applied incrementally.
///
/// Go `Builder.ApplyDiff` (`builder.go:70-115`), case for case. Its
/// `default:` is a real default here too (`applyDefaultAction`), so every
/// action kind Go applies through the one-table-in-place path is applied the
/// same way. A refusal is not a failure -- it is the caller's signal to take
/// the full load, which is always correct -- and it happens exactly where
/// Go's own applier returns an error the loader would then turn into a full
/// load: a database, table or policy the diff names that is not there.
///
/// What this catalog does not keep, Go's appliers for it touch nothing else:
/// placement policies, resource groups and masking policies (their own maps),
/// placement bundles, auto-ID allocators, `sortedTablesBuckets` and the
/// temporary-table set (in-memory caches). A full load reads none of those
/// either, so applying their diffs as no-ops is the same catalog a full load
/// would produce, not a guess.
///
/// The IDs answered are Go's, case for case: the tables (and partitions,
/// `appendAffectedIDs`) the diff dropped or (re)created, which the schema
/// validator records against the version so a transaction planned before it
/// can be refused at commit.
fn apply_schema_diff<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &mut ClusterCatalog,
    diff: &SchemaDiff,
) -> AppliedDiff {
    let version = diff.version;
    match diff.action_type {
        // Go `applyCreateSchema` (`builder.go:698-712`): the database read
        // from the store, with no tables yet.
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
            Ok(Ok(Vec::new()))
        }
        // Go `applyDropSchema` (`builder.go:747-771`): absent is a no-op;
        // every table of the database is answered as dropped.
        ActionType::ACTION_DROP_SCHEMA => {
            let mut ids = Vec::new();
            if let Some(database) = catalog
                .databases
                .iter()
                .find(|db| db.info.id == diff.schema_id)
            {
                for table in &database.tables {
                    affected_ids(table, &mut ids);
                }
            }
            catalog.databases.retain(|db| db.info.id != diff.schema_id);
            Ok(Ok(ids))
        }
        ActionType::ACTION_RECOVER_SCHEMA => recover_schema(snapshot, catalog, diff),
        ActionType::ACTION_MODIFY_SCHEMA_CHARSET_AND_COLLATE => modify_schema(
            snapshot,
            catalog,
            version,
            diff.schema_id,
            |loaded, fresh| {
                loaded.charset = fresh.charset;
                loaded.collate = fresh.collate;
            },
        ),
        ActionType::ACTION_MODIFY_SCHEMA_DEFAULT_PLACEMENT => modify_schema(
            snapshot,
            catalog,
            version,
            diff.schema_id,
            |loaded, fresh| {
                loaded.placement_policy_ref = fresh.placement_policy_ref;
            },
        ),
        // Go `applyCreatePolicy` / `applyAlterPolicy` / `applyDropPolicy`
        // (`builder_misc.go:25-72`), `applyCreateOrAlterResourceGroup` /
        // `applyDropResourceGroup` (`:74-95`) and `applyMaskingPolicyChange`
        // (`builder.go:412-415`) touch only the policy, resource-group and
        // masking-policy maps, none of which this catalog holds.
        ActionType::ACTION_CREATE_PLACEMENT_POLICY
        | ActionType::ACTION_ALTER_PLACEMENT_POLICY
        | ActionType::ACTION_DROP_PLACEMENT_POLICY
        | ActionType::ACTION_CREATE_RESOURCE_GROUP
        | ActionType::ACTION_ALTER_RESOURCE_GROUP
        | ActionType::ACTION_DROP_RESOURCE_GROUP
        | ActionType::ACTION_CREATE_MASKING_POLICY
        | ActionType::ACTION_ALTER_MASKING_POLICY
        | ActionType::ACTION_DROP_MASKING_POLICY => Ok(Ok(Vec::new())),
        // Go `applyTruncateTableOrPartition`, `applyDropTableOrPartition`,
        // `applyRecoverTable` and `applyReorganizePartition`
        // (`builder.go:260-320`, `:397-410`): one `applyTableUpdate`, then
        // bundle bookkeeping only. `AffectedOpts` name partitions there, for
        // the bundles -- they are not tables to reload; a truncated
        // partition's old ID is still answered as changed
        // (`builder.go:275-281`).
        ActionType::ACTION_TRUNCATE_TABLE_PARTITION
        | ActionType::ACTION_TRUNCATE_TABLE
        | ActionType::ACTION_DROP_TABLE
        | ActionType::ACTION_DROP_TABLE_PARTITION
        | ActionType::ACTION_RECOVER_TABLE
        | ActionType::ACTION_REORGANIZE_PARTITION
        | ActionType::ACTION_REMOVE_PARTITIONING
        | ActionType::ACTION_ALTER_TABLE_PARTITIONING => {
            let mut ids = match apply_table_update(snapshot, catalog, diff)? {
                Ok(ids) => ids,
                Err(reason) => return Ok(Err(reason)),
            };
            if diff.action_type == ActionType::ACTION_TRUNCATE_TABLE_PARTITION {
                for affected in diff.affected_options.iter_handles() {
                    let affected =
                        affected.expect("nil affected option in truncate-partition schema diff");
                    ids.push(affected.read().old_table_id);
                }
            }
            Ok(Ok(ids))
        }
        // Go `applyCreateTables` (`builder.go:117-119`): every created table
        // is an `AffectedOption`, each replayed as its own `CreateTable`.
        ActionType::ACTION_CREATE_TABLES => apply_affected_opts(
            snapshot,
            catalog,
            diff,
            ActionType::ACTION_CREATE_TABLE,
            "nil affected option in create-tables schema diff",
        ),
        ActionType::ACTION_EXCHANGE_TABLE_PARTITION => {
            exchange_table_partition(snapshot, catalog, diff)
        }
        // Go returns `[]int64{-1}` and changes nothing: a flashback whose
        // diff demands the whole map be rebuilt carries `RegenerateSchemaMap`
        // and never reaches here. `-1` is the "every table" ID the validator
        // matches against any transaction.
        ActionType::ACTION_FLASHBACK_CLUSTER => Ok(Ok(vec![-1])),
        ActionType::ACTION_REFRESH_META => refresh_meta(snapshot, catalog, diff),
        // Go `applyDefaultAction` (`builder.go:471-478`): the diff's own
        // table, then every `AffectedOption` replayed with the same action.
        _ => {
            let mut ids = match apply_table_update(snapshot, catalog, diff)? {
                Ok(ids) => ids,
                Err(reason) => return Ok(Err(reason)),
            };
            match apply_affected_opts(
                snapshot,
                catalog,
                diff,
                diff.action_type,
                "nil affected option in schema diff",
            )? {
                Ok(more) => ids.extend(more),
                Err(reason) => return Ok(Err(reason)),
            }
            Ok(Ok(ids))
        }
    }
}

/// Go `appendAffectedIDs` (`builder.go:688-696`): the table's ID and, for a
/// partitioned table, every partition's.
fn affected_ids(table: &tidb_model::table_info::TableInfo, into: &mut Vec<i64>) {
    into.push(table.id);
    if let Some(partition) = table.get_partition_info() {
        let definitions = partition.read().definitions.snapshot();
        into.extend(definitions.iter().map(|definition| definition.id));
    }
}

/// Go `Builder.applyAffectedOpts` (`builder.go:450-469`): each option is
/// replayed as its own diff of type `action` through the whole dispatch. Go
/// dereferences each `*AffectedOption`, so a stored null element is corrupt
/// metadata and panics rather than being silently skipped.
fn apply_affected_opts<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &mut ClusterCatalog,
    diff: &SchemaDiff,
    action: ActionType,
    nil_message: &'static str,
) -> AppliedDiff {
    let mut ids = Vec::new();
    for affected in diff.affected_options.iter_handles() {
        let affected = affected.expect(nil_message);
        let affected_diff = {
            let affected = affected.read();
            SchemaDiff {
                version: diff.version,
                action_type: action,
                schema_id: affected.schema_id,
                table_id: affected.table_id,
                old_schema_id: affected.old_schema_id,
                old_table_id: affected.old_table_id,
                ..SchemaDiff::default()
            }
        };
        match apply_schema_diff(snapshot, catalog, &affected_diff)? {
            Ok(more) => ids.extend(more),
            Err(reason) => return Ok(Err(reason)),
        }
    }
    Ok(Ok(ids))
}

/// Go `tableIDIsValid`.
const fn table_id_is_valid(table_id: i64) -> bool {
    table_id > 0
}

/// Go `Builder.getTableIDs` (`builder.go:480-528`): which table ID a diff
/// drops and which it (re)creates. `0` on either side means "none".
fn table_ids<S: MetaSnapshot>(
    snapshot: &mut S,
    diff: &SchemaDiff,
) -> Result<(i64, i64), ClusterCatalogError> {
    Ok(match diff.action_type {
        ActionType::ACTION_CREATE_SEQUENCE | ActionType::ACTION_RECOVER_TABLE => (0, diff.table_id),
        // A `CREATE TABLE` with foreign keys reaches public in two steps and
        // its second diff sets `OldTableID` (`schema_version.go:257-259`), so
        // the write-only copy is dropped before the public one is added.
        ActionType::ACTION_CREATE_TABLE => (diff.old_table_id, diff.table_id),
        ActionType::ACTION_DROP_TABLE
        | ActionType::ACTION_DROP_VIEW
        | ActionType::ACTION_DROP_SEQUENCE => {
            let old = diff.table_id;
            // BR's refreshMeta drops outright: no `ON DELETE CASCADE` to serve.
            if diff.is_refresh_meta {
                return Ok((old, 0));
            }
            // The table stays until its state reaches none, so a foreign
            // key's `ON DELETE/UPDATE CASCADE` can still find it (Go keeps
            // this for every table, not just those with foreign keys).
            let stored = snapshot.get(&key::table_kv_key(diff.schema_id, old))?;
            let still_there = match stored {
                Some(stored) => {
                    let table =
                        value::parse_table_info(&stored, diff.schema_id).map_err(|error| {
                            ClusterCatalogError::Decode(format!("TableInfo {old}: {error}"))
                        })?;
                    table.state != SchemaState::NONE
                }
                None => false,
            };
            (old, if still_there { diff.table_id } else { 0 })
        }
        ActionType::ACTION_TRUNCATE_TABLE
        | ActionType::ACTION_CREATE_VIEW
        | ActionType::ACTION_EXCHANGE_TABLE_PARTITION
        | ActionType::ACTION_ALTER_TABLE_PARTITIONING
        | ActionType::ACTION_REMOVE_PARTITIONING => (diff.old_table_id, diff.table_id),
        _ => (diff.table_id, diff.table_id),
    })
}

/// Go `Builder.applyTableUpdate` (`builder.go:590-620`) with
/// `dropTableForUpdate` (`:546-579`) inlined: the diff's database must be
/// loaded; the old table, if any, is dropped -- from the OLD database when a
/// rename crossed databases, since Go's one `dbInfo` is resolved from the new
/// `SchemaID` throughout -- and the new one, if any, is read from the store
/// and added. Everything else Go does there (`updateBundleForTableUpdate`,
/// `copySortedTables`, kept auto-ID allocators, the masking-policy cache
/// reset) maintains caches this catalog does not have.
///
/// The IDs answered are Go's `tblIDs`: the dropped table's (`applyDropTable`,
/// only when it was loaded) then the created table's (`applyCreateTable`,
/// except for a truncated partition, `builder.go:836-838`).
fn apply_table_update<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &mut ClusterCatalog,
    diff: &SchemaDiff,
) -> AppliedDiff {
    let version = diff.version;
    if !catalog
        .databases
        .iter()
        .any(|db| db.info.id == diff.schema_id)
    {
        return Ok(Err(missing(
            version,
            format!("unknown database {}", diff.schema_id),
        )));
    }
    let (old_table_id, new_table_id) = table_ids(snapshot, diff)?;
    let mut ids = Vec::new();
    if table_id_is_valid(old_table_id) {
        let is_rename = matches!(
            diff.action_type,
            ActionType::ACTION_RENAME_TABLE | ActionType::ACTION_RENAME_TABLES
        );
        let drop_from = if is_rename && diff.old_schema_id != diff.schema_id {
            diff.old_schema_id
        } else {
            diff.schema_id
        };
        match drop_table(catalog, version, drop_from, old_table_id)? {
            Ok(dropped) => ids.extend(dropped),
            Err(reason) => return Ok(Err(reason)),
        }
    }
    if table_id_is_valid(new_table_id) {
        match create_table(snapshot, catalog, version, diff.schema_id, new_table_id)? {
            Ok(created) => {
                if diff.action_type != ActionType::ACTION_TRUNCATE_TABLE_PARTITION {
                    ids.extend(created);
                }
            }
            Err(reason) => return Ok(Err(reason)),
        }
    }
    Ok(Ok(ids))
}

/// Go `Builder.applyRecoverSchema` (`builder.go:774-788`): the database must
/// not be loaded yet, and comes back with its tables. `RECOVER SCHEMA`'s own
/// diff always sets `ReadTableFromMeta` (`schema_version.go:280`), which has
/// Go list the database's tables from the store instead of trusting the
/// diff's `AffectedOpts`; the listing here is the same one the full load
/// uses, at this snapshot. Every table that comes back is answered as
/// created, as Go's `applyCreateTables` over the listing answers.
fn recover_schema<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &mut ClusterCatalog,
    diff: &SchemaDiff,
) -> AppliedDiff {
    let version = diff.version;
    if catalog
        .databases
        .iter()
        .any(|db| db.info.id == diff.schema_id)
    {
        return Ok(Err(FullReloadReason::ConflictingObject {
            version,
            detail: format!("database {} which is already loaded", diff.schema_id),
        }));
    }
    let Some(info) = read_database(snapshot, diff.schema_id)? else {
        return Ok(Err(missing(
            version,
            format!("database {}", diff.schema_id),
        )));
    };
    if diff.read_table_from_meta {
        let tables = load_database_tables(snapshot, info.id)?;
        let mut ids = Vec::new();
        for table in &tables {
            affected_ids(table, &mut ids);
        }
        catalog.databases.push(LoadedDatabase { info, tables });
        return Ok(Ok(ids));
    }
    catalog.databases.push(LoadedDatabase {
        info,
        tables: Vec::new(),
    });
    apply_affected_opts(
        snapshot,
        catalog,
        diff,
        ActionType::ACTION_CREATE_TABLE,
        "nil affected option in recover-schema schema diff",
    )
}

/// Go `applyModifySchemaCharsetAndCollate` / `applyModifySchemaDefaultPlacement`
/// (`builder.go:714-745`): re-read the `DBInfo` and copy the changed fields
/// onto the loaded one. No table is answered as changed.
fn modify_schema<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &mut ClusterCatalog,
    version: i64,
    db_id: i64,
    apply: impl FnOnce(&mut tidb_model::db::DBInfo, tidb_model::db::DBInfo),
) -> AppliedDiff {
    let Some(fresh) = read_database(snapshot, db_id)? else {
        return Ok(Err(missing(version, format!("database {db_id}"))));
    };
    let Some(database) = catalog.databases.iter_mut().find(|db| db.info.id == db_id) else {
        return Ok(Err(missing(version, format!("unknown database {db_id}"))));
    };
    apply(&mut database.info, fresh);
    Ok(Ok(Vec::new()))
}

/// Go `applyExchangeTablePartition` (`builder.go:322-395`): the non-partitioned
/// table and the partitioned table are each one `applyTableUpdate`, and the
/// partitioned table's IDs are answered first (`append(ptIDs, ntIDs...)`).
/// The auto-ID levelling it ends with (`updateAutoIDForExchangePartition`)
/// is a store write this reader does not make: it is the same idempotent
/// write every node performs, the DDL-running Go nodes included, and this
/// catalog keeps no allocator state for it to feed. A full load would not
/// make it either.
fn exchange_table_partition<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &mut ClusterCatalog,
    diff: &SchemaDiff,
) -> AppliedDiff {
    let first_affected = |diff: &SchemaDiff| {
        diff.affected_options.iter_handles().next().map(|affected| {
            let affected = affected.expect("nil affected option in exchange-partition schema diff");
            let affected = affected.read();
            (
                affected.schema_id,
                affected.table_id,
                affected.old_schema_id,
            )
        })
    };
    // Not yet public: the diff names one table in place, and the partitioned
    // table only if the first option carries its database.
    if diff.old_table_id == diff.table_id && diff.old_schema_id == diff.schema_id {
        let nt_ids = match apply_table_update(snapshot, catalog, diff)? {
            Ok(ids) => ids,
            Err(reason) => return Ok(Err(reason)),
        };
        let Some((_, pt_id, pt_schema_id)) = first_affected(diff) else {
            return Ok(Ok(nt_ids));
        };
        if pt_schema_id == 0 {
            return Ok(Ok(nt_ids));
        }
        let pt_diff = SchemaDiff {
            action_type: diff.action_type,
            version: diff.version,
            table_id: pt_id,
            schema_id: pt_schema_id,
            old_table_id: pt_id,
            old_schema_id: pt_schema_id,
            ..SchemaDiff::default()
        };
        return Ok(match apply_table_update(snapshot, catalog, &pt_diff)? {
            Ok(mut pt_ids) => {
                pt_ids.extend(nt_ids);
                Ok(pt_ids)
            }
            Err(reason) => Err(reason),
        });
    }
    let nt_schema_id = diff.old_schema_id;
    let nt_id = diff.old_table_id;
    let mut pt_schema_id = diff.schema_id;
    let mut pt_id = diff.table_id;
    let part_id = diff.table_id;
    if let Some((affected_schema_id, affected_table_id, _)) = first_affected(diff) {
        pt_id = affected_table_id;
        if affected_schema_id != 0 {
            pt_schema_id = affected_schema_id;
        }
    }
    // The normal table first; it takes the partition's ID when the diff
    // carries the partitioned table's ID in its first option.
    let mut current = SchemaDiff {
        action_type: diff.action_type,
        version: diff.version,
        table_id: nt_id,
        schema_id: nt_schema_id,
        ..SchemaDiff::default()
    };
    if pt_id != part_id {
        current.table_id = part_id;
        current.old_table_id = nt_id;
        current.old_schema_id = nt_schema_id;
    }
    let nt_ids = match apply_table_update(snapshot, catalog, &current)? {
        Ok(ids) => ids,
        Err(reason) => return Ok(Err(reason)),
    };
    // Then the partitioned table, re-read whole.
    current.table_id = pt_id;
    current.schema_id = pt_schema_id;
    current.old_table_id = pt_id;
    current.old_schema_id = pt_schema_id;
    Ok(match apply_table_update(snapshot, catalog, &current)? {
        Ok(mut pt_ids) => {
            pt_ids.extend(nt_ids);
            Ok(pt_ids)
        }
        Err(reason) => Err(reason),
    })
}

/// Go `equalPlacementPolicy` (`builder.go:122-130`).
fn equal_placement_policy(
    left: Option<&tidb_model::GoShared<tidb_model::placement::PolicyRefInfo>>,
    right: Option<&tidb_model::GoShared<tidb_model::placement::PolicyRefInfo>>,
) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => {
            let (left, right) = (left.read(), right.read());
            left.id == right.id && left.name.lowercase() == right.name.lowercase()
        }
        _ => false,
    }
}

/// Go `applyRefreshMeta` (`builder.go:137-258`), BR's PITR path: each diff
/// names one database or one table, and the store decides whether it is
/// dropped, created or updated. A database operation answers no table.
fn refresh_meta<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &mut ClusterCatalog,
    diff: &SchemaDiff,
) -> AppliedDiff {
    let (schema_id, table_id) = (diff.schema_id, diff.table_id);
    if table_id == 0 {
        let Some(fresh) = read_database(snapshot, schema_id)? else {
            // Gone from the store: drop it (absent is a no-op, as in Go).
            catalog.databases.retain(|db| db.info.id != schema_id);
            return Ok(Ok(Vec::new()));
        };
        match catalog
            .databases
            .iter_mut()
            .find(|db| db.info.id == schema_id)
        {
            None => catalog.databases.push(LoadedDatabase {
                info: fresh,
                tables: Vec::new(),
            }),
            Some(database) => {
                let loaded = &mut database.info;
                if loaded.charset != fresh.charset || loaded.collate != fresh.collate {
                    loaded.charset = fresh.charset;
                    loaded.collate = fresh.collate;
                }
                if !equal_placement_policy(
                    loaded.placement_policy_ref.as_ref(),
                    fresh.placement_policy_ref.as_ref(),
                ) {
                    loaded.placement_policy_ref = fresh.placement_policy_ref;
                }
            }
        }
        return Ok(Ok(Vec::new()));
    }
    // A table under a database this catalog does not have: the database is
    // gone, so its tables are too.
    if !catalog.databases.iter().any(|db| db.info.id == schema_id) {
        return Ok(Ok(Vec::new()));
    }
    let stored = snapshot
        .get(&key::table_kv_key(schema_id, table_id))?
        .is_some();
    let synthetic = SchemaDiff {
        version: diff.version,
        action_type: if stored {
            ActionType::ACTION_CREATE_TABLE
        } else {
            ActionType::ACTION_DROP_TABLE
        },
        schema_id,
        table_id,
        old_schema_id: schema_id,
        old_table_id: table_id,
        is_refresh_meta: true,
        ..SchemaDiff::default()
    };
    // Present: Go `applyDefaultAction` with `AffectedOpts` unset; absent: Go
    // `applyDropTableOrPartition`. Both are one `applyTableUpdate`.
    apply_table_update(snapshot, catalog, &synthetic)
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

/// Go `applyCreateTable`'s catalog effect: the table read from the store
/// replaces any loaded copy with the same ID in that database. Answers the
/// created table's affected IDs.
fn create_table<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &mut ClusterCatalog,
    version: i64,
    db_id: i64,
    table_id: i64,
) -> AppliedDiff {
    let Some(database) = catalog.databases.iter_mut().find(|db| db.info.id == db_id) else {
        return Ok(Err(missing(version, format!("unknown database {db_id}"))));
    };
    let Some(stored) = snapshot.get(&key::table_kv_key(db_id, table_id))? else {
        return Ok(Err(missing(
            version,
            format!("table {table_id} in database {db_id}, which the snapshot does not store"),
        )));
    };
    let mut table = value::parse_table_info(&stored, db_id)
        .map_err(|error| ClusterCatalogError::Decode(format!("TableInfo {table_id}: {error}")))?;
    normalize_diff_loaded_table_info(&mut table);
    let mut ids = Vec::new();
    affected_ids(&table, &mut ids);
    database.tables.retain(|existing| existing.id != table.id);
    database.tables.push(table);
    Ok(Ok(ids))
}

/// Go `applyDropTable`'s catalog effect; an absent table is a no-op that
/// answers nothing (`builder.go:969-971`), an absent database is Go's
/// `ErrDatabaseNotExists`.
fn drop_table(
    catalog: &mut ClusterCatalog,
    version: i64,
    db_id: i64,
    table_id: i64,
) -> AppliedDiff {
    let Some(database) = catalog.databases.iter_mut().find(|db| db.info.id == db_id) else {
        return Ok(Err(missing(version, format!("unknown database {db_id}"))));
    };
    let mut ids = Vec::new();
    if let Some(index) = database
        .tables
        .iter()
        .position(|existing| existing.id == table_id)
    {
        let dropped = database.tables.remove(index);
        affected_ids(&dropped, &mut ids);
    }
    Ok(Ok(ids))
}
