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
        ActionType::ACTION_CREATE_TABLE => {
            if let Err(reason) =
                create_table(snapshot, catalog, version, diff.schema_id, diff.table_id)?
            {
                return Ok(Err(reason));
            }
        }
        ActionType::ACTION_CREATE_TABLES => {
            // Go `applyCreateTables`: the diff's own table ID is unset and
            // every created table is listed as an affected option.
            for affected in &diff.affected_options {
                if let Err(reason) = create_table(
                    snapshot,
                    catalog,
                    version,
                    affected.schema_id,
                    affected.table_id,
                )? {
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
