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

//! Go `ShowExec` (`executor/show.go`, `show_stats.go`,
//! `show_placement.go`): `fetchAll` and the fetchers it dispatches to, as
//! provided methods over the session primitives Go's executor reads through
//! `sctx` and its domain.

use tidb_stats_handle::catalog_statistics::CatalogStatistics;
use tidb_datatype::Datum;
use tidb_stats_handle_autoanalyze::analyze_jobs::AnalyzeStatusProvider;
use tidb_stats_handle_usage_predicatecolumn::cluster_predicate_column::ColumnStatsUsageProvider;
use tidb_hack::{GoCaseCompare, GoToLower};
use tidb_hack::go_to_lower;

use super::placement::*;
use super::stats::*;
use crate::DriverError;

/// Go `ShowExec`.
pub trait ShowExec: crate::simple::account_password::AccountPasswordExec {
    /// The statement's infoschema under a name scope: with or without the
    /// session's LOCAL temporary tables (Go `preprocess.go` detaching them
    /// for `SHOW CREATE VIEW` / `SEQUENCE`).
    fn with_catalog_scope<T>(
        &mut self,
        include_local: bool,
        body: impl FnOnce(&mut crate::Catalog) -> Result<T, DriverError>,
    ) -> Result<T, DriverError>;
    /// Go `GlobalBindingHandle`'s bindings, loaded from the domain's cache
    /// or the binding storage.
    fn load_global_bindings(
        &mut self,
    ) -> Result<tidb_bindinfo::binding::SessionBindings, DriverError>;
    /// Go `SessionBindingHandle`: the session's own bindings.
    fn session_bindings_mut(&mut self) -> &mut tidb_bindinfo::binding::SessionBindings;
    /// Go `e.Ctx().Txn(true)`'s start timestamp (`fetchShowMasterStatus`).
    fn activated_txn_start_ts(&mut self) -> i64;
    /// The account table and the session's identity, `None` for a session
    /// with no privilege manager.
    fn privilege_context(&self) -> Option<(&tidb_privilege::PrivilegeRegistry, &str, &str)>;
    /// Go `RequestVerification` with any of `mask`'s privileges.
    fn has_any_scoped_privilege(&self, database: &str, table: &str, mask: u64) -> bool;
    /// Go `DBIsVisible`.
    fn database_is_visible(&self, database: &str) -> bool;
    /// The schema visibility the information_schema readers filter by.
    fn schema_visibility(&self) -> crate::infoschema_reader::SchemaVisibility;
    /// Go `buildShow`'s 1044 gate on the schema.
    fn require_visible_database(&self, database: &str) -> Result<(), DriverError>;
    /// Go `ErrNoDB` for an unqualified name with no current database.
    fn require_current_database(&self) -> Result<&str, DriverError>;
    /// Go `buildShow`'s SHOW CREATE privilege check.
    fn require_show_create_privilege(&self, database: &str, table_name: &str)
        -> Result<(), DriverError>;
    /// Go `buildShow`'s SELECT check on the `mysql` stats table.
    fn require_show_stats_privilege(&self, table: &str) -> Result<(), DriverError>;
    /// Go `infoschema`'s `TableSizeStats` image: the statement's catalog
    /// with the table-size cache folded in.
    fn table_storage_statistics_catalog(
        &mut self,
        needs_column_lengths: bool,
    ) -> Result<crate::Catalog, DriverError>;
    /// Go `statstypes.ColumnStatsUsage` storage reader.
    fn column_stats_usage_provider(&self) -> Option<std::sync::Arc<dyn ColumnStatsUsageProvider>>;
    /// Go's `mysql.analyze_jobs` reader.
    fn analyze_status_provider(&self) -> Option<std::sync::Arc<dyn AnalyzeStatusProvider>>;
    /// Go `fetchShowStatus` over `variable.GetStatusVars`.
    fn show_status_rows(&self, global: bool) -> Result<Vec<Vec<Datum>>, DriverError>;
    /// Go `fetchShowWarnings`.
    fn warning_rows(&self, count_only: bool, errors_only: bool) -> Vec<Vec<Datum>>;
    /// What the instance-wide `information_schema` readers read of this
    /// session.
    fn viewer_state(&self) -> crate::memtable_reader::ViewerState;
    /// Go `SessionManager`: this server's process registry.
    fn process_registry(&self) -> Option<tidb_sessmgr::ProcessRegistry>;
    /// Go `fetchShowSessionStates` over `EncodeStates`.
    fn show_session_states_rows(&self) -> Result<Vec<Vec<Datum>>, DriverError>;
    /// Go `fetchShowClusterConfigs`.
    fn cluster_config_table_rows(
        &mut self,
        filters: &[tidb_planner::memtable_predicate_extractor::ClusterTableFilter],
    ) -> Result<Vec<Vec<Datum>>, DriverError>;

    /// Go `buildShow`'s selections over a virtual SHOW result: `LIKE`
    /// matches the first column and `WHERE` is a `LogicalSelection` over the
    /// result's own columns. A `WHERE` that reads a subquery is planned as
    /// that selection, which builds the subquery the way any SELECT's does
    /// (`field IN (SELECT ...)`, `field < ALL (SELECT ...)`); any other
    /// predicate evaluates row by row against the same columns.
    /// Go `ShowExec.fetchShowAffinity`: one row per table (`table` level) or
    /// partition (`partition` level) carrying an AFFINITY, with its PD group's
    /// state from `affinity.GetAllGroupStates` -- NULL where PD holds no such
    /// group. The rows follow `ListTablesWithSpecialAttribute` over infoschema
    /// v2: schema name, then table id, both descending, each table the
    /// extractor admits by its lowercased name.
    fn show_affinity_rows(
        &mut self,
        extractor: crate::show::Extractor<'_>,
    ) -> Result<Vec<Vec<Datum>>, DriverError> {
        struct AffinityRow {
            db_lower: String,
            table_id: i64,
            db: String,
            table: String,
            partition: Option<String>,
            group_id: String,
        }
        let mut infos = Vec::new();
        {
            let catalog = self.lock_catalog()?;
            for database in catalog.database_names() {
                for name in catalog.table_names(&database).unwrap_or_default() {
                    let Some(crate::TableEntry::Kv(table)) =
                        catalog.table_in(&database, &name)
                    else {
                        continue;
                    };
                    let Some(affinity) = table.affinity() else {
                        continue;
                    };
                    let lower = go_to_lower(&table.name());
                    if !crate::show::extractor_admits(extractor, &lower, &lower) {
                        continue;
                    }
                    let row = |partition: Option<String>, group_id: String| AffinityRow {
                        db_lower: go_to_lower(&database),
                        table_id: table.table_id(),
                        db: database.clone(),
                        table: table.name(),
                        partition,
                        group_id,
                    };
                    match affinity.level.as_str() {
                        "table" => infos.push(row(
                            None,
                            tidb_ddl::ddl_affinity::get_table_affinity_group_id(table.table_id()),
                        )),
                        "partition" => {
                            for definition in
                                table.partition().map_or(&[][..], |spec| &spec.definitions)
                            {
                                infos.push(row(
                                    Some(definition.name.clone()),
                                    tidb_ddl::ddl_affinity::get_partition_affinity_group_id(
                                        table.table_id(),
                                        definition.id,
                                    ),
                                ));
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
        // A stable sort keeps a table's partitions in definition order.
        infos.sort_by(|left, right| {
            right
                .db_lower
                .cmp(&left.db_lower)
                .then(right.table_id.cmp(&left.table_id))
        });
        let states = tidb_domain_affinity::get_all_group_states().map_err(|error| {
            DriverError::Mysql(crate::MysqlError::unknown(error.to_string()))
        })?;
        let rows = infos
            .into_iter()
            .map(|info| {
                let state = states.get(&info.group_id);
                let text = |text: &str| Datum::Bytes(text.as_bytes().to_vec());
                vec![
                    text(&info.db),
                    text(&info.table),
                    info.partition.as_deref().map_or(Datum::Null, text),
                    state
                        .filter(|state| state.group.leader_store_id != 0)
                        .map_or(Datum::Null, |state| {
                            Datum::UInt(state.group.leader_store_id)
                        }),
                    state
                        .filter(|state| !state.group.voter_store_ids.is_empty())
                        .map_or(Datum::Null, |state| {
                            text(
                                &state
                                    .group
                                    .voter_store_ids
                                    .iter()
                                    .map(u64::to_string)
                                    .collect::<Vec<_>>()
                                    .join(","),
                            )
                        }),
                    state.map_or(Datum::Null, |state| {
                        text(match state.phase.as_str() {
                            "pending" => "Pending",
                            "preparing" => "Preparing",
                            "stable" => "Stable",
                            other => other,
                        })
                    }),
                    state.map_or(Datum::Null, |state| Datum::Int(state.region_count)),
                    state.map_or(Datum::Null, |state| Datum::Int(state.affinity_region_count)),
                ]
            })
            .collect();
        Ok(rows)
    }

    /// Go `ShowExec.fetchShowCreate{Database,ResourceGroup,PlacementPolicy,
    /// Table,Sequence,View}`: the one row a SHOW CREATE answers.
    fn show_create_rows(
        &mut self,
        contents: &tidb_planner_core_operator_logicalop::ShowContents,
    ) -> Result<Vec<Vec<Datum>>, DriverError> {
        if contents.tp == tidb_ast::ShowStmtType::CreateDatabase {
            let database = &contents.db_name;
            let (reported, charset, placement) = self.with_catalog_mut(|catalog| {
                let (reported, charset) =
                    catalog.database_definition(database).ok_or_else(|| {
                        DriverError::from(
                            tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS
                                .generate_with_stack_by_args(&[(database.clone()).into()]),
                        )
                    })?;
                let placement = catalog
                    .database_placement_policy(database)
                    .map(|reference| reference.name.original().to_owned());
                Ok((reported, charset, placement))
            })?;
            return Ok(crate::show::create_database::rows(
                reported,
                charset,
                placement.as_deref(),
                contents.if_not_exists,
                self.session_vars().sql_mode().has_ansi_quotes_mode(),
            ));
        }
        // Go `fetchShowCreateResourceGroup`: the group by name
        // (`ErrResourceGroupNotExists` when absent), reported under
        // the name as written beside `CREATE RESOURCE GROUP`
        // followed by its settings.
        if contents.tp == tidb_ast::ShowStmtType::CreateResourceGroup {
            let group_name = &contents.resource_group_name;
            let text = self.with_catalog_mut(|catalog| {
                let group = catalog.resource_group(group_name).ok_or_else(|| {
                    DriverError::Mysql(tidb_terror::MysqlError::from_terror(
                        &tidb_infoschema::infoschema_errors::ERR_RESOURCE_GROUP_NOT_EXISTS
                            .generate_with_stack_by_args(&[group_name.into()]),
                    ))
                })?;
                let settings = group
                    .settings
                    .as_ref()
                    .map(|settings| settings.to_string())
                    .unwrap_or_default();
                Ok(format!(
                    "CREATE RESOURCE GROUP `{}` {settings}",
                    group.name.original()
                ))
            })?;
            return Ok(vec![vec![
                Datum::Bytes(group_name.clone().into_bytes()),
                Datum::Bytes(text.into_bytes()),
            ]]);
        }
        // Go `fetchShowCreatePlacementPolicy` (`executor/show.go:1774`)
        // looks the policy up and reports `ErrPlacementPolicyNotExists`
        // (8239) for a name that is not there. The row is the policy's
        // ORIGINAL-cased name beside
        // `ConstructResultOfShowCreatePlacementPolicy` (`:1742`).
        if contents.tp == tidb_ast::ShowStmtType::CreatePlacementPolicy {
            let policy_name = &contents.db_name;
            let (reported, clause) = self.with_catalog_mut(|catalog| {
                catalog
                    .policy(policy_name)
                    .map(|policy| {
                        let settings = policy
                            .placement_settings
                            .as_ref()
                            .map(|settings| settings.read().to_clause())
                            .unwrap_or_default();
                        (policy.name.original().to_owned(), settings)
                    })
                    .ok_or_else(|| {
                        DriverError::from(
                            tidb_infoschema::infoschema_errors::ERR_PLACEMENT_POLICY_NOT_EXISTS
                                .generate_with_stack_by_args(&[policy_name.clone().into()]),
                        )
                    })
            })?;
            return Ok(crate::show::create_placement_policy::rows(&reported, &clause));
        }
        // `SHOW CREATE SEQUENCE` and `SHOW CREATE TABLE` take the
        // SAME path: Go's `buildShow` picks the column names from
        // whether the session-visible object IS a sequence, not from
        // the keyword written, and `fetchShowCreateTable` prints a
        // sequence's own DDL (`constructResultOfShowCreateTable`'s
        // `IsSequence` arm). `SHOW CREATE SEQUENCE` reads the
        // persistent object, which a local temporary table of the
        // same name does not shadow, and refuses anything that is not
        // a sequence (`fetchShowCreateSequence`'s ErrWrongObject).
        let want_view = match contents.tp {
            tidb_ast::ShowStmtType::CreateTable | tidb_ast::ShowStmtType::CreateSequence => {
                false
            }
            tidb_ast::ShowStmtType::CreateView => true,
            other => {
                return Err(DriverError::unsupported(format!(
                    "SHOW {other:?} has no SHOW CREATE row"
                )))
            }
        };
        let want_sequence = contents.tp == tidb_ast::ShowStmtType::CreateSequence;
        let (database, table_name) =
            (contents.db_name.clone(), contents.table_name.clone().unwrap_or_default());
        self.require_show_create_privilege(&database, &table_name)?;
        let ctx = self.statement_context(false);
        // A view answers either spelling with the same row, which
        // is Go's own behaviour; only `SHOW CREATE VIEW` on a base
        // table is refused.
        let ansi_quotes = self.session_vars().sql_mode().has_ansi_quotes_mode();
        // Go `preprocess.go` detaches the LOCAL temporary tables for
        // `SHOW CREATE VIEW` as for `SHOW CREATE SEQUENCE`: a
        // temporary table of the same name does not shadow either.
        let include_local = !want_sequence && !want_view;
        let shown = self.with_catalog_scope(include_local, |catalog| {
            let Some(entry) = catalog.table_in(&database, &table_name) else {
                return Err(DriverError::from(
                    tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                        .generate_with_stack_by_args(&[(&database).into(), (&table_name).into()]),
                ));
            };
            match entry {
                // Go prints the object's stored name
                // (`TableInfo.Name.O`), not the name as written.
                crate::TableEntry::View(view) => Ok((
                    crate::show::create::show_create_view_text(view, ansi_quotes),
                    view.name.clone(),
                    Some((
                        view.character_set_client.clone(),
                        view.collation_connection.clone(),
                    )),
                )),
                _ if want_view => Err(DriverError::from(
                    tidb_infoschema::infoschema_errors::ERR_WRONG_OBJECT.generate_with_stack_by_args(
                        &[(&database).into(), (&table_name).into(), "VIEW".into()],
                    ),
                )),
                crate::TableEntry::Sequence(sequence) => Ok((
                    crate::show_create_sequence(sequence),
                    sequence.name.clone(),
                    None,
                )),
                _ if want_sequence => Err(DriverError::from(
                    tidb_infoschema::infoschema_errors::ERR_WRONG_OBJECT.generate_with_stack_by_args(
                        &[(&database).into(), (&table_name).into(), "SEQUENCE".into()],
                    ),
                )),
                crate::TableEntry::Kv(table) => {
                    let stored = match table.name() {
                        name if name.is_empty() => table_name.clone(),
                        name => name,
                    };
                    Ok((
                        crate::show::create::show_create_table_text(&database, &stored, table, &ctx)?,
                        stored,
                        None,
                    ))
                }
                crate::TableEntry::Mem(_) => Err(DriverError::unsupported(
                    "SHOW CREATE TABLE needs a storage-backed table",
                )),
            }
        });
        let (text, reported, view_charset) = shown?;
        // Go's view form carries the session's character set and
        // collation; `buildShowSchema` names the columns from the
        // object, not the keyword written.
        if let Some((character_set_client, collation_connection)) = view_charset {
            return Ok(vec![vec![
                Datum::Bytes(reported.into_bytes()),
                Datum::Bytes(text.into_bytes()),
                Datum::Bytes(character_set_client.into_bytes()),
                Datum::Bytes(collation_connection.into_bytes()),
            ]]);
        }
        Ok(vec![vec![
            Datum::Bytes(reported.into_bytes()),
            Datum::Bytes(text.into_bytes()),
        ]])
    }

    /// Go `ShowNextRowIDExec.Next`: one row per allocator of the table, read
    /// through the domain's infoschema, which holds no local temporary table.
    fn show_next_row_id_rows(&mut self, path: &[String]) -> Result<Vec<Vec<Datum>>, DriverError> {
        let [database, table_name] = path else {
            return Err(DriverError::unsupported("SHOW TABLE NEXT_ROW_ID without its table"));
        };
        let ids = self.with_catalog_mut(|catalog| {
            let Some(entry) = catalog.persistent_table_in(database, table_name) else {
                return Err(DriverError::from(
                    tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                        .generate_with_stack_by_args(&[(&database).into(), (&table_name).into()]),
                ));
            };
            let crate::TableEntry::Kv(table) = entry else {
                return Err(DriverError::unsupported(
                    "SHOW TABLE NEXT_ROW_ID needs a storage-backed table",
                ));
            };
            table
                .next_global_row_ids()
                .map_err(|error| DriverError::from(tidb_terror::MysqlError::unknown(error.0)))
        })?;
        Ok(ids
            .into_iter()
            .map(|(column, next, id_type)| {
                vec![
                    Datum::Bytes(database.as_bytes().to_vec()),
                    Datum::Bytes(table_name.as_bytes().to_vec()),
                    Datum::Bytes(column.into_bytes()),
                    Datum::Int(next),
                    Datum::Bytes(id_type.as_bytes().to_vec()),
                ]
            })
            .collect())
    }

    /// Go `ShowExec.fetchAll`: the rows of one planned SHOW, by its type.
    /// Only the kinds [`is_planned_show`] lists reach it.
    fn fetch_show(
        &mut self,
        leaf: &tidb_planner_core_operator_physicalop::PhysicalShow,
    ) -> Result<Vec<Vec<Datum>>, DriverError> {
        let stats_handle = self.stats_handle();
        use tidb_ast::ShowStmtType as T;
        let contents = &leaf.contents;
        let extractor = base_extractor(leaf);
                let strings = |names: Vec<String>| -> Vec<Vec<Datum>> {
            names.into_iter().map(|name| vec![Datum::new_string(name)]).collect()
        };
        Ok(match contents.tp {
            T::Charset => crate::show::fetch_show_charset(self),
            T::Collation => crate::show::fetch_show_collation(self, extractor),
            T::Databases => {
                let domain = self.domain_info_schema();
                let catalog = domain.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                strings(crate::show::fetch_show_databases(self, &catalog, extractor))
            }
            T::Engines => crate::show::fetch_show_engines(),
            // Go `fetchShowProcedureStatus`, `fetchShowTriggers` and the
            // `ShowEvents` / `ShowProfiles` / `ShowFunctionStatus` arms
            // answer nothing; `fetchShowOpenTables` answers the empty set;
            // `fetchShowPlugins` lists an empty `plugin.GetAll()`; the BRIE
            // listings read a queue this server never fills.
            // Go `fetchShowImportJobs` / `fetchShowImportGroups` read the
            // dist-task framework, which runs no IMPORT job here.
            T::ImportJobs | T::ImportGroups => Vec::new(),
            T::ProcedureStatus
            | T::FunctionStatus
            | T::Triggers
            | T::Events
            | T::Profiles
            | T::OpenTables
            | T::Plugins
            | T::Backups
            | T::Restores => Vec::new(),
            T::Tables => {
                let domain = self.domain_info_schema();
                let catalog = domain.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                crate::show::fetch_show_tables(
                    self,
                    &catalog,
                    &contents.db_name,
                    contents.full,
                    extractor,
                )?
                .into_iter()
                .map(|(name, table_type)| {
                    let mut row = vec![Datum::new_string(name)];
                    row.extend(table_type.map(Datum::new_string));
                    row
                })
                .collect()
            }
            T::Variables => {
                crate::show::fetch_show_variables(self, contents.global_scope, extractor)
            }
            // Go `executorBuilder.buildShow`: "show master status need start
            // ts", then `fetchShowMasterStatus`.
            T::MasterStatus | T::BinlogStatus => {
                super::master_status_rows(self.activated_txn_start_ts())
            }
            T::Privileges => super::show_privileges_rows(),
            T::Status => self.show_status_rows(contents.global_scope)?,
            T::CreateTable
            | T::CreateView
            | T::CreateSequence
            | T::CreateDatabase
            | T::CreatePlacementPolicy
            | T::CreateResourceGroup => self.show_create_rows(contents)?,
            T::Grants => self.show_grants_rows(contents.user.as_ref(), &contents.roles)?,
            // Pinned Go `ShowExec.fetchAll` keeps the grammar entry but
            // rejects execution because extended statistics was removed.
            T::StatsExtended => {
                return Err(DriverError::unsupported(
                    "Extended statistics feature has been removed",
                ))
            }
            // Go `ShowExec.fetchShowStatsMeta` (`executor/show_stats.go:36`).
            T::StatsMeta => {
                self.require_show_stats_privilege("stats_meta")?;
                self.stats_meta_rows(leaf.extractor.as_deref())?
            }
            T::StatsHealthy => self.stats_healthy_rows(leaf.extractor.as_deref())?,
            T::ColumnStatsUsage => self.column_stats_usage_rows()?,
            T::HistogramsInFlight => {
                let count =
                    self.with_catalog_mut(|catalog| Ok(catalog.clean_needed_statistics_items(&stats_handle)))?;
                vec![crate::show_stats::histograms_in_flight_row(count)]
            }
            T::AnalyzeStatus => self.analyze_status_rows()?,
            // Go `fetchShowBind` (LIKE is a case-sensitive selection on
            // `Original_sql`: Go gives BINDINGS no extractor).
            T::Bindings => self.show_bindings_rows(contents.global_scope)?,
            T::BindingCacheStatus => self.binding_cache_status_rows()?,
            // Go `ShowExec.fetchShowProcessList`: one row per live connection
            // of this server, read from the session manager.
            T::ProcessList => self.process_list_rows(contents.full),
            T::SessionStates => self.show_session_states_rows()?,
            // Go `fetchShowClusterConfigs`; Go gives CONFIG no extractor, so
            // LIKE is the plan's selection on `Type`.
            T::Config => self.cluster_config_table_rows(&[Default::default()])?,
            T::Affinity => self.show_affinity_rows(extractor)?,
            // Go `fetchShowPlacement` and its FOR DATABASE / TABLE /
            // PARTITION and LABELS forms (`executor/show_placement.go`); Go
            // gives them no extractor, so LIKE is the plan's selection.
            T::Placement
            | T::PlacementLabels
            | T::PlacementForDatabase
            | T::PlacementForTable
            | T::PlacementForPartition => self.placement_rows(contents)?,
            // Go `fetchShowTableRegions`.
            T::Regions => {
                let table_name = contents.table_name.clone().unwrap_or_default();
                let store = self.region_store();
                self.with_catalog_mut(|catalog| {
                    crate::show_regions::show_table_regions(
                        catalog,
                        store.as_ref(),
                        &contents.db_name,
                        &table_name,
                        &contents.table_partitions,
                        (!contents.index_name.is_empty()).then_some(contents.index_name.as_str()),
                    )
                })?
            }
            // Go `fetchShowDistributions`, which needs PD's HTTP client: the
            // table and partitions are checked, then PD's error answers.
            T::Distributions => {
                let table_name = contents.table_name.clone().unwrap_or_default();
                self.with_catalog_mut(|catalog| {
                    crate::show_regions::show_table_distributions(
                        catalog,
                        &contents.db_name,
                        &table_name,
                        &contents.table_partitions,
                    )
                })?;
                Vec::new()
            }
            T::StatsTopN => self.stats_topn_rows()?,
            T::StatsBuckets => {
                self.require_show_stats_privilege("stats_buckets")?;
                self.stats_buckets_rows()?
            }
            T::StatsHistograms => {
                self.require_show_stats_privilege("stats_histograms")?;
                self.stats_histograms_rows()?
            }
            T::StatsLocked => {
                self.require_show_stats_privilege("stats_table_locked")?;
                self.stats_locked_rows()?
            }
            T::CreateUser => {
                let spec = contents
                    .user
                    .as_ref()
                    .ok_or_else(|| DriverError::unsupported("SHOW CREATE USER without a user"))?;
                self.show_create_user_rows(spec)?
            }
            // Go `fetchShowIndex`: one row per index COLUMN, ordered
            // with the clustered primary key first, then the table's own
            // indexes in definition order.
            //
            // Cardinality is each column's NDV from the table's stats and
            // Sub_part the declared prefix length; Packed is NULL and
            // Comment empty, as Go writes them; Expression is NULL when no
            // expression key part exists; Global is the stored
            // `IndexInfo.Global` flag.
            T::Index => {
                let database = contents.db_name.clone();
                let table_name = contents.table_name.clone().unwrap_or_default();
                self.with_catalog_mut(|catalog| {
                    let Some(entry) = catalog.table_in(&database, &table_name) else {
                        return Err(DriverError::from(
                            tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                                .generate_with_stack_by_args(&[
                                    (&database).into(),
                                    (&table_name).into(),
                                ]),
                        ));
                    };
                    let table = match entry {
                        crate::TableEntry::Kv(table) => table,
                        // A view's `TableInfo` carries no indexes (Go answers
                        // the header and no rows).
                        crate::TableEntry::View(_)
                        | crate::TableEntry::Sequence(_) => return Ok(Vec::new()),
                        _ => {
                            return Err(DriverError::unsupported(
                                "SHOW INDEX needs a storage-backed table",
                            ));
                        }
                    };
                    // Go `GetPhysicalTableStats(tb.Meta().ID, ...)`.
                    let statistics = catalog.table_statistics_in(&stats_handle, table.table_id());
                    Ok(crate::show::index::show_index_rows(&table_name, table, |column_id| {
                        statistics
                            .as_ref()
                            .and_then(|statistics| statistics.hist_coll.get_column(column_id))
                            .map_or(0, |column| {
                                column
                                    .read()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                                    .histogram
                                    .ndv
                            })
                    }))
                })?
            }
            // Go `fetchShowColumns`.
            T::Columns => {
                let table = [
                    contents.db_name.clone(),
                    contents.table_name.clone().unwrap_or_default(),
                ];
                self.show_column_rows(
                    &contents.db_name,
                    &table,
                    extractor,
                    contents.full,
                    contents.extended,
                )?
            }
            // Go `fetchShowTableStatus`: the 1044 gate SHOW TABLES applies,
            // the unknown schema, then its restricted SQL over
            // `information_schema.tables` for the schema, ordered by table
            // name (so every size and count is the TABLES reader's), each
            // table the extractor admits by its lowercased name.
            T::TableStatus => {
                let database = contents.db_name.clone();
                self.require_visible_database(&database)?;
                let zone = self.session_time_zone();
                let visibility = self.schema_visibility();
                let catalog = self.table_storage_statistics_catalog(true)?;
                if catalog.table_names(&database).is_none() {
                    return Err(DriverError::from(
                        tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS
                            .generate_with_stack_by_args(&[database.into()]),
                    ));
                }
                crate::infoschema_reader::show_table_status_rows(&catalog, &visibility, &zone, &database)
                    .into_iter()
                    .filter(|row| {
                        let name = go_to_lower(tidb_datatype::datum_text::sysvar_datum_text(&row[0]).unwrap_or_default());
                        crate::show::extractor_admits(extractor, &name, &name)
                    })
                    .collect()
            }
            T::Warnings | T::Errors => {
                self.warning_rows(contents.count_warnings_or_errors, contents.tp == T::Errors)
            }
            // Go `fetchShowBuiltins` over `expression.GetBuiltinList`.
            T::Builtins => strings(crate::builtin_list()),
            other => {
                return Err(DriverError::unsupported(format!(
                    "SHOW {other:?} is not planned"
                )))
            }
        })
    }

    /// Go `ShowExec.fetchShowColumns`: one row per column of the table (its
    /// hidden ones too with `extended`), the `FULL` shape with `full`, each
    /// column the extractor admits by its lowercased name.
    fn show_column_rows(
        &mut self,
        database: &str,
        table_path: &[String],
        extractor: crate::show::Extractor<'_>,
        full: bool,
        extended: bool,
    ) -> Result<Vec<Vec<Datum>>, DriverError> {
        let admits = |name: &str| {
            let lower = go_to_lower(name);
            crate::show::extractor_admits(extractor, &lower, &lower)
        };
        // A `db.tbl` path names its own schema, as everywhere else.
        let (database, table_name) = match table_path {
            [name] => (database.to_owned(), name.clone()),
            [db, name] => (db.clone(), name.clone()),
            _ => return Err(DriverError::unsupported("empty table name")),
        };
        // Go `fetchShowColumns` asks for any column privilege once the table
        // resolves, and refuses as a denied SELECT on the table as stored.
        let stored_name = self
            .lock_catalog()?
            .table_in(&database, &table_name)
            .map(|entry| match entry {
                crate::TableEntry::Kv(table) if !table.name().is_empty() => {
                    table.name().to_owned()
                }
                crate::TableEntry::View(view) => view.name.clone(),
                _ => table_name.clone(),
            });
        if let (Some(stored_name), Some((_, user, host))) = (stored_name, self.privilege_context())
        {
            if !self.has_any_scoped_privilege(
                &database,
                &table_name,
                tidb_privilege::all_column_privs_mask(),
            ) {
                return Err(DriverError::from(
                    tidb_plannererrors::ERR_TABLEACCESS_DENIED.generate_with_stack_by_args(
                        &[
                            "SELECT".into(),
                            user.to_owned().into(),
                            host.to_owned().into(),
                            stored_name.into(),
                        ],
                    ),
                ));
            }
        }
        let ctx = self.statement_context(false);
        let rows = self.with_catalog_mut(|catalog| {
            let Some(entry) = catalog.table_in(&database, &table_name) else {
                return Err(DriverError::from(
                    tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                        .generate_with_stack_by_args(&[(&database).into(), (&table_name).into()]),
                ));
            };
            if let crate::TableEntry::View(view) = entry {
                // Go re-plans the body here (`tryFillViewColumnType`), so the
                // types reported are the ones the base tables have now, and a
                // body that no longer resolves fails the statement with its
                // own error rather than with ErrViewInvalid.
                let view = view.clone();
                let columns = crate::view_column_list(&view, &database, catalog, &ctx)?;
                return Ok(columns
                    .iter()
                    .filter(|(candidate, _)| admits(candidate))
                    .map(|(name, field_type)| crate::show::columns::view_column_description(name, field_type, full))
                    .collect::<Vec<_>>());
            }
            let crate::TableEntry::Kv(table) = entry else {
                return Err(DriverError::unsupported(
                    "SHOW COLUMNS needs a storage-backed table",
                ));
            };
            // `EXTENDED` adds the hidden columns MySQL keeps internally
            // (`tb.Cols()` rather than `tb.VisibleCols()`).
            let listed = if extended {
                table.columns().iter().enumerate().collect()
            } else {
                table.visible_columns()
            };
            listed
                .into_iter()
                .filter(|(_, candidate)| admits(&candidate.name))
                .map(|(offset, candidate)| crate::show::columns::column_description(candidate, offset, table, full, &ctx))
                .collect::<Result<Vec<_>, _>>()
        })?;
        Ok(rows)
    }

    /// Pinned Go `ShowExec.fetchShowColumnStatsUsage`: load the persisted map
    /// once, then walk every schema, table, logical/global table ID, physical
    /// partition ID, and column in infoschema order.
    fn column_stats_usage_rows(&mut self) -> Result<Vec<Vec<Datum>>, DriverError> {
        let usage = match self.column_stats_usage_provider() {
            Some(provider) => provider
                .load_column_stats_usage(&self.session_time_zone(), self.active_resource_group())
                .map_err(DriverError::unsupported)?,
            None => std::collections::HashMap::new(),
        };
        let rows = self.with_catalog_mut(|catalog| {
            let mut rows = Vec::new();
            for database in catalog.database_names() {
                let Some(names) = catalog.table_names(&database) else {
                    continue;
                };
                for name in names {
                    let Some(crate::TableEntry::Kv(table)) =
                        catalog.table_in(&database, &name)
                    else {
                        continue;
                    };
                    let mut targets = vec![(
                        table.table_id(),
                        crate::show_stats::column_stats_usage_label(
                            table.partition().is_some(),
                            None,
                        ),
                    )];
                    if let Some(partition) = table.partition() {
                        targets.extend(partition.definitions.iter().map(|definition| {
                            (
                                definition.id,
                                crate::show_stats::column_stats_usage_label(
                                    false,
                                    Some(&definition.name),
                                ),
                            )
                        }));
                    }
                    for (physical_id, partition) in targets {
                        for column in table.columns() {
                            let item = tidb_model::TableItemID {
                                table_id: physical_id,
                                id: column.id,
                                is_index: false,
                                is_sync_load_failed: false,
                            };
                            let Some((last_used_at, last_analyzed_at)) = usage.get(&item) else {
                                continue;
                            };
                            rows.push(crate::show_stats::column_stats_usage_row(
                                &database,
                                &name,
                                &partition,
                                &column.name,
                                *last_used_at,
                                *last_analyzed_at,
                            ));
                        }
                    }
                }
            }
            Ok(rows)
        })?;
        Ok(rows)
    }

    /// Go `dataForAnalyzeStatusHelper`, which both `SHOW ANALYZE STATUS` and
    /// `information_schema.ANALYZE_STATUS` answer with: the recorded analyze
    /// jobs the session may see, with a running job's progress estimate.
    fn analyze_status_rows(&mut self) -> Result<Vec<Vec<Datum>>, DriverError> {
        let provider = self.analyze_status_provider();
        let resource_group = self.active_resource_group().to_owned();
        let visibility = self.schema_visibility();
        let zone = self.session_time_zone();
        let stats_handle = self.stats_handle();
        self.with_catalog_mut(|catalog| {
            analyze_status_rows_of(
                provider.as_deref(),
                &resource_group,
                &visibility,
                &zone,
                &stats_handle,
                catalog,
            )
        })
    }

    /// Go `ShowExec.fetchShowStatsMeta` (`pkg/executor/show_stats.go:36`):
    /// one row per table or partition whose statistics this session has
    /// loaded, with the two TSOs of the stored `mysql.stats_meta` row
    /// rendered as DATETIMEs.
    ///
    /// Go walks the infoschema and asks the stats handle for each physical ID;
    /// the walk here is the catalog and the handle is
    /// [`Catalog::table_statistics`], where `None` is exactly Go's
    /// `GetNonPseudoPhysicalTableStats` miss: no `mysql.stats_meta` row was
    /// ever loaded for that physical table. A LOADED row is shown even when
    /// it carries no histograms -- Go shows an un-analyzed table too, with a
    /// NULL `Last_analyze_time`, which is the branch
    /// `appendTableForStatsMeta` makes on `IsAnalyzed()`.
    fn stats_meta_rows(
        &mut self,
        extractor: Option<&dyn tidb_planner_core_base::ShowPredicateExtractor>,
    ) -> Result<Vec<Vec<Datum>>, DriverError> {
        let stats_handle = self.stats_handle();
        // Go renders through `oracle.GetTimeFromTS`, which builds the
        // `time.Time` in the process's LOCAL zone -- not the session zone --
        // so both servers on one host render identical strings.
        let dynamic_partition_prune = !self
            .session_vars()
            .get_system(tidb_vardef::tidb_vars::TIDB_PARTITION_PRUNE_MODE)
            .is_ok_and(|mode| mode.eq_ignore_ascii_case("static"));
        let rows = self.with_catalog_mut(|catalog| {
            let mut rows = Vec::new();
            for database in catalog.database_names() {
                // Go `fetchShowStatsMeta` walks `SchemaSimpleTableInfos`, whose
                // rows come out in TABLE-ID (creation) order; an alphabetical
                // walk reorders `e` ahead of `t` where Go keeps `t` first.
                let mut named = catalog
                    .table_names(&database)
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|name| {
                        catalog.table_in(&database, &name).map(|entry| match entry {
                            crate::TableEntry::Kv(table) => (table.table_id(), name),
                            _ => (i64::MAX, name),
                        })
                    })
                    .collect::<Vec<_>>();
                named.sort_by_key(|(id, _)| *id);
                for (_, name) in named {
                    let Some(crate::TableEntry::Kv(table)) =
                        catalog.table_in(&database, &name)
                    else {
                        continue;
                    };
                    let partitions: Vec<(i64, String)> = table
                        .partition()
                        .map(|partition| {
                            partition
                                .definitions
                                .iter()
                                .map(|definition| (definition.id, definition.name.clone()))
                                .collect()
                        })
                        .unwrap_or_default();
                    for target in crate::show_stats::PartitionTargets::for_table(
                        table.table_id(),
                        &partitions,
                        dynamic_partition_prune,
                    ) {
                        let Some(statistics) = catalog.table_statistics_in(&stats_handle, target.physical_id) else {
                            continue;
                        };
                        if let Some(row) = crate::show_stats::table_statistics_meta_row(
                            &database,
                            &name,
                            &target.label,
                            &statistics,
                            &chrono::Local,
                        ) {
                            rows.push(row);
                        }
                    }
                }
            }
            Ok(rows)
        })?;
        Ok(rows
            .into_iter()
            .filter(|row| stats_row_admitted(extractor, row))
            .collect())
    }

    /// Go `ShowExec.fetchShowStatsLocked`: enumerate every physical table in
    /// the current prune mode, intersect it with the persisted lock rows, and
    /// emit in ascending physical-ID order.
    fn stats_locked_rows(&mut self) -> Result<Vec<Vec<Datum>>, DriverError> {
        let dynamic_partition_prune = !self
            .session_vars()
            .get_system(tidb_vardef::tidb_vars::TIDB_PARTITION_PRUNE_MODE)
            .is_ok_and(|mode| mode.eq_ignore_ascii_case("static"));
        let context = self.statement_context(false);
        let rows = {
            let domain = self.domain_info_schema();
            let catalog = domain.lock().map_err(|_| DriverError::CatalogPoisoned)?;
            let locked =
                crate::stats_lock::query_catalog_locked_tables(&catalog, &context)?;
            let mut physical = std::collections::BTreeMap::new();
            for database in catalog.database_names() {
                let Some(names) = catalog.table_names(&database) else {
                    continue;
                };
                for name in names {
                    let Some(crate::TableEntry::Kv(table)) =
                        catalog.table_in(&database, &name)
                    else {
                        continue;
                    };
                    let partitions = table
                        .partition()
                        .map(|partition| {
                            partition
                                .definitions
                                .iter()
                                .map(|definition| (definition.id, definition.name.clone()))
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default();
                    for target in crate::show_stats::PartitionTargets::for_table(
                        table.table_id(),
                        &partitions,
                        dynamic_partition_prune,
                    ) {
                        physical.insert(
                            target.physical_id,
                            (database.clone(), name.clone(), target.label),
                        );
                    }
                }
            }
            physical
                .into_iter()
                .filter(|(physical_id, _)| locked.contains(physical_id))
                .map(|(_, (database, table, partition))| {
                    crate::show_stats::stats_locked_row(&database, &table, &partition)
                })
                .collect()
        };
        Ok(rows)
    }

    /// Go `ShowExec.fetchShowStatsHealthy`: one health percentage for every
    /// non-pseudo physical statistics object visible under this prune mode.
    fn stats_healthy_rows(
        &mut self,
        extractor: Option<&dyn tidb_planner_core_base::ShowPredicateExtractor>,
    ) -> Result<Vec<Vec<Datum>>, DriverError> {
        let stats_handle = self.stats_handle();
        let dynamic_partition_prune = !self
            .session_vars()
            .get_system(tidb_vardef::tidb_vars::TIDB_PARTITION_PRUNE_MODE)
            .is_ok_and(|mode| mode.eq_ignore_ascii_case("static"));
        let rows = self.with_catalog_mut(|catalog| {
            let mut rows = Vec::new();
            for database in catalog.database_names() {
                let mut named = catalog
                    .table_names(&database)
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|name| {
                        catalog.table_in(&database, &name).map(|entry| match entry {
                            crate::TableEntry::Kv(table) => (table.table_id(), name),
                            _ => (i64::MAX, name),
                        })
                    })
                    .collect::<Vec<_>>();
                named.sort_by_key(|(id, _)| *id);
                for (_, name) in named {
                    let Some(crate::TableEntry::Kv(table)) =
                        catalog.table_in(&database, &name)
                    else {
                        continue;
                    };
                    let partitions = table
                        .partition()
                        .map(|partition| {
                            partition
                                .definitions
                                .iter()
                                .map(|definition| (definition.id, definition.name.clone()))
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default();
                    for target in crate::show_stats::PartitionTargets::for_table(
                        table.table_id(),
                        &partitions,
                        dynamic_partition_prune,
                    ) {
                        let Some(statistics) = catalog.table_statistics_in(&stats_handle, target.physical_id) else {
                            continue;
                        };
                        if let Some(row) = crate::show_stats::healthy_row(
                            &database,
                            &name,
                            &target.label,
                            &statistics,
                        ) {
                            rows.push(row);
                        }
                    }
                }
            }
            Ok(rows)
        })?;
        Ok(rows
            .into_iter()
            .filter(|row| stats_row_admitted(extractor, row))
            .collect())
    }

    /// Go `ShowExec.fetchShowStatsHistogram`: expose every initialized
    /// resident column and index statistics object through the ordinary
    /// statistics cache traversal.
    fn stats_histograms_rows(&mut self) -> Result<Vec<Vec<Datum>>, DriverError> {
        let stats_handle = self.stats_handle();
        let dynamic_partition_prune = !self
            .session_vars()
            .get_system(tidb_vardef::tidb_vars::TIDB_PARTITION_PRUNE_MODE)
            .is_ok_and(|mode| mode.eq_ignore_ascii_case("static"));
        let zone = chrono::Local;
        let rows = self.with_catalog_mut(|catalog| {
            let mut rows = Vec::new();
            for database in catalog.database_names() {
                let mut named = catalog
                    .table_names(&database)
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|name| {
                        catalog.table_in(&database, &name).map(|entry| match entry {
                            crate::TableEntry::Kv(table) => (table.table_id(), name),
                            _ => (i64::MAX, name),
                        })
                    })
                    .collect::<Vec<_>>();
                named.sort_by_key(|(id, _)| *id);
                for (_, name) in named {
                    let Some(crate::TableEntry::Kv(table)) =
                        catalog.table_in(&database, &name)
                    else {
                        continue;
                    };
                    let partitions = table
                        .partition()
                        .map(|partition| {
                            partition
                                .definitions
                                .iter()
                                .map(|definition| (definition.id, definition.name.clone()))
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default();
                    for target in crate::show_stats::PartitionTargets::for_table(
                        table.table_id(),
                        &partitions,
                        dynamic_partition_prune,
                    ) {
                        let Some(statistics) = catalog.table_statistics_in(&stats_handle, target.physical_id) else {
                            continue;
                        };
                        if statistics.hist_coll.pseudo {
                            continue;
                        }
                        for (offset, column) in table.columns().iter().enumerate() {
                            let Some(column_stats) = statistics.hist_coll.get_column(column.id) else {
                                continue;
                            };
                            let column_stats = column_stats
                                .read()
                                .unwrap_or_else(std::sync::PoisonError::into_inner);
                            let status = column_stats.stats_loaded_status;
                            if !status.stats_initialized() {
                                continue;
                            }
                            let row_size =
                                tidb_planner_property::stats_info::RowSizeColumnStats::new(
                                    tidb_planner_property::stats_info::RowSizeType::from_field_type_code(
                                        column.field_type.code(),
                                    ),
                                    column_stats.histogram.tot_col_size,
                                    column_stats.histogram.null_count,
                                    column_stats.total_row_count(),
                                    table.pk_handle_offset() == Some(offset),
                                );
                            let avg_col_size =
                                tidb_planner_cardinality::row_size::avg_col_size(
                                    &row_size,
                                    statistics.hist_coll.realtime_count,
                                    false,
                                );
                            rows.push(crate::show_stats::histogram_row(
                                &database,
                                &name,
                                &target.label,
                                &column.name,
                                false,
                                &column_stats.histogram,
                                avg_col_size,
                                status.status_to_string(),
                                column_stats.memory_usage().into(),
                                &zone,
                            ));
                        }
                        for index in table.indexes() {
                            let Some(index_stats) = statistics.hist_coll.get_index(index.id) else {
                                continue;
                            };
                            let index_stats = index_stats
                                .read()
                                .unwrap_or_else(std::sync::PoisonError::into_inner);
                            let status = index_stats.stats_loaded_status;
                            if !status.stats_initialized() {
                                continue;
                            }
                            rows.push(crate::show_stats::histogram_row(
                                &database,
                                &name,
                                &target.label,
                                &index.name,
                                true,
                                &index_stats.histogram,
                                0.0,
                                status.status_to_string(),
                                index_stats.memory_usage().into(),
                                &zone,
                            ));
                        }
                    }
                }
            }
            Ok(rows)
        })?;
        Ok(rows)
    }

    /// Go `ShowExec.fetchShowStatsTopN`: render every resident TopN value
    /// through the session-aware statistics value decoder.
    fn stats_topn_rows(&mut self) -> Result<Vec<Vec<Datum>>, DriverError> {
        let stats_handle = self.stats_handle();
        let dynamic_partition_prune = !self
            .session_vars()
            .get_system(tidb_vardef::tidb_vars::TIDB_PARTITION_PRUNE_MODE)
            .is_ok_and(|mode| mode.eq_ignore_ascii_case("static"));
        let zone = self.session_time_zone();
        let rows = self.with_catalog_mut(|catalog| {
            let mut rows = Vec::new();
            for database in catalog.database_names() {
                let mut named = catalog
                    .table_names(&database)
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|name| {
                        catalog.table_in(&database, &name).map(|entry| match entry {
                            crate::TableEntry::Kv(table) => (table.table_id(), name),
                            _ => (i64::MAX, name),
                        })
                    })
                    .collect::<Vec<_>>();
                named.sort_by_key(|(id, _)| *id);
                for (_, name) in named {
                    let Some(crate::TableEntry::Kv(table)) =
                        catalog.table_in(&database, &name)
                    else {
                        continue;
                    };
                    let partitions = table
                        .partition()
                        .map(|partition| {
                            partition
                                .definitions
                                .iter()
                                .map(|definition| (definition.id, definition.name.clone()))
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default();
                    for target in crate::show_stats::PartitionTargets::for_table(
                        table.table_id(),
                        &partitions,
                        dynamic_partition_prune,
                    ) {
                        let Some(statistics) = catalog.table_statistics_in(&stats_handle, target.physical_id) else {
                            continue;
                        };
                        if statistics.hist_coll.pseudo {
                            continue;
                        }
                        for (id, column_stats) in &sorted_columns(&statistics) {
                            let Some(column) =
                                table.columns().iter().find(|column| column.id == *id)
                            else {
                                continue;
                            };
                            let column_types = [column.field_type.code()];
                            let mut render = |value: &Datum,
                                              num_columns: usize,
                                              types: &[tidb_datatype::FieldTypeCode]| {
                                tidb_stats::histogram::value_to_string(
                                    value,
                                    num_columns,
                                    Some(types),
                                    Some(&zone),
                                )
                                .map_err(|error| {
                                    DriverError::Exec(crate::ExecError::internal(
                                        error.to_string(),
                                    ))
                                })
                            };
                            rows.extend(crate::show_stats::topn_to_rows(
                                &database,
                                &name,
                                &target.label,
                                &column.name,
                                1,
                                false,
                                column_stats.top_n.as_ref(),
                                &column_types,
                                &mut render,
                            )?);
                        }
                        for (id, index_stats) in &sorted_indexes(&statistics) {
                            let Some(index) = table.indexes().iter().find(|index| index.id == *id)
                            else {
                                continue;
                            };
                            let column_types = index
                                .column_offsets
                                .iter()
                                .filter_map(|offset| table.columns().get(*offset))
                                .map(|column| column.field_type.code())
                                .collect::<Vec<_>>();
                            let mut render = |value: &Datum,
                                              num_columns: usize,
                                              types: &[tidb_datatype::FieldTypeCode]| {
                                tidb_stats::histogram::value_to_string(
                                    value,
                                    num_columns,
                                    Some(types),
                                    Some(&zone),
                                )
                                .map_err(|error| {
                                    DriverError::Exec(crate::ExecError::internal(
                                        error.to_string(),
                                    ))
                                })
                            };
                            rows.extend(crate::show_stats::topn_to_rows(
                                &database,
                                &name,
                                &target.label,
                                &index.name,
                                index.column_offsets.len(),
                                true,
                                index_stats.top_n.as_ref(),
                                &column_types,
                                &mut render,
                            )?);
                        }
                    }
                }
            }
            Ok(rows)
        })?;
        Ok(rows)
    }

    /// Go `ShowExec.fetchShowStatsBuckets`: expose the resident cumulative
    /// histogram buckets in stable column/index ID order.
    fn stats_buckets_rows(&mut self) -> Result<Vec<Vec<Datum>>, DriverError> {
        let stats_handle = self.stats_handle();
        let dynamic_partition_prune = !self
            .session_vars()
            .get_system(tidb_vardef::tidb_vars::TIDB_PARTITION_PRUNE_MODE)
            .is_ok_and(|mode| mode.eq_ignore_ascii_case("static"));
        let zone = self.session_time_zone();
        let rows = self.with_catalog_mut(|catalog| {
            let mut rows = Vec::new();
            for database in catalog.database_names() {
                let mut named = catalog
                    .table_names(&database)
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|name| {
                        catalog.table_in(&database, &name).map(|entry| match entry {
                            crate::TableEntry::Kv(table) => (table.table_id(), name),
                            _ => (i64::MAX, name),
                        })
                    })
                    .collect::<Vec<_>>();
                named.sort_by_key(|(id, _)| *id);
                for (_, name) in named {
                    let Some(crate::TableEntry::Kv(table)) =
                        catalog.table_in(&database, &name)
                    else {
                        continue;
                    };
                    let partitions = table
                        .partition()
                        .map(|partition| {
                            partition
                                .definitions
                                .iter()
                                .map(|definition| (definition.id, definition.name.clone()))
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default();
                    for target in crate::show_stats::PartitionTargets::for_table(
                        table.table_id(),
                        &partitions,
                        dynamic_partition_prune,
                    ) {
                        let Some(statistics) = catalog.table_statistics_in(&stats_handle, target.physical_id) else {
                            continue;
                        };
                        if statistics.hist_coll.pseudo {
                            continue;
                        }
                        for (id, column_stats) in &sorted_columns(&statistics) {
                            let Some(column) =
                                table.columns().iter().find(|column| column.id == *id)
                            else {
                                continue;
                            };
                            let mut render = |value: &Datum,
                                              num_columns: usize,
                                              types: &[tidb_datatype::FieldTypeCode]| {
                                tidb_stats::histogram::value_to_string(
                                    value,
                                    num_columns,
                                    Some(types),
                                    Some(&zone),
                                )
                                .map_err(|error| {
                                    DriverError::Exec(crate::ExecError::internal(
                                        error.to_string(),
                                    ))
                                })
                            };
                            rows.extend(crate::show_stats::buckets_to_rows(
                                &database,
                                &name,
                                &target.label,
                                &column.name,
                                0,
                                &column_stats.histogram,
                                &[],
                                &mut render,
                            )?);
                        }
                        for (id, index_stats) in &sorted_indexes(&statistics) {
                            let Some(index) = table.indexes().iter().find(|index| index.id == *id)
                            else {
                                continue;
                            };
                            let column_types = index
                                .column_offsets
                                .iter()
                                .filter_map(|offset| table.columns().get(*offset))
                                .map(|column| column.field_type.code())
                                .collect::<Vec<_>>();
                            let mut render = |value: &Datum,
                                              num_columns: usize,
                                              types: &[tidb_datatype::FieldTypeCode]| {
                                tidb_stats::histogram::value_to_string(
                                    value,
                                    num_columns,
                                    Some(types),
                                    Some(&zone),
                                )
                                .map_err(|error| {
                                    DriverError::Exec(crate::ExecError::internal(
                                        error.to_string(),
                                    ))
                                })
                            };
                            rows.extend(crate::show_stats::buckets_to_rows(
                                &database,
                                &name,
                                &target.label,
                                &index.name,
                                index.column_offsets.len(),
                                &index_stats.histogram,
                                &column_types,
                                &mut render,
                            )?);
                        }
                    }
                }
            }
            Ok(rows)
        })?;
        Ok(rows)
    }

    /// The unfiltered rows of one SHOW PLACEMENT form.
    /// Go `fetchShowPlacement` and its FOR DATABASE / TABLE / PARTITION and
    /// LABELS forms (`executor/show_placement.go`).
    fn placement_rows(
        &mut self,
        show: &tidb_planner_core_operator_logicalop::ShowContents,
    ) -> Result<Vec<Vec<Datum>>, DriverError> {
        match show.tp {
            tidb_ast::ShowStmtType::Placement => self.all_placement_rows(),
            tidb_ast::ShowStmtType::PlacementForDatabase => {
                let database = &show.db_name;
                self.require_visible_database(database)?;
                self.with_catalog_mut(|catalog| {
                    let Some(name) = catalog
                        .database_names()
                        .into_iter()
                        .find(|name| name.go_lower_eq(database))
                    else {
                        return Err(DriverError::from(
                            tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS
                                .generate_with_stack_by_args(&[(database.clone()).into()]),
                        ));
                    };
                    Ok(database_rows(catalog, &name)?.into_iter().collect())
                })
            }
            tidb_ast::ShowStmtType::PlacementForTable => {
                let path = [show.db_name.clone(), show.table_name.clone().unwrap_or_default()];
                let path = path.as_slice();
                let (database, table_name) = self.placement_table_name(path)?;
                self.require_show_create_privilege(&database, &table_name)?;
                self.with_catalog_mut(|catalog| {
                    let (database, entry_name, entry) =
                        placement_table(catalog, &database, &table_name)?;
                    table_rows(catalog, &database, entry, &entry_name)
                        .map(|(own, _)| own.into_iter().collect())
                })
            }
            tidb_ast::ShowStmtType::PlacementForPartition => {
                let table = [show.db_name.clone(), show.table_name.clone().unwrap_or_default()];
                let table = table.as_slice();
                let partition = &show.partition;
                let (database, table_name) = self.placement_table_name(table)?;
                self.require_show_create_privilege(&database, &table_name)?;
                self.with_catalog_mut(|catalog| {
                    let (database, entry_name, entry) =
                        placement_table(catalog, &database, &table_name)?;
                    let unknown = || {
                        DriverError::from(
                            tidb_table::table_errors::ERR_UNKNOWN_PARTITION
                                .generate_with_stack_by_args(&[
                                    partition.clone().into(),
                                    entry_name.clone().into(),
                                ]),
                        )
                    };
                    let crate::TableEntry::Kv(table) = entry else {
                        return Err(unknown());
                    };
                    let Some(partitioning) = table.partition() else {
                        return Err(unknown());
                    };
                    let Some(definition) = partitioning
                        .definitions
                        .iter()
                        .find(|definition| definition.name.go_lower_eq(partition))
                    else {
                        return Err(unknown());
                    };
                    let table_placement = crate::show_regions::policy_placement(catalog, table.placement_policy().as_ref())?;
                    let placement =
                        crate::show_regions::policy_placement(catalog, definition.placement_policy.as_ref())?
                            .or(table_placement);
                    Ok(placement
                        .map(|placement| {
                            row(
                                format!(
                                    "TABLE {database}.{entry_name} PARTITION {}",
                                    definition.name
                                ),
                                &placement,
                                PENDING,
                            )
                        })
                        .into_iter()
                        .collect())
                })
            }
            // Go `fetchShowPlacementLabels`, whose restricted SQL fails with
            // the store-status retriever's plain error.
            tidb_ast::ShowStmtType::PlacementLabels => {
                let store = self.region_store();
                crate::memtable_retrievers::show_placement_labels_rows(store.as_ref()).map_err(
                    |message| DriverError::Mysql(crate::MysqlError::unknown(message)),
                )
            }
            tp => unreachable!("placement_rows called for {tp:?}"),
        }
    }

    /// The `(schema, table)` a placement SHOW names, the schema defaulting
    /// to the current database.
    fn placement_table_name(&self, path: &[String]) -> Result<(String, String), DriverError> {
        match path {
            [table] => Ok((self.require_current_database()?.to_owned(), table.clone())),
            [database, table] => Ok((database.clone(), table.clone())),
            _ => Err(DriverError::unsupported("empty table name")),
        }
    }

    /// Go `fetchShowPlacement`: policies, then databases, then tables.
    fn all_placement_rows(&mut self) -> Result<Vec<Vec<Datum>>, DriverError> {
        struct TableSet {
            database: String,
            name: String,
            rows: Vec<Vec<Datum>>,
        }
        let (mut rows, databases, tables) = self.with_catalog_mut(|catalog| {
            let mut policies: Vec<_> = catalog.policies().collect();
            policies.sort_by(|a, b| a.name.original().cmp(b.name.original()));
            let rows: Vec<Vec<Datum>> = policies
                .into_iter()
                .map(|policy| {
                    vec![
                        Datum::Bytes(format!("POLICY {}", policy.name.original()).into_bytes()),
                        Datum::Bytes(
                            policy
                                .placement_settings
                                .as_ref()
                                .map(|settings| settings.read().to_string())
                                .unwrap_or_default()
                                .into_bytes(),
                        ),
                        Datum::Bytes(b"NULL".to_vec()),
                    ]
                })
                .collect();
            let mut names = catalog.database_names();
            names.sort();
            let mut databases = Vec::new();
            for name in &names {
                databases.push((name.clone(), database_rows(catalog, name)?));
            }
            // The v2 infoschema walks its table btree downwards, so the
            // schemas come out by lower-cased name, last first.
            let mut by_lower = names.clone();
            by_lower.sort_by_key(|name| std::cmp::Reverse(name.go_to_lower()));
            let mut tables = Vec::new();
            for database in by_lower {
                let mut sets = Vec::new();
                for table_name in catalog.table_names(&database).unwrap_or_default() {
                    let Some(entry) = catalog.table_in(&database, &table_name) else {
                        continue;
                    };
                    let (own, partitions) = table_rows(catalog, &database, entry, &table_name)?;
                    let rows: Vec<_> = own.into_iter().chain(partitions).collect();
                    if !rows.is_empty() {
                        sets.push(TableSet {
                            database: database.clone(),
                            name: table_name,
                            rows,
                        });
                    }
                }
                sets.sort_by(|a, b| a.name.cmp(&b.name));
                tables.extend(sets);
            }
            Ok((rows, databases, tables))
        })?;
        for (database, database_rows) in databases {
            if self.database_is_visible(&database) {
                rows.extend(database_rows);
            }
        }
        for set in tables {
            if self.has_any_scoped_privilege(
                &set.database,
                &set.name,
                tidb_privilege::any_priv_mask(),
            ) {
                rows.extend(set.rows);
            }
        }
        Ok(rows)
    }

    /// The rows of `SHOW [FULL] PROCESSLIST`.
    ///
    /// With a server front end this is the whole live connection list. A
    /// session with NO front end (in-process tests, the embedded driver) has
    /// no peers to report, so it lists exactly one row: itself, with the
    /// values it honestly knows -- its own connection id (0 when the front
    /// end never assigned one), no client host, its current schema, and the
    /// statement it is running, which is this SHOW.
    ///
    /// Filtered by the `PROCESS` privilege the same way Go's
    /// `setDataForProcessList` / `fetchShowProcessList` both filter: a
    /// session without it sees only its own connections.
    /// Go `ShowExec.fetchShowProcessList`: one row per connection the
    /// session may see (`FULL` keeps the statement text whole).
    fn process_list_rows(&self, full: bool) -> Vec<Vec<Datum>> {
        let rows = self.viewer_state().visible_process_rows(full);
        let nullable_text = |value: String| {
            if value.is_empty() {
                Datum::Null
            } else {
                Datum::Bytes(value.into_bytes())
            }
        };
        rows.into_iter()
            .map(|row| {
                vec![
                    Datum::UInt(row.id),
                    Datum::Bytes(row.user.into_bytes()),
                    Datum::Bytes(row.host.into_bytes()),
                    // Go reports an unselected schema as SQL NULL.
                    nullable_text(row.db),
                    Datum::Bytes(row.command.into_bytes()),
                    Datum::Int(i64::try_from(row.time).unwrap_or(i64::MAX)),
                    Datum::Bytes(row.state.into_bytes()),
                    // Go reports an idle connection's statement as NULL,
                    // and truncates a running one to 100 runes without
                    // FULL.
                    // The text carries a byte that is not UTF-8 as its
                    // `raw_bytes` escape; Go's string holds the byte.
                    match row.info {
                        Some(info) => Datum::Bytes(
                            tidb_hack::raw_bytes::str_to_bytes(
                                &tidb_sessmgr::truncate_process_info(&info, full),
                            )
                            .into_owned(),
                        ),
                        None => Datum::Null,
                    },
                ]
            })
            .collect()
    }

    /// Go `fetchShowBind`, session scope.
    /// Go `ShowExec.fetchShowBindingCacheStatus`: the cache count is the
    /// enabled/`using` bindings the handle holds, the table count is
    /// `count(*)` over the same statuses, and the usage is the sum of
    /// `Binding.size()` -- each string field's byte length plus two 16-byte
    /// `types.Time` stamps (the `ID` a fresh binding carries is empty).
    /// This tier's cache IS the table, so the two counts agree by
    /// construction; on real TiDB they differ only while the cache lags a
    /// peer's write. Captured: a fresh store answers
    /// `0|0|0 Bytes|64 MB`, one binding `1|1|156 Bytes|64 MB`.
    fn binding_cache_status_rows(&mut self) -> Result<Vec<Vec<Datum>>, DriverError> {
        use tidb_bindinfo::binding::STATUS_USING;
        let loaded = self.load_global_bindings()?;
        let all = loaded.all_sorted();
        // A DISABLED row counts in NEITHER column but still holds cache
        // memory (captured: disabling flips `1|1|156 Bytes` to
        // `0|0|157 Bytes` -- one byte more, `disabled` over `enabled`).
        let live = all
            .iter()
            .filter(|binding| {
                binding.status == tidb_bindinfo::binding::STATUS_ENABLED
                    || binding.status == STATUS_USING
            })
            .count();
        let usage: i64 = all
            .iter()
            .map(|binding| tidb_bindinfo::binding_cache::binding_size(binding))
            .sum();
        let quota = self
            .session_vars()
            .get_system("tidb_mem_quota_binding_cache")
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(64 << 20);
        let count = i64::try_from(live).unwrap_or(i64::MAX);
        Ok(vec![vec![
            Datum::Int(count),
            Datum::Int(count),
            Datum::new_string(tidb_memory::format_bytes(usage)),
            Datum::new_string(tidb_memory::format_bytes(quota)),
        ]])
    }

    /// Go `ShowExec.fetchShowBind`: the GLOBAL or SESSION bindings, one row
    /// each, their times as the `TIMESTAMP(6)` values `mysql.bind_info` keeps.
    fn show_bindings_rows(&mut self, global_scope: bool) -> Result<Vec<Vec<Datum>>, DriverError> {
        let time = |text: &str| {
            tidb_datatype::parse_time(
                text,
                tidb_datatype::TimeType::DateTime,
                6,
                false,
                true,
                false,
                &tidb_datatype::SessionTimeZone::utc(),
            )
            .map_or(Datum::Null, |parsed| Datum::Time(parsed.time))
        };
        let global;
        let listed = if global_scope {
            global = self.load_global_bindings()?;
            global.all_sorted()
        } else {
            self.session_bindings_mut().all_sorted()
        };
        let rows = listed
            .into_iter()
            .map(|binding| {
                vec![
                    Datum::Bytes(binding.original_sql.clone().into_bytes()),
                    Datum::Bytes(binding.bind_sql.clone().into_bytes()),
                    Datum::Bytes(binding.db.clone().into_bytes()),
                    Datum::Bytes(binding.status.as_bytes().to_vec()),
                    time(&binding.create_time),
                    time(&binding.update_time),
                    Datum::Bytes(binding.charset.clone().into_bytes()),
                    Datum::Bytes(binding.collation.clone().into_bytes()),
                    Datum::Bytes(binding.source.as_bytes().to_vec()),
                    Datum::Bytes(binding.sql_digest.clone().into_bytes()),
                    // Go leaves `Plan_digest` empty for a manually created
                    // binding; only `FROM HISTORY` fills it.
                    Datum::Bytes(binding.plan_digest.clone().into_bytes()),
                ]
            })
            .collect();
        Ok(rows)
    }
}

/// The `ShowBaseExtractor` `buildShow` installed on a planned SHOW.
fn base_extractor(
    leaf: &tidb_planner_core_operator_physicalop::PhysicalShow,
) -> Option<&tidb_planner::show_predicate_extractor::ShowBaseExtractor> {
    leaf.extractor
        .as_deref()?
        .as_any()
        .downcast_ref::<tidb_planner::show_predicate_extractor::ShowBaseExtractor>()
}

/// The column statistics in id order, the order SHOW renders them in.
fn sorted_columns(statistics: &tidb_stats::Table) -> Vec<(i64, tidb_stats::column::Column)> {
    let mut columns = Vec::new();
    statistics.hist_coll.for_each_column(|id, column| {
        columns.push((id, column.clone()));
        false
    });
    columns.sort_by_key(|(id, _)| *id);
    columns
}

/// The index statistics in id order, the order SHOW renders them in.
fn sorted_indexes(statistics: &tidb_stats::Table) -> Vec<(i64, tidb_stats::index::Index)> {
    let mut indexes = Vec::new();
    statistics.hist_coll.for_each_index(|id, index| {
        indexes.push((id, index.clone()));
        false
    });
    indexes.sort_by_key(|(id, _)| *id);
    indexes
}
