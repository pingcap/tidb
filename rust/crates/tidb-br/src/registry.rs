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

//! Go `br/pkg/registry`: `mysql.tidb_restore_registry`, where restores
//! register so concurrent ones neither restore the same tables nor reuse a
//! paused task's timestamp by accident. Restore name routing (`--rename`)
//! is a `br` CLI option; its routed rows are refused rather than evaluated.
//! The PiTR-only operations (`OperationAfterWaitIDs`,
//! `GlobalOperationAfterSetResettingStatus`, `FindAndDeleteMatchingTask`)
//! are not ported.

use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tidb_datatype::Datum;
use tidb_gocontext::Context;
use tidb_log::{Field, Value};
use tidb_model::TableInfo;
use tidb_sqlescape::SqlArg;

use crate::errors::{self, Result, ERR_INVALID_ARGUMENT, ERR_TABLES_ALREADY_EXISTED};
use crate::glue::{Domain, Glue, Session};
use crate::metautil;
use crate::utils::filter::match_table;

pub mod heartbeat;

use heartbeat::HeartbeatManager;

/// Go `RestoreRegistryDBName`.
pub const RESTORE_REGISTRY_DB_NAME: &str = "mysql";
/// Go `RestoreRegistryTableName`.
pub const RESTORE_REGISTRY_TABLE_NAME: &str = "tidb_restore_registry";
/// Go `FilterSeparator`: ASCII Unit Separator, never in an identifier.
pub const FILTER_SEPARATOR: &str = "\u{1F}";
/// Go `StaleTaskThresholdMinutes`.
pub const STALE_TASK_THRESHOLD_MINUTES: u32 = 5;

/// Go `legacyLookupRegistrationSQLTemplate`.
const LEGACY_LOOKUP_REGISTRATION_SQL: &str = "
		SELECT id, status FROM mysql.tidb_restore_registry
		WHERE filter_hash = MD5(%?)
		AND start_ts = %?
		AND restored_ts = %?
		AND upstream_cluster_id = %?
		AND with_sys_table = %?
		AND cmd = %?
		ORDER BY id DESC
		FOR UPDATE";
/// Go `updateStatusFromMultipleSQLTemplate` with the table named.
fn update_status_from_multiple_sql(statuses: &str) -> String {
    format!(
        "
		UPDATE mysql.tidb_restore_registry
		SET status = %?
		WHERE id = %? AND status IN ({statuses})"
    )
}
/// Go `resumeTaskByIDSQLTemplate`.
const RESUME_TASK_BY_ID_SQL: &str = "
		UPDATE mysql.tidb_restore_registry
		SET status = 'running', last_heartbeat_time = FROM_UNIXTIME(%?)
		WHERE id = %?";
/// Go `deleteRegistrationSQLTemplate`.
const DELETE_REGISTRATION_SQL: &str = "DELETE FROM mysql.tidb_restore_registry WHERE id = %?";
/// Go `selectRegistrationsByMaxIDSQLTemplate`.
const SELECT_REGISTRATIONS_BY_MAX_ID_SQL: &str = "
		SELECT
		id, filter_strings, source_filter_strings, route_strings, route_hash, start_ts, restored_ts,
		upstream_cluster_id, with_sys_table, status, cmd, filter_hash
		FROM mysql.tidb_restore_registry
		WHERE id < %?
		ORDER BY id ASC";
/// Go `legacySelectRegistrationsByMaxIDSQLTemplate`.
const LEGACY_SELECT_REGISTRATIONS_BY_MAX_ID_SQL: &str = "
		SELECT
		id, filter_strings, start_ts, restored_ts, upstream_cluster_id, with_sys_table, status, cmd, filter_hash
		FROM mysql.tidb_restore_registry
		WHERE id < %?
		ORDER BY id ASC";
/// Go `legacyCreateNewTaskSQLTemplate`.
const LEGACY_CREATE_NEW_TASK_SQL: &str = "
		INSERT INTO mysql.tidb_restore_registry
		(filter_strings, filter_hash, start_ts, restored_ts, upstream_cluster_id,
		 with_sys_table, status, cmd, task_start_time, last_heartbeat_time)
		VALUES (%?, MD5(%?), %?, %?, %?, %?, 'running', %?, FROM_UNIXTIME(%?), FROM_UNIXTIME(%?))";
/// Go `selectTaskHeartbeatSQLTemplate`.
const SELECT_TASK_HEARTBEAT_SQL: &str = "
		SELECT CAST(UNIX_TIMESTAMP(last_heartbeat_time) AS UNSIGNED INTEGER)
		FROM mysql.tidb_restore_registry
		WHERE id = %?";
/// Go `legacySelectConflictingTaskSQLTemplate`.
const LEGACY_SELECT_CONFLICTING_TASK_SQL: &str = "
		SELECT id, restored_ts, status, CAST(UNIX_TIMESTAMP(last_heartbeat_time) AS UNSIGNED INTEGER) FROM mysql.tidb_restore_registry
		WHERE filter_hash = MD5(%?)
		AND start_ts = %?
		AND upstream_cluster_id = %?
		AND with_sys_table = %?
		AND cmd = %?
		ORDER BY id DESC
		LIMIT 1";
/// Go `selectResettingStatusTasksSQLTemplate`.
const SELECT_RESETTING_STATUS_TASKS_SQL: &str =
    "SELECT id FROM mysql.tidb_restore_registry WHERE status = 'resetting'";
/// Go `transitionStaleTaskToPausedSQLTemplate`.
const TRANSITION_STALE_TASK_TO_PAUSED_SQL: &str = "
		UPDATE mysql.tidb_restore_registry
		SET status = 'paused'
		WHERE id = %? AND status IN ('running', 'resetting') AND last_heartbeat_time = FROM_UNIXTIME(%?)";

/// Go `restoreRegistryRouteIndexColumns`: `(name, prefix length)`, -1 for
/// the whole column.
const RESTORE_REGISTRY_ROUTE_INDEX_COLUMNS: [(&str, i64); 7] = [
    ("filter_hash", -1),
    ("route_hash", -1),
    ("start_ts", -1),
    ("restored_ts", -1),
    ("upstream_cluster_id", -1),
    ("with_sys_table", -1),
    ("cmd", 256),
];
/// Go `restoreRegistryLegacyIndexName`.
const RESTORE_REGISTRY_LEGACY_INDEX_NAME: &str = "unique_registration_params";
/// Go `restoreRegistryRouteIndexName`.
const RESTORE_REGISTRY_ROUTE_INDEX_NAME: &str = "unique_registration_params_v2";

/// Go `TaskStatus`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskStatus {
    /// Go `TaskStatusRunning`.
    Running,
    /// Go `TaskStatusPaused`.
    Paused,
    /// Go `TaskStatusResetting`.
    Resetting,
}

impl TaskStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Paused => "paused",
            Self::Resetting => "resetting",
        }
    }
}

/// Go `RegistrationInfo`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RegistrationInfo {
    /// Go `FilterStrings`.
    pub filter_strings: Vec<String>,
    /// Go `RouteStrings`: canonical restore routes, empty without
    /// `--rename`.
    pub route_strings: Vec<String>,
    /// Go `RouteHash`.
    pub route_hash: String,
    /// Go `FilterHashInput`.
    pub filter_hash_input: String,
    /// Go `StartTS`.
    pub start_ts: u64,
    /// Go `RestoredTS`.
    pub restored_ts: u64,
    /// Go `UpstreamClusterID`.
    pub upstream_cluster_id: u64,
    /// Go `WithSysTable`.
    pub with_sys_table: bool,
    /// Go `Cmd`.
    pub cmd: String,
}

/// Go `RegistrationInfoWithID`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegistrationInfoWithID {
    /// Go's embedded `RegistrationInfo`.
    pub info: RegistrationInfo,
    /// Go `restoreID`.
    pub restore_id: u64,
}

/// Go `registrationFilterHashInput`.
fn registration_filter_hash_input(info: &RegistrationInfo) -> String {
    if info.filter_hash_input.is_empty() {
        info.filter_strings.join(FILTER_SEPARATOR)
    } else {
        info.filter_hash_input.clone()
    }
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64)
}

fn str_arg(s: &str) -> SqlArg<'_> {
    SqlArg::String(s.as_bytes())
}

fn datum_u64(d: &Datum) -> u64 {
    d.as_uint().unwrap_or_else(|| d.get_int64() as u64)
}

fn datum_string(d: &Datum) -> String {
    String::from_utf8_lossy(d.go_bytes()).into_owned()
}

/// Go `hasRestoreRegistryRouteSchema`: the route columns are public and
/// only the route-aware unique index exists.
#[must_use]
pub fn has_restore_registry_route_schema(table: &TableInfo) -> bool {
    let has_public_column = |name: &str| {
        table.columns.iter_deref().any(|c| {
            c.read().name.lowercase() == name && c.read().state == tidb_model::SchemaState::PUBLIC
        })
    };
    if !["source_filter_strings", "route_strings", "route_hash"]
        .iter()
        .all(|n| has_public_column(n))
    {
        return false;
    }
    let find_index = |name: &str| {
        table
            .indices
            .iter_deref()
            .find(|i| i.read().name.lowercase() == name)
    };
    if find_index(RESTORE_REGISTRY_LEGACY_INDEX_NAME).is_some() {
        return false;
    }
    let Some(index) = find_index(RESTORE_REGISTRY_ROUTE_INDEX_NAME) else {
        return false;
    };
    let index = index.read();
    if !index.unique || index.state != tidb_model::SchemaState::PUBLIC || index.primary {
        return false;
    }
    let columns: Vec<_> = index.columns.iter_deref().collect();
    columns.len() == RESTORE_REGISTRY_ROUTE_INDEX_COLUMNS.len()
        && columns
            .iter()
            .zip(RESTORE_REGISTRY_ROUTE_INDEX_COLUMNS)
            .all(|(c, (name, length))| {
                c.read().name.lowercase() == name && c.read().length == length
            })
}

/// A glue session shared between the registry and its heartbeat thread.
pub(crate) type SharedSession = std::sync::Arc<Mutex<Box<dyn Session>>>;

/// Go `Registry`.
pub struct Registry {
    se: Mutex<Box<dyn Session>>,
    heartbeat_session: SharedSession,
    heartbeat_manager: Mutex<Option<HeartbeatManager>>,
    wait_ids: Mutex<Vec<u64>>,
    table_exists: bool,
    route_schema_ready: bool,
    /// Go's `time.Minute` stale-check tick (`is-task-stale-ticker-duration`).
    stale_check_tick: Duration,
}

/// Go `NewRestoreRegistry`: two sessions, and whether the registry table
/// and its route schema exist.
///
/// # Errors
/// A session cannot be created, or the schema cannot be read.
pub fn new_restore_registry(g: &dyn Glue, dom: &dyn Domain) -> Result<Registry> {
    let se = g.create_session()?;
    let heartbeat_session = g.create_session()?;
    let table = dom
        .info_schema()?
        .table_by_name(RESTORE_REGISTRY_DB_NAME, RESTORE_REGISTRY_TABLE_NAME);
    let mut stale_check_tick = Duration::from_secs(60);
    if let Some(v) = tidb_failpoint::eval(
        "github.com/pingcap/tidb/br/pkg/registry/is-task-stale-ticker-duration",
    ) {
        stale_check_tick = Duration::from_secs(u64::try_from(v.int()).unwrap_or(0));
    }
    Ok(Registry {
        se: Mutex::new(se),
        heartbeat_session: std::sync::Arc::new(Mutex::new(heartbeat_session)),
        heartbeat_manager: Mutex::new(None),
        wait_ids: Mutex::new(Vec::new()),
        table_exists: table.is_some(),
        route_schema_ready: table
            .as_ref()
            .is_some_and(has_restore_registry_route_schema),
        stale_check_tick,
    })
}

impl Registry {
    /// Go `Close`: the heartbeat stopped; the sessions are dropped with the
    /// registry.
    pub fn close(&self) {
        tidb_log::info("closing registry", &[]);
        self.stop_heartbeat_manager();
    }

    /// Whether `mysql.tidb_restore_registry` exists.
    #[must_use]
    pub fn table_exists(&self) -> bool {
        self.table_exists
    }

    fn rows(&self, ctx: &Context, sql: &str, args: &[SqlArg<'_>]) -> Result<Vec<Vec<Datum>>> {
        self.se.lock().unwrap().exec_restricted_sql(ctx, sql, args)
    }

    /// Go `executeInTransaction`: `f` inside `BEGIN PESSIMISTIC`, committed
    /// on success and rolled back otherwise, all on the registry session.
    fn execute_in_transaction<T>(
        &self,
        ctx: &Context,
        f: impl FnOnce(&dyn Session) -> Result<T>,
    ) -> Result<T> {
        let se = self.se.lock().unwrap();
        se.exec_restricted_sql(ctx, "BEGIN PESSIMISTIC", &[])
            .map_err(|e| errors::annotate(e, "failed to begin transaction"))?;
        match f(se.as_ref()) {
            Err(err) => {
                if let Err(rollback) = se.exec_restricted_sql(ctx, "ROLLBACK", &[]) {
                    tidb_log::error(
                        "failed to rollback transaction",
                        &[Field::new("error", Value::Str(rollback.to_string()))],
                    );
                }
                Err(err)
            }
            Ok(v) => {
                if let Err(commit) = se.exec_restricted_sql(ctx, "COMMIT", &[]) {
                    tidb_log::error(
                        "failed to commit transaction",
                        &[Field::new("error", Value::Str(commit.to_string()))],
                    );
                    return Err(commit);
                }
                Ok(v)
            }
        }
    }

    /// Go `normalizeRegistrationRoutes` and `checkRouteColumnCompatibility`
    /// for the routes BRIE never sets.
    fn check_routes(info: &RegistrationInfo) -> Result<()> {
        if info.route_strings.is_empty() {
            if !info.route_hash.is_empty() {
                return Err(errors::annotatef(
                    &ERR_INVALID_ARGUMENT,
                    "restore route hash is set without route rules",
                ));
            }
            return Ok(());
        }
        Err(errors::annotatef(
            &ERR_INVALID_ARGUMENT,
            "restore name routing is not supported by this TiDB",
        ))
    }

    /// Go `ResumeOrCreateRegistration`: a paused registration with the same
    /// parameters is resumed; a running one fails; otherwise a new one is
    /// created. Returns the restore ID and the restored TS to use.
    ///
    /// # Errors
    /// A running registration exists, or a statement fails.
    pub fn resume_or_create_registration(
        &self,
        ctx: &Context,
        mut info: RegistrationInfo,
        is_restored_ts_user_specified: bool,
    ) -> Result<(u64, u64)> {
        Self::check_routes(&info)?;
        let resolved_restore_ts =
            self.resolve_restore_ts(ctx, &info, is_restored_ts_user_specified)?;
        if resolved_restore_ts != info.restored_ts {
            tidb_log::info(
                "using resolved restoredTS from existing task",
                &[
                    Field::new("original_restored_ts", Value::U64(info.restored_ts)),
                    Field::new("resolved_restored_ts", Value::U64(resolved_restore_ts)),
                ],
            );
            info.restored_ts = resolved_restore_ts;
        }
        let filter_strings = info.filter_strings.join(FILTER_SEPARATOR);
        let filter_hash_input = registration_filter_hash_input(&info);
        tidb_log::info(
            "attempting to resume or create registration",
            &[
                Field::new("filter_strings", Value::Str(filter_strings.clone())),
                Field::new("filter_hash_input", Value::Str(filter_hash_input.clone())),
                Field::new("start_ts", Value::U64(info.start_ts)),
                Field::new("restored_ts", Value::U64(info.restored_ts)),
                Field::new("upstream_cluster_id", Value::U64(info.upstream_cluster_id)),
                Field::new("with_sys_table", Value::Bool(info.with_sys_table)),
                Field::new("cmd", Value::Str(info.cmd.clone())),
                Field::new(
                    "is_restored_ts_user_specified",
                    Value::Bool(is_restored_ts_user_specified),
                ),
            ],
        );
        let task_id = self.execute_in_transaction(ctx, |se| {
            let rows = se
                .exec_restricted_sql(
                    ctx,
                    LEGACY_LOOKUP_REGISTRATION_SQL,
                    &[
                        str_arg(&filter_hash_input),
                        SqlArg::Unsigned(info.start_ts),
                        SqlArg::Unsigned(info.restored_ts),
                        SqlArg::Unsigned(info.upstream_cluster_id),
                        SqlArg::Bool(info.with_sys_table),
                        str_arg(&info.cmd),
                    ],
                )
                .map_err(|e| errors::annotate(e, "failed to look up existing task"))?;
            if let Some(row) = rows.first() {
                let existing = datum_u64(&row[0]);
                let status = datum_string(&row[1]);
                if existing == 0 {
                    return Err(errors::errorf("invalid task ID: got 0 from lookup"));
                }
                if status == TaskStatus::Running.as_str()
                    || status == TaskStatus::Resetting.as_str()
                {
                    tidb_log::warn(
                        "task already exists and is running",
                        &[Field::new("restore_id", Value::U64(existing))],
                    );
                    return Err(errors::annotatef(
                        &ERR_INVALID_ARGUMENT,
                        format!("task with ID {existing} already exists and is running"),
                    ));
                }
                if status == TaskStatus::Paused.as_str() {
                    se.exec_restricted_sql(
                        ctx,
                        RESUME_TASK_BY_ID_SQL,
                        &[SqlArg::Signed(now_unix()), SqlArg::Unsigned(existing)],
                    )
                    .map_err(|e| errors::annotate(e, "failed to resume paused task"))?;
                    tidb_log::info(
                        "successfully resumed existing registration",
                        &[Field::new("restore_id", Value::U64(existing))],
                    );
                    return Ok(existing);
                }
                tidb_log::warn(
                    "task exists but in unexpected state",
                    &[
                        Field::new("restore_id", Value::U64(existing)),
                        Field::new("status", Value::Str(status.clone())),
                    ],
                );
                return Err(errors::annotatef(
                    &ERR_INVALID_ARGUMENT,
                    format!("task with ID {existing} exists but is in unexpected state: {status}"),
                ));
            }
            let now = now_unix();
            se.exec_restricted_sql(
                ctx,
                LEGACY_CREATE_NEW_TASK_SQL,
                &[
                    str_arg(&filter_strings),
                    str_arg(&filter_hash_input),
                    SqlArg::Unsigned(info.start_ts),
                    SqlArg::Unsigned(info.restored_ts),
                    SqlArg::Unsigned(info.upstream_cluster_id),
                    SqlArg::Bool(info.with_sys_table),
                    str_arg(&info.cmd),
                    SqlArg::Signed(now),
                    SqlArg::Signed(now),
                ],
            )
            .map_err(|e| errors::annotate(e, "failed to create new registration"))?;
            let last = se
                .exec_restricted_sql(ctx, "SELECT LAST_INSERT_ID()", &[])
                .map_err(|e| errors::annotate(e, "failed to get ID of newly created task"))?;
            let Some(row) = last.first() else {
                return Err(errors::errorf("failed to get LAST_INSERT_ID()"));
            };
            let id = datum_u64(&row[0]);
            if id == 0 {
                return Err(errors::errorf(
                    "invalid task ID: got 0 from LAST_INSERT_ID()",
                ));
            }
            tidb_log::info(
                "successfully created new registration",
                &[Field::new("restore_id", Value::U64(id))],
            );
            Ok(id)
        })?;
        self.collect_resetting_status_tasks(ctx)?;
        Ok((task_id, resolved_restore_ts))
    }

    /// Go `collectResettingStatusTasks`.
    fn collect_resetting_status_tasks(&self, ctx: &Context) -> Result<()> {
        let rows = self
            .rows(ctx, SELECT_RESETTING_STATUS_TASKS_SQL, &[])
            .map_err(|e| errors::annotate(e, "failed to look up tasks with resetting status"))?;
        *self.wait_ids.lock().unwrap() = rows.iter().map(|r| datum_u64(&r[0])).collect();
        Ok(())
    }

    /// Go `updateTaskStatusFromMultiple`.
    fn update_task_status_from_multiple(
        &self,
        ctx: &Context,
        restore_id: u64,
        current: &[TaskStatus],
        new: TaskStatus,
    ) -> Result<()> {
        if current.is_empty() {
            return Err(errors::errorf("currentStatuses cannot be empty"));
        }
        let names: Vec<&str> = current.iter().map(|s| s.as_str()).collect();
        let in_clause = names
            .iter()
            .map(|s| format!("'{s}'"))
            .collect::<Vec<_>>()
            .join(", ");
        tidb_log::info(
            "attempting to update task status from multiple possible statuses",
            &[
                Field::new("restore_id", Value::U64(restore_id)),
                Field::new("current_statuses", Value::Str(format!("{names:?}"))),
                Field::new("new_status", Value::Str(new.as_str().into())),
            ],
        );
        self.se
            .lock()
            .unwrap()
            .execute_internal(
                ctx,
                &update_status_from_multiple_sql(&in_clause),
                &[str_arg(new.as_str()), SqlArg::Unsigned(restore_id)],
            )
            .map_err(|e| {
                errors::annotate(
                    e,
                    format!(
                        "failed to conditionally update task status from [{}] to {}",
                        names.join(" "),
                        new.as_str()
                    ),
                )
            })
    }

    /// Go `Unregister`.
    ///
    /// # Errors
    /// The delete fails.
    pub fn unregister(&self, ctx: &Context, restore_id: u64) -> Result<()> {
        self.stop_heartbeat_manager();
        self.se
            .lock()
            .unwrap()
            .execute_internal(
                ctx,
                DELETE_REGISTRATION_SQL,
                &[SqlArg::Unsigned(restore_id)],
            )
            .map_err(|e| {
                errors::annotate(e, format!("failed to unregister restore {restore_id}"))
            })?;
        tidb_log::info(
            "unregistered restore task",
            &[Field::new("restore_id", Value::U64(restore_id))],
        );
        Ok(())
    }

    /// Go `PauseTask`: paused only when running or resetting.
    ///
    /// # Errors
    /// The update fails.
    pub fn pause_task(&self, ctx: &Context, restore_id: u64) -> Result<()> {
        self.stop_heartbeat_manager();
        self.update_task_status_from_multiple(
            ctx,
            restore_id,
            &[TaskStatus::Running, TaskStatus::Resetting],
            TaskStatus::Paused,
        )
    }
}

impl Registry {
    /// Go `GetRegistrationsByMaxID`: the registrations before `max_id`.
    ///
    /// # Errors
    /// The query fails.
    pub fn get_registrations_by_max_id(
        &self,
        ctx: &Context,
        max_id: u64,
    ) -> Result<Vec<RegistrationInfoWithID>> {
        let sql = if self.route_schema_ready {
            SELECT_REGISTRATIONS_BY_MAX_ID_SQL
        } else {
            LEGACY_SELECT_REGISTRATIONS_BY_MAX_ID_SQL
        };
        let rows = self
            .rows(ctx, sql, &[SqlArg::Unsigned(max_id)])
            .map_err(|e| {
                errors::annotate(
                    e,
                    format!("failed to query registrations with max ID {max_id}"),
                )
            })?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let id = datum_u64(&row[0]);
            tidb_log::info(
                "found existing restore task",
                &[
                    Field::new("restore_id", Value::U64(id)),
                    Field::new("max_id", Value::U64(max_id)),
                ],
            );
            let mut filter_strings = datum_string(&row[1]);
            let mut route_strings = Vec::new();
            let mut route_hash = String::new();
            let mut offset = 0;
            if self.route_schema_ready {
                let source = datum_string(&row[2]);
                if !source.is_empty() {
                    filter_strings = source;
                }
                let encoded = datum_string(&row[3]);
                if !encoded.is_empty() {
                    route_strings = serde_json::from_str(&encoded).map_err(|e| {
                        errors::annotate(
                            errors::annotate(
                                errors::errorf(e.to_string()),
                                "failed to decode restore routes",
                            ),
                            format!("invalid routes in restore registration {id}"),
                        )
                    })?;
                }
                route_hash = datum_string(&row[4]);
                offset = 3;
            }
            out.push(RegistrationInfoWithID {
                info: RegistrationInfo {
                    filter_strings: filter_strings
                        .split(FILTER_SEPARATOR)
                        .map(str::to_owned)
                        .collect(),
                    route_strings,
                    route_hash,
                    filter_hash_input: String::new(),
                    start_ts: datum_u64(&row[2 + offset]),
                    restored_ts: datum_u64(&row[3 + offset]),
                    upstream_cluster_id: datum_u64(&row[4 + offset]),
                    with_sys_table: row[5 + offset].get_int64() != 0,
                    cmd: datum_string(&row[7 + offset]),
                },
                restore_id: id,
            });
        }
        Ok(out)
    }

    /// Go `CheckTablesWithRegisteredTasks` for a snapshot restore (no PiTR
    /// tracker, no routes): no table is being restored by an earlier task.
    ///
    /// # Errors
    /// [`ERR_TABLES_ALREADY_EXISTED`] for a conflict, or the query fails.
    pub fn check_tables_with_registered_tasks(
        &self,
        ctx: &Context,
        restore_id: u64,
        dbs: &[metautil::Database],
        tables: &[metautil::Table],
    ) -> Result<()> {
        let registrations = self
            .get_registrations_by_max_id(ctx, restore_id)
            .map_err(|e| errors::annotate(e, "failed to query existing registrations"))?;
        if registrations.is_empty() {
            tidb_log::info("found zero existing registered tasks", &[]);
            return Ok(());
        }
        for reg in &registrations {
            let filters: Vec<&str> = reg.info.filter_strings.iter().map(String::as_str).collect();
            let f = match tidb_table_filter::parse(&filters) {
                Ok(f) => tidb_table_filter::case_insensitive(f),
                Err(err) => {
                    tidb_log::warn(
                        "failed to parse filter strings from registration",
                        &[
                            Field::new(
                                "filter_strings",
                                Value::Str(format!("{:?}", reg.info.filter_strings)),
                            ),
                            Field::new("error", Value::Str(err.to_string())),
                        ],
                    );
                    continue;
                }
            };
            if !reg.info.route_strings.is_empty() {
                return Err(errors::annotatef(
                    &ERR_INVALID_ARGUMENT,
                    format!(
                        "restore registration {} uses name routing, which this TiDB cannot check",
                        reg.restore_id
                    ),
                ));
            }
            check_for_table_conflicts(dbs, tables, reg, f.as_ref(), restore_id)?;
        }
        tidb_log::info(
            "no conflicts found with existing restore tasks",
            &[
                Field::new("tables_count", Value::U64(tables.len() as u64)),
                Field::new("current_restore_id", Value::U64(restore_id)),
            ],
        );
        Ok(())
    }

    /// Go `StartHeartbeatManager`.
    pub fn start_heartbeat_manager(&self, ctx: &Context, restore_id: u64) {
        self.stop_heartbeat_manager();
        let manager = HeartbeatManager::start(
            ctx,
            std::sync::Arc::clone(&self.heartbeat_session),
            restore_id,
        );
        *self.heartbeat_manager.lock().unwrap() = Some(manager);
        tidb_log::info(
            "started heartbeat manager for restore task",
            &[Field::new("restore_id", Value::U64(restore_id))],
        );
    }

    /// Go `StopHeartbeatManager`.
    pub fn stop_heartbeat_manager(&self) {
        if let Some(m) = self.heartbeat_manager.lock().unwrap().take() {
            m.stop();
            tidb_log::info("stopped heartbeat manager for restore task", &[]);
        }
    }

    /// Go `resolveRestoreTS`: a paused task with the same parameters (or a
    /// running one whose heartbeat stopped) lends its restored TS.
    fn resolve_restore_ts(
        &self,
        ctx: &Context,
        info: &RegistrationInfo,
        user_specified: bool,
    ) -> Result<u64> {
        let rows = self
            .rows(
                ctx,
                LEGACY_SELECT_CONFLICTING_TASK_SQL,
                &[
                    str_arg(&registration_filter_hash_input(info)),
                    SqlArg::Unsigned(info.start_ts),
                    SqlArg::Unsigned(info.upstream_cluster_id),
                    SqlArg::Bool(info.with_sys_table),
                    str_arg(&info.cmd),
                ],
            )
            .map_err(|e| {
                errors::annotate(e, "failed to check for existing tasks with same parameters")
            })?;
        let Some(row) = rows.first() else {
            tidb_log::info(
                "no existing tasks found with same parameters",
                &[Field::new("restored_ts", Value::U64(info.restored_ts))],
            );
            return Ok(info.restored_ts);
        };
        let (id, existing_ts, status, heartbeat) = (
            datum_u64(&row[0]),
            datum_u64(&row[1]),
            datum_string(&row[2]),
            row[3].get_int64(),
        );
        tidb_log::info(
            "found existing task with same parameters",
            &[
                Field::new("existing_task_id", Value::U64(id)),
                Field::new("existing_restored_ts", Value::U64(existing_ts)),
                Field::new("existing_status", Value::Str(status.clone())),
                Field::new("current_restored_ts", Value::U64(info.restored_ts)),
            ],
        );
        if user_specified && existing_ts != info.restored_ts {
            return Err(errors::annotatef(
                &ERR_INVALID_ARGUMENT,
                format!(
                    "existing task has different restoredTS({existing_ts}) from user-specified({})",
                    info.restored_ts
                ),
            ));
        }
        if status == TaskStatus::Paused.as_str() {
            tidb_log::info(
                "existing task is paused, reusing its restoredTS",
                &[Field::new("existing_task_id", Value::U64(id))],
            );
            return Ok(existing_ts);
        }
        if status == TaskStatus::Running.as_str() || status == TaskStatus::Resetting.as_str() {
            let stale = match self.is_task_stale(ctx, id, heartbeat) {
                Ok(s) => s,
                Err(err) => {
                    tidb_log::warn(
                        "failed to check if task is stale, using current restoredTS",
                        &[
                            Field::new("task_id", Value::U64(id)),
                            Field::new("error", Value::Str(err.to_string())),
                        ],
                    );
                    return Ok(info.restored_ts);
                }
            };
            if !stale {
                tidb_log::info(
                    "existing running task is active, using current restoredTS",
                    &[Field::new("existing_task_id", Value::U64(id))],
                );
                return Ok(info.restored_ts);
            }
            return match self.transition_stale_task_to_paused(ctx, id, heartbeat) {
                Ok(true) => Ok(existing_ts),
                Ok(false) => Ok(info.restored_ts),
                Err(err) => {
                    tidb_log::warn(
                        "failed to transition stale task to paused, using current restoredTS",
                        &[
                            Field::new("task_id", Value::U64(id)),
                            Field::new("error", Value::Str(err.to_string())),
                        ],
                    );
                    Ok(info.restored_ts)
                }
            };
        }
        tidb_log::warn(
            "existing task is in unexpected state, using current restoredTS",
            &[
                Field::new("existing_task_id", Value::U64(id)),
                Field::new("status", Value::Str(status)),
            ],
        );
        Ok(info.restored_ts)
    }

    /// Go `isTaskStale`: the heartbeat unchanged over five ticks.
    fn is_task_stale(&self, ctx: &Context, task_id: u64, initial: i64) -> Result<bool> {
        let mut remaining = STALE_TASK_THRESHOLD_MINUTES;
        while remaining > 0 {
            if ctx.sleep(self.stale_check_tick) {
                return Err(errors::errorf(
                    ctx.err().map(|e| e.to_string()).unwrap_or_default(),
                ));
            }
            remaining -= 1;
            let rows = match self.rows(ctx, SELECT_TASK_HEARTBEAT_SQL, &[SqlArg::Unsigned(task_id)])
            {
                Ok(r) => r,
                Err(err) => {
                    tidb_log::warn(
                        "failed to check heartbeat during stale check, assuming task is active",
                        &[
                            Field::new("task_id", Value::U64(task_id)),
                            Field::new("error", Value::Str(err.to_string())),
                        ],
                    );
                    return Ok(false);
                }
            };
            let Some(row) = rows.first() else {
                return Ok(false);
            };
            if row[0].get_int64() != initial {
                return Ok(false);
            }
        }
        tidb_log::info(
            "task heartbeat unchanged for 5 minutes, task is stale",
            &[Field::new("task_id", Value::U64(task_id))],
        );
        Ok(true)
    }

    /// Go `transitionStaleTaskToPaused`: whether the task is paused now.
    fn transition_stale_task_to_paused(
        &self,
        ctx: &Context,
        task_id: u64,
        expected_heartbeat: i64,
    ) -> Result<bool> {
        self.execute_in_transaction(ctx, |se| {
            se.exec_restricted_sql(
                ctx,
                TRANSITION_STALE_TASK_TO_PAUSED_SQL,
                &[
                    SqlArg::Unsigned(task_id),
                    SqlArg::Signed(expected_heartbeat),
                ],
            )
            .map_err(|e| errors::annotate(e, "failed to transition stale task to paused"))?;
            let rows = se
                .exec_restricted_sql(
                    ctx,
                    "SELECT status FROM mysql.tidb_restore_registry WHERE id = %?",
                    &[SqlArg::Unsigned(task_id)],
                )
                .map_err(|e| {
                    errors::annotate(e, "failed to check task status after transition attempt")
                })?;
            Ok(rows
                .first()
                .is_some_and(|r| datum_string(&r[0]) == TaskStatus::Paused.as_str()))
        })
    }
}

impl Drop for Registry {
    fn drop(&mut self) {
        self.stop_heartbeat_manager();
    }
}

/// Go `checkForTableConflicts` for a snapshot restore without routes: a
/// table the registered task's filter matches is being restored by it. An
/// earlier PiTR task also claims the databases.
fn check_for_table_conflicts(
    dbs: &[metautil::Database],
    tables: &[metautil::Table],
    reg: &RegistrationInfoWithID,
    f: &dyn tidb_table_filter::Filter,
    cur_restore_id: u64,
) -> Result<()> {
    let info = &reg.info;
    if info.cmd == "Point Restore" {
        for db in dbs {
            let name = db.info.name.original();
            if crate::utils::filter::match_schema(f, name, info.with_sys_table) {
                tidb_log::warn(
                    "schema already covered by another restore task",
                    &[
                        Field::new("existing_restore_id", Value::U64(reg.restore_id)),
                        Field::new("current_restore_id", Value::U64(cur_restore_id)),
                        Field::new("database", Value::Str(name.into())),
                    ],
                );
                return Err(errors::annotatef(
                    &errors::ERR_DATABASES_ALREADY_EXISTED,
                    format!(
                        "database {name} cannot be restored concurrently by current task with ID {cur_restore_id} because it is already being restored by task (restoreId: {}, time range: {}->{}, cmd: {})",
                        reg.restore_id, info.start_ts, info.restored_ts, info.cmd
                    ),
                ));
            }
        }
    }
    for table in tables {
        let (db, name) = (table.db.name.original(), table.info().name.original());
        if match_table(f, db, name, info.with_sys_table) {
            tidb_log::warn(
                "table already covered by another restore task",
                &[
                    Field::new("existing_restore_id", Value::U64(reg.restore_id)),
                    Field::new("current_restore_id", Value::U64(cur_restore_id)),
                    Field::new("database", Value::Str(db.into())),
                    Field::new("table", Value::Str(name.into())),
                    Field::new("cmd", Value::Str(info.cmd.clone())),
                ],
            );
            return Err(errors::annotatef(
                &ERR_TABLES_ALREADY_EXISTED,
                format!(
                    "table {db}.{name} cannot be restored by current task with ID {cur_restore_id} because it is already being restored by task (restoreId: {}, time range: {}->{}, cmd: {})",
                    reg.restore_id, info.start_ts, info.restored_ts, info.cmd
                ),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
