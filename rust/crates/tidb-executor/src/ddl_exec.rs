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

//! `pkg/executor/ddl.go`: `DDLExec` (:52), the OPERATOR that runs one DDL
//! statement -- its `Next` (:96) dispatch and the guards that run before the
//! statement ever reaches the DDL layer.
//!
//! # What `DDLExec` actually does, and what it does not
//!
//! Nearly every `execute*` method in `ddl.go` is a one-line forward to
//! `e.ddlExecutor.X(e.Ctx(), stmt)`. The DDL layer behind that interface --
//! building the job, enqueuing it, the owner loop, schema-version bumping,
//! the multi-schema-change state machine -- is `pkg/ddl`, a different package
//! and NOT this file. Reading `ddl.go` as "the DDL implementation" is the
//! mistake this header exists to prevent.
//!
//! What `DDLExec` genuinely owns, and therefore what is ported here, is
//! everything that happens BEFORE the forward:
//!
//! 1. **The local-temporary-table fork.** A session-local temporary table
//!    lives entirely in session memory and has no DDL job at all. Every
//!    statement that can name one must first ask
//!    `getLocalTemporaryTable` (:79) and then do one of three different
//!    things -- REFUSE, REROUTE, or FILTER. [`LocalTempTableRule`] is that
//!    three-way classification, and it is the single densest piece of
//!    behaviour in the file.
//! 2. **The `DROP TABLE` list surgery** (:114-148): local temporary tables
//!    are removed from the statement's own table list before the rest is
//!    forwarded, so one `DROP TABLE t1, t2` can be half session-memory and
//!    half DDL job.
//! 3. **The statement-level gates**: the `mysql` database guard (:360), the
//!    `enable-table-lock` config gate (:729, :748), the resource-control
//!    variable gate (:798), and the two `FLASHBACK ... TO TIMESTAMP` forms
//!    that are rejected outright (:190-197).
//! 4. **`DROP DATABASE`'s session side effects** (:366-377): dropping the
//!    session's current database resets `CurrentDB` AND both database-scoped
//!    charset variables.
//! 5. **The error routing** (:236-244): whether a failure is re-interpreted
//!    as `ErrInfoSchemaChanged` depends on whether the job had already
//!    reached the queue.
//!
//! # Reused rather than restated, and what is deliberately NOT the same thing
//!
//! * [`crate::ddl`] (this crate's `ddl.rs` and `ddl/`) is the `CREATE TABLE` /
//!   `ALTER TABLE` METADATA LOWERING -- Go `pkg/ddl`'s `buildTableInfo` half.
//!   It is what a forward from here would eventually reach, not a peer of
//!   this file. This module is named `ddl_exec` rather than `ddl` for exactly
//!   that reason: `ddl.rs` was already the lowering.
//! * `tidb_exec::cluster_ddl` plans a DDL as ONE set of meta-key mutations
//!   over ONE snapshot, with NO job queue -- its own header says so. It is
//!   therefore not Go's `DDLExec` either: it is what `pkg/ddl` would do if
//!   the owner loop, the queue and the multi-version schema state machine did
//!   not exist. Nothing here assumes the two agree.
//! * [`tidb_ast::DdlStmt`] IS the parsed statement Go switches on.
//!
//! # Narrowings (each named, with the exact blocking Go symbol)
//!
//! * `ddl.Executor` (the interface `e.ddlExecutor` holds) is the whole DDL
//!   job pipeline. Every `execute*` one-liner is therefore a
//!   [`DdlAction::Forward`] here rather than a call.
//! * `getLocalTemporaryTable` (:79) needs
//!   `e.Ctx().GetInfoSchema().TableByName`; the LOOKUP is the caller's, and
//!   this module takes its answer. `temptable.TemporaryTableDDL`'s three
//!   methods (`CreateLocalTemporaryTable`, `DropLocalTemporaryTable`,
//!   `TruncateLocalTemporaryTable`) are likewise named as targets, not
//!   called.
//! * `toErr` (:63) needs `domain.NewSchemaChecker` over the domain's schema
//!   validator and the transaction's start TS. [`DdlErrorRoute`] ports the
//!   DECISION (:238-243) and names the check it would then perform.
//! * `executeRecoverTable` (:434), `getRecoverTableByJobID` (:475),
//!   `getRecoverTableByTableName` (:531), `getRecoverDBByName` (:668) and
//!   `GetDropOrTruncateTableInfoFromJobs` (:522) all SCAN THE DDL HISTORY
//!   JOB LOG backwards through `ddl.IterHistoryDDLJobs`, validating each
//!   candidate against the GC safe point and reading the table meta from a
//!   snapshot `infoschema`. That is the job queue plus PD's GC safe point
//!   plus historical snapshot reads -- three absent tiers. Only the
//!   statement-level pre-checks are ported ([`RecoverTableCheck`]).
//! * `executeCreateView` (:328) runs `core.Preprocess` over the view's
//!   SELECT to reject a stale-read view; the planner preprocessor is not
//!   reachable from here.
//! * `createSessionTemporaryTable` (:294) calls
//!   `ddl.BuildSessionTemporaryTableInfo`, which is the metadata lowering
//!   again; its guards ARE ported ([`SessionTempTableCheck`]).
//! * `executeFlashBackCluster`'s `TO TIMESTAMP` arm (:594) needs
//!   `staleread.CalculateAsOfTsExpr`; only the two rejected FORMS are ported.
//!
//! # Sequential here, sequential there
//!
//! `DDLExec::Next` is a single-shot operator: it sets `e.done` on entry and
//! returns nothing on every subsequent call. There is no concurrency in this
//! file to narrow -- the concurrency in DDL lives entirely behind
//! `ddl.Executor`, in the owner loop this module does not reach.

use tidb_ast::{CreateTableTemporary, DdlStmt, DropTemporary};

/// The operation name Go embeds in
/// `dbterror.ErrUnsupportedLocalTempTableDDL.GenWithStackByArgs(...)`.
///
/// The strings are Go's verbatim and reach the user's error message, so they
/// are the statement's SQL spelling and not the executor method's name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnsupportedLocalTempTableOp {
    /// Go :277.
    RenameTable,
    /// Go :350.
    CreateIndex,
    /// Go :414.
    DropIndex,
    /// Go :426.
    AlterTable,
    /// Go :741.
    LockTables,
    /// Go :765.
    AdminCleanupTableLock,
}

impl UnsupportedLocalTempTableOp {
    /// The exact argument Go passes.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RenameTable => "RENAME TABLE",
            Self::CreateIndex => "CREATE INDEX",
            Self::DropIndex => "DROP INDEX",
            Self::AlterTable => "ALTER TABLE",
            Self::LockTables => "LOCK TABLES",
            Self::AdminCleanupTableLock => "ADMIN CLEANUP TABLE LOCK",
        }
    }
}

/// What a statement does when one of the tables it names turns out to be a
/// SESSION-LOCAL temporary table.
///
/// The three arms are not stylistic variants; they follow from what a local
/// temporary table IS. It exists only in this session's memory, has no
/// metadata in the schema, and produces no DDL job. So:
///
/// * A statement whose meaning is a schema change on a persistent object
///   (`RENAME`, `CREATE`/`DROP INDEX`, `ALTER`, the two lock statements) has
///   no meaning at all against it, and Go REFUSES rather than silently
///   doing nothing.
/// * A statement whose meaning is expressible purely in session memory
///   (`TRUNCATE`, `CREATE TEMPORARY`, `DROP`) is REROUTED to
///   `temptable.TemporaryTableDDL`.
/// * `DROP TABLE` alone can name a MIX, so it FILTERS: the local ones are
///   split off and dropped from memory, the rest becomes a real DDL job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalTempTableRule {
    /// `dbterror.ErrUnsupportedLocalTempTableDDL`.
    Refuse(UnsupportedLocalTempTableOp),
    /// Handled by `temptable.TemporaryTableDDL.TruncateLocalTemporaryTable`
    /// (:259).
    RerouteTruncate,
    /// Handled by `temptable.TemporaryTableDDL.CreateLocalTemporaryTable`
    /// (:319) -- reached from `Next`'s pre-switch arm at :106.
    RerouteCreate,
    /// `DROP TABLE`'s split (:114-148).
    FilterDropTable,
    /// The statement cannot name a local temporary table, so no check runs.
    NotApplicable,
}

/// Go: which `execute*` arm of the `Next` switch (:159-231) a statement takes,
/// and what that arm does before forwarding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DdlAction {
    /// The statement forwards to `ddl.Executor` unchanged once its guards
    /// pass. The string is the `ddl.Executor` method Go calls.
    ///
    /// boundary: `pkg/ddl.Executor`, the DDL job pipeline.
    Forward(&'static str),
    /// Go handled the statement entirely inside `DDLExec`, without a job.
    SessionLocal,
    /// The statement is rejected before any DDL work.
    Rejected(DdlRejection),
}

/// A statement-level rejection `DDLExec` raises itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DdlRejection {
    /// Go :361 `dbterror.ErrForbiddenDDL.FastGenByArgs("Drop 'mysql' database")`.
    /// The comment at :359 is candid: no legitimate use was found, so the
    /// statement is blocked rather than gated behind a privilege.
    DropMysqlDatabase,
    /// Go :191/:193 `dbterror.ErrGeneralUnsupportedDDL`. `FLASHBACK ... TO
    /// TIMESTAMP` restores the WHOLE CLUSTER; naming tables or a database
    /// would mean a partial restore, which the mechanism cannot do.
    FlashbackToTimestampScope {
        /// Go's message tail: `"table"` (:191) or `"database"` (:193).
        scope: &'static str,
    },
    /// Go :799 `infoschema.ErrResourceGroupSupportDisabled`.
    ResourceControlDisabled,
    /// Go :275 etc.
    UnsupportedLocalTempTable(UnsupportedLocalTempTableOp),
}

/// Go `Next`'s dispatch (:159-231), plus the pre-switch fork at :104-148.
///
/// The classification is complete for the statements Go's switch names. A
/// statement Go's switch does NOT name falls out of the switch with `err`
/// still nil and reaches :246 as a SUCCESS -- Go has no default arm. That is
/// reproduced as `None` rather than as a rejection, because turning a silent
/// no-op into an error would change behaviour.
#[must_use]
pub fn classify(statement: &DdlStmt) -> Option<DdlAction> {
    Some(match statement {
        // :106: a LOCAL temporary CREATE TABLE returns before the txn is even
        // started, so it never becomes a DDL job.
        DdlStmt::CreateTable(create) => {
            if create.temporary == CreateTableTemporary::Local {
                DdlAction::SessionLocal
            } else {
                DdlAction::Forward("CreateTable")
            }
        }
        DdlStmt::CreateView(_) => DdlAction::Forward("CreateView"),
        DdlStmt::CreateIndex(_) => DdlAction::Forward("CreateIndex"),
        DdlStmt::DropIndex(_) => DdlAction::Forward("DropIndex"),
        DdlStmt::CreateDatabase { .. } => DdlAction::Forward("CreateSchema"),
        DdlStmt::AlterDatabase { .. } => DdlAction::Forward("AlterSchema"),
        DdlStmt::AlterTable(_) => DdlAction::Forward("AlterTable"),
        DdlStmt::RenameTable(_) => DdlAction::Forward("RenameTable"),
        DdlStmt::LockTables(_) => DdlAction::Forward("LockTables"),
        DdlStmt::UnlockTables => DdlAction::Forward("UnlockTables"),
        DdlStmt::DropTable(_) => DdlAction::Forward("DropTable"),
        DdlStmt::DropView { .. } => DdlAction::Forward("DropView"),
        DdlStmt::DropDatabase { name, .. } => {
            if name.eq_ignore_ascii_case("mysql") {
                DdlAction::Rejected(DdlRejection::DropMysqlDatabase)
            } else {
                DdlAction::Forward("DropSchema")
            }
        }
        DdlStmt::TruncateTable(_) => DdlAction::Forward("TruncateTable"),
        DdlStmt::CreateSequence(_) => DdlAction::Forward("CreateSequence"),
        DdlStmt::AlterSequence(_) => DdlAction::Forward("AlterSequence"),
        DdlStmt::DropSequence(_) => DdlAction::Forward("DropSequence"),
        DdlStmt::CreatePlacementPolicy(_) => DdlAction::Forward("CreatePlacementPolicy"),
        DdlStmt::AlterPlacementPolicy(_) => DdlAction::Forward("AlterPlacementPolicy"),
        DdlStmt::DropPlacementPolicy(_) => DdlAction::Forward("DropPlacementPolicy"),
        DdlStmt::CreateMaskingPolicy(_) => DdlAction::Forward("CreateMaskingPolicy"),
        DdlStmt::CreateResourceGroup(_) => DdlAction::Forward("AddResourceGroup"),
        DdlStmt::AlterResourceGroup(_) => DdlAction::Forward("AlterResourceGroup"),
        DdlStmt::DropResourceGroup(_) => DdlAction::Forward("DropResourceGroup"),
        DdlStmt::RecoverTable(_) => DdlAction::Forward("RecoverTable"),
        DdlStmt::FlashbackTable(_) => DdlAction::Forward("RecoverTable"),
        DdlStmt::FlashbackDatabase(_) => DdlAction::Forward("RecoverSchema"),
        DdlStmt::RepairTable(_) => DdlAction::Forward("RepairTable"),
        // Go's switch has no arm for these, so they leave `Next` succeeding
        // without having done anything.
        _ => return None,
    })
}

/// Go `Next` :189-197: which `FLASHBACK ... TO TIMESTAMP` forms are refused.
///
/// The statement restores the whole cluster to a point in time by rewriting
/// every region's data; there is no per-table or per-database variant of that
/// mechanism, so naming either is rejected rather than silently widened.
#[must_use]
pub fn flashback_to_timestamp_action(has_tables: bool, has_db_name: bool) -> DdlAction {
    // Go tests the table list FIRST, so `FLASHBACK TABLE ... TO TIMESTAMP`
    // inside a named database reports the table message.
    if has_tables {
        DdlAction::Rejected(DdlRejection::FlashbackToTimestampScope { scope: "table" })
    } else if has_db_name {
        DdlAction::Rejected(DdlRejection::FlashbackToTimestampScope { scope: "database" })
    } else {
        DdlAction::Forward("FlashbackCluster")
    }
}

/// Go :797-815: the gate all three resource-group statements share.
///
/// `InRestrictedSQL` bypasses the variable because bootstrap and internal
/// maintenance SQL must be able to touch resource groups even when the
/// feature is switched off for users.
#[must_use]
pub fn resource_group_action(
    enable_resource_control: bool,
    in_restricted_sql: bool,
    forward: &'static str,
) -> DdlAction {
    if enable_resource_control || in_restricted_sql {
        DdlAction::Forward(forward)
    } else {
        DdlAction::Rejected(DdlRejection::ResourceControlDisabled)
    }
}

/// Go :728-732 and :747-751: the `enable-table-lock` config gate.
///
/// `false` means the statement produces a WARNING and succeeds -- it is not
/// an error. `LOCK TABLES` against a server without table locks is a no-op
/// with a note, which is what lets a dump/restore script written for MySQL
/// run unchanged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableLockGate {
    /// Proceed to the local-temporary check and then forward.
    Enabled,
    /// Go `exeerrors.ErrFuncNotEnabled.FastGenByArgs(op, "enable-table-lock")`
    /// appended as a warning, then `return nil`.
    WarnAndSucceed {
        /// Go's first argument: `"LOCK TABLES"` or `"UNLOCK TABLES"`.
        operation: &'static str,
    },
}

/// Go :729 / :748.
#[must_use]
pub const fn table_lock_gate(enabled: bool, unlocking: bool) -> TableLockGate {
    if enabled {
        TableLockGate::Enabled
    } else {
        TableLockGate::WarnAndSucceed {
            operation: if unlocking {
                "UNLOCK TABLES"
            } else {
                "LOCK TABLES"
            },
        }
    }
}

/// What `DROP TABLE` does once its table list has been split, Go :114-148.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DropTablePlan {
    /// Every named table was session-local: `dropLocalTemporaryTables` alone
    /// (:146). No DDL job at all.
    LocalOnly {
        /// The local temporary tables, in the order Go accumulated them.
        local: Vec<usize>,
    },
    /// Go :150 onward: forward the REMAINING names to `ddl.DropTable`, then
    /// drop the local ones (:184-186) -- in that order, and only if the
    /// forward succeeded.
    Split {
        /// Indices, into the original name list, that stay in the statement.
        remaining: Vec<usize>,
        /// Indices handled from session memory.
        local: Vec<usize>,
    },
    /// Go :129-142: `DROP TEMPORARY TABLE` naming something that is not a
    /// local temporary table.
    TemporaryNamesNotLocal {
        /// The offending names' indices, in statement order.
        missing: Vec<usize>,
        /// Go :137-140: with `IF EXISTS` the error becomes a NOTE and the
        /// statement succeeds; without it the statement fails.
        as_note: bool,
    },
}

/// Go `Next` :112-148: split a `DROP TABLE`'s names by local-temporariness.
///
/// `is_local_temporary` is the per-name answer of `getLocalTemporaryTable`.
///
/// Go walks the list BACKWARDS (`for tbIdx := len(s.Tables) - 1; tbIdx >= 0`)
/// because it deletes from the slice as it goes. The accumulated local list is
/// therefore in REVERSE statement order, which is preserved here: the drops
/// happen in that order and a later error leaves a different prefix dropped.
///
/// `is_view` (:110) short-circuits the whole fork: `DROP VIEW` shares the
/// statement type but a view is never a local temporary table.
#[must_use]
pub fn plan_drop_table(
    name_count: usize,
    temporary: DropTemporary,
    if_exists: bool,
    is_view: bool,
    is_local_temporary: &dyn Fn(usize) -> bool,
) -> DropTablePlan {
    if is_view {
        return DropTablePlan::Split {
            remaining: (0..name_count).collect(),
            local: Vec::new(),
        };
    }
    let mut local = Vec::new();
    let mut remaining: Vec<usize> = (0..name_count).collect();
    for index in (0..name_count).rev() {
        if is_local_temporary(index) {
            local.push(index);
            remaining.retain(|kept| *kept != index);
        }
    }

    // :129-142. Only `DROP TEMPORARY TABLE` reaches this: a plain `DROP
    // TABLE` naming a persistent table is an ordinary DDL job.
    if temporary == DropTemporary::Local && !remaining.is_empty() {
        return DropTablePlan::TemporaryNamesNotLocal {
            missing: remaining,
            as_note: if_exists,
        };
    }
    if remaining.is_empty() {
        return DropTablePlan::LocalOnly { local };
    }
    DropTablePlan::Split { remaining, local }
}

/// Go `executeDropDatabase` :366-377: what dropping a database does to the
/// session once the DDL itself succeeded.
///
/// The charset reset is not cosmetic. `character_set_database` and
/// `collation_database` describe the CURRENT database; with no current
/// database they must fall back to the server defaults, or the next
/// `CREATE TABLE` in a freshly selected database would inherit the dropped
/// one's charset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropDatabaseSessionReset {
    /// Go sets `sessionVars.CurrentDB = ""`.
    pub clear_current_db: bool,
    /// Go sets `vardef.CharsetDatabase` to `mysql.DefaultCharset`.
    pub charset_database: Option<&'static str>,
    /// Go sets `vardef.CollationDatabase` to `mysql.DefaultCollationName`.
    pub collation_database: Option<&'static str>,
}

/// Go `executeDropDatabase` :366.
///
/// The comparison is `strings.ToLower(sessionVars.CurrentDB) == dbName.L`:
/// both sides lower-cased, so the reset fires regardless of how either name
/// was spelled. It fires ONLY when the DDL succeeded (`err == nil`), so a
/// failed drop leaves the session pointing at a database that still exists.
#[must_use]
pub fn drop_database_session_reset(
    current_db: &str,
    dropped_db: &str,
    ddl_succeeded: bool,
) -> DropDatabaseSessionReset {
    if ddl_succeeded && current_db.eq_ignore_ascii_case(dropped_db) {
        DropDatabaseSessionReset {
            clear_current_db: true,
            charset_database: Some("utf8mb4"),
            collation_database: Some("utf8mb4_bin"),
        }
    } else {
        DropDatabaseSessionReset {
            clear_current_db: false,
            charset_database: None,
            collation_database: None,
        }
    }
}

/// Go `Next` :236-244: how a DDL failure is reported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DdlErrorRoute {
    /// Go `e.toErr(err)`: re-check the schema version against the
    /// transaction's start TS and, if it moved, report
    /// `ErrInfoSchemaChanged` instead of the original error.
    ///
    /// boundary: Go `domain.NewSchemaChecker(dom.GetSchemaValidator(),
    /// e.is.SchemaMetaVersion(), nil, true).Check(txn.StartTS())`.
    RecheckSchema,
    /// Return the original error untouched.
    Original,
}

/// Go `Next` :239-243.
///
/// The reasoning is Go's comment at :237 and it is a genuine ambiguity rather
/// than a heuristic. `ErrTableNotExists` can arise in two places: BEFORE the
/// job is queued, where it means the table really is absent; or from the DDL
/// OWNER after queuing, where it can equally mean another session dropped the
/// table concurrently -- i.e. a schema change. So:
///
/// * job never queued -> any error is the executor's own, re-check anyway.
/// * job queued and the error is `ErrTableNotExists` -> ambiguous, re-check.
/// * job queued and the error is anything else -> the owner's verdict, report
///   it as-is.
#[must_use]
pub const fn ddl_error_route(job_in_queue: bool, is_table_not_exists: bool) -> DdlErrorRoute {
    if !job_in_queue || is_table_not_exists {
        DdlErrorRoute::RecheckSchema
    } else {
        DdlErrorRoute::Original
    }
}

/// Go `createSessionTemporaryTable` :295-312: the two checks before the
/// metadata is built.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionTempTableCheck {
    /// Go :298 `infoschema.ErrDatabaseNotExists`. A session-local temporary
    /// table still needs a real database to live in.
    DatabaseNotExists,
    /// Go :307 `infoschema.ErrTableExists`, or with `IF NOT EXISTS` a NOTE
    /// and success (:308-311).
    TableExists {
        /// Whether `IF NOT EXISTS` demoted the error to a note.
        as_note: bool,
    },
    /// Proceed to `ddl.BuildSessionTemporaryTableInfo` and then
    /// `CreateLocalTemporaryTable`.
    ///
    /// boundary: Go `pkg/ddl.BuildSessionTemporaryTableInfo`, then
    /// `sessiontxn.GetTxnManager(...).OnLocalTemporaryTableCreated()` (:323).
    Proceed,
}

/// Go `createSessionTemporaryTable` :295-312.
#[must_use]
pub const fn session_temp_table_check(
    database_exists: bool,
    table_exists: bool,
    if_not_exists: bool,
) -> SessionTempTableCheck {
    if !database_exists {
        return SessionTempTableCheck::DatabaseNotExists;
    }
    if table_exists {
        return SessionTempTableCheck::TableExists {
            as_note: if_not_exists,
        };
    }
    SessionTempTableCheck::Proceed
}

/// Go `executeRecoverTable` :448-451 and `executeFlashbackTable` :614-618:
/// the check that runs after the dropped table's meta is recovered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoverTableCheck {
    /// Go `infoschema.ErrTableExists.GenWithStack("Table '%-.192s' already
    /// been recover to '%-.192s', can't be recover repeatedly", ...)`.
    ///
    /// The check is by table ID rather than by name, which is what makes it a
    /// REPEAT detector: a recovered table keeps its original ID, so finding
    /// that ID in the current schema means this exact recovery already ran.
    AlreadyRecovered {
        /// The name the recovered meta carries.
        recovered_name: String,
        /// The name that ID currently holds, which a `FLASHBACK ... TO name`
        /// may have changed.
        existing_name: String,
    },
    /// Proceed to `ddl.Executor.RecoverTable`.
    Proceed,
}

/// Go :448-451 / :614-618.
#[must_use]
pub fn recover_table_check(
    recovered_name: &str,
    existing_table_with_same_id: Option<&str>,
) -> RecoverTableCheck {
    match existing_table_with_same_id {
        Some(existing_name) => RecoverTableCheck::AlreadyRecovered {
            recovered_name: recovered_name.to_owned(),
            existing_name: existing_name.to_owned(),
        },
        None => RecoverTableCheck::Proceed,
    }
}

/// Go `executeFlashbackDatabase` :646-660: the two existence checks, in Go's
/// order.
///
/// Both raise `infoschema.ErrDatabaseExists`, but they detect different
/// things: the NAME check rejects flashing back onto an occupied name, while
/// the ID check (:658) rejects flashing the SAME database back twice, since a
/// recovered schema keeps its ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlashbackDatabaseCheck {
    /// Go :650 -- the target name is taken.
    NameExists,
    /// Go :658 -- this schema ID has already been recovered.
    AlreadyRecovered,
    /// Proceed to `ddl.Executor.RecoverSchema`.
    Proceed,
}

/// Go :646-660. `target_name` is `s.NewName` when given and `s.DBName`
/// otherwise (:643-646), so `FLASHBACK DATABASE d TO d2` checks `d2`.
#[must_use]
pub const fn flashback_database_check(
    target_name_exists: bool,
    schema_id_exists: bool,
) -> FlashbackDatabaseCheck {
    if target_name_exists {
        FlashbackDatabaseCheck::NameExists
    } else if schema_id_exists {
        FlashbackDatabaseCheck::AlreadyRecovered
    } else {
        FlashbackDatabaseCheck::Proceed
    }
}

/// The local-temporary-table rule for a parsed statement, Go's per-`execute*`
/// `getLocalTemporaryTable` calls collected into one place.
#[must_use]
pub fn local_temp_table_rule(statement: &DdlStmt) -> LocalTempTableRule {
    match statement {
        DdlStmt::RenameTable(_) => {
            LocalTempTableRule::Refuse(UnsupportedLocalTempTableOp::RenameTable)
        }
        DdlStmt::CreateIndex(_) => {
            LocalTempTableRule::Refuse(UnsupportedLocalTempTableOp::CreateIndex)
        }
        DdlStmt::DropIndex(_) => LocalTempTableRule::Refuse(UnsupportedLocalTempTableOp::DropIndex),
        DdlStmt::AlterTable(_) => {
            LocalTempTableRule::Refuse(UnsupportedLocalTempTableOp::AlterTable)
        }
        DdlStmt::LockTables(_) => {
            LocalTempTableRule::Refuse(UnsupportedLocalTempTableOp::LockTables)
        }
        DdlStmt::TruncateTable(_) => LocalTempTableRule::RerouteTruncate,
        DdlStmt::CreateTable(create) => {
            if create.temporary == CreateTableTemporary::Local {
                LocalTempTableRule::RerouteCreate
            } else {
                // Go never checks: a non-local CREATE TABLE naming an existing
                // local temporary table is the DDL layer's duplicate-name
                // problem, not this fork's.
                LocalTempTableRule::NotApplicable
            }
        }
        DdlStmt::DropTable(_) => LocalTempTableRule::FilterDropTable,
        _ => LocalTempTableRule::NotApplicable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // WRITTEN test for :114-148: the DROP TABLE split, including Go's
    // backwards walk.
    #[test]
    fn drop_table_splits_local_temporaries_in_reverse_order() {
        // t0 persistent, t1 local, t2 local.
        let is_local = |index: usize| index > 0;
        let plan = plan_drop_table(3, DropTemporary::None, false, false, &is_local);
        assert_eq!(
            plan,
            DropTablePlan::Split {
                remaining: vec![0],
                // Go accumulates while walking backwards, so 2 precedes 1.
                local: vec![2, 1],
            }
        );

        // Every name local: no DDL job at all.
        let plan = plan_drop_table(2, DropTemporary::None, false, false, &|_| true);
        assert_eq!(plan, DropTablePlan::LocalOnly { local: vec![1, 0] });

        // No name local: an ordinary DROP TABLE.
        let plan = plan_drop_table(2, DropTemporary::None, false, false, &|_| false);
        assert_eq!(
            plan,
            DropTablePlan::Split {
                remaining: vec![0, 1],
                local: vec![],
            }
        );
    }

    // WRITTEN test for :129-142: DROP TEMPORARY TABLE naming a non-local
    // table, with and without IF EXISTS.
    #[test]
    fn drop_temporary_table_rejects_non_local_names() {
        let plan = plan_drop_table(2, DropTemporary::Local, false, false, &|index| index == 1);
        assert_eq!(
            plan,
            DropTablePlan::TemporaryNamesNotLocal {
                missing: vec![0],
                as_note: false,
            }
        );
        let plan = plan_drop_table(2, DropTemporary::Local, true, false, &|index| index == 1);
        assert_eq!(
            plan,
            DropTablePlan::TemporaryNamesNotLocal {
                missing: vec![0],
                as_note: true,
            }
        );
        // All local: the check at :129 never fires because s.Tables is empty.
        let plan = plan_drop_table(2, DropTemporary::Local, false, false, &|_| true);
        assert_eq!(plan, DropTablePlan::LocalOnly { local: vec![1, 0] });
    }

    // WRITTEN test for :110: DROP VIEW skips the fork entirely.
    #[test]
    fn drop_view_never_consults_the_local_temporary_fork() {
        let plan = plan_drop_table(2, DropTemporary::None, false, true, &|_| {
            panic!("DROP VIEW must not consult getLocalTemporaryTable")
        });
        assert_eq!(
            plan,
            DropTablePlan::Split {
                remaining: vec![0, 1],
                local: vec![],
            }
        );
    }

    // WRITTEN test for :189-197.
    #[test]
    fn flashback_to_timestamp_rejects_narrower_scopes() {
        assert_eq!(
            flashback_to_timestamp_action(true, false),
            DdlAction::Rejected(DdlRejection::FlashbackToTimestampScope { scope: "table" })
        );
        assert_eq!(
            flashback_to_timestamp_action(false, true),
            DdlAction::Rejected(DdlRejection::FlashbackToTimestampScope { scope: "database" })
        );
        // Tables are tested first, so a table list wins over a database name.
        assert_eq!(
            flashback_to_timestamp_action(true, true),
            DdlAction::Rejected(DdlRejection::FlashbackToTimestampScope { scope: "table" })
        );
        assert_eq!(
            flashback_to_timestamp_action(false, false),
            DdlAction::Forward("FlashbackCluster")
        );
    }

    // WRITTEN test for :797-815: InRestrictedSQL bypasses the variable.
    #[test]
    fn resource_group_gate_lets_internal_sql_through() {
        assert_eq!(
            resource_group_action(false, false, "AddResourceGroup"),
            DdlAction::Rejected(DdlRejection::ResourceControlDisabled)
        );
        assert_eq!(
            resource_group_action(true, false, "AddResourceGroup"),
            DdlAction::Forward("AddResourceGroup")
        );
        assert_eq!(
            resource_group_action(false, true, "AddResourceGroup"),
            DdlAction::Forward("AddResourceGroup")
        );
    }

    // WRITTEN test for :728-751: a disabled table lock WARNS, it does not
    // fail.
    #[test]
    fn table_lock_gate_warns_rather_than_erroring() {
        assert_eq!(table_lock_gate(true, false), TableLockGate::Enabled);
        assert_eq!(
            table_lock_gate(false, false),
            TableLockGate::WarnAndSucceed {
                operation: "LOCK TABLES"
            }
        );
        assert_eq!(
            table_lock_gate(false, true),
            TableLockGate::WarnAndSucceed {
                operation: "UNLOCK TABLES"
            }
        );
    }

    // WRITTEN test for :366-377: the reset is case-insensitive on both sides
    // and only fires on success.
    #[test]
    fn dropping_the_current_database_resets_the_charset_variables() {
        let reset = drop_database_session_reset("MyDb", "mydb", true);
        assert!(reset.clear_current_db);
        assert_eq!(reset.charset_database, Some("utf8mb4"));
        assert_eq!(reset.collation_database, Some("utf8mb4_bin"));

        // A failed DROP leaves the session alone.
        let reset = drop_database_session_reset("mydb", "mydb", false);
        assert!(!reset.clear_current_db);
        assert_eq!(reset.charset_database, None);

        let reset = drop_database_session_reset("other", "mydb", true);
        assert!(!reset.clear_current_db);
    }

    // WRITTEN test for :239-243: the three-way error routing.
    #[test]
    fn error_route_rechecks_unless_the_owner_gave_a_definite_verdict() {
        // Never queued: the error is ours, re-check anyway.
        assert_eq!(ddl_error_route(false, false), DdlErrorRoute::RecheckSchema);
        assert_eq!(ddl_error_route(false, true), DdlErrorRoute::RecheckSchema);
        // Queued and ErrTableNotExists: ambiguous.
        assert_eq!(ddl_error_route(true, true), DdlErrorRoute::RecheckSchema);
        // Queued and something else: the owner's verdict stands.
        assert_eq!(ddl_error_route(true, false), DdlErrorRoute::Original);
    }

    // WRITTEN test for :295-312.
    #[test]
    fn session_temp_table_needs_a_real_database() {
        assert_eq!(
            session_temp_table_check(false, false, false),
            SessionTempTableCheck::DatabaseNotExists
        );
        assert_eq!(
            session_temp_table_check(true, true, false),
            SessionTempTableCheck::TableExists { as_note: false }
        );
        assert_eq!(
            session_temp_table_check(true, true, true),
            SessionTempTableCheck::TableExists { as_note: true }
        );
        assert_eq!(
            session_temp_table_check(true, false, false),
            SessionTempTableCheck::Proceed
        );
    }

    // WRITTEN test for :448-451 and :646-660.
    #[test]
    fn recovery_checks_detect_repeats_by_id() {
        assert_eq!(recover_table_check("t", None), RecoverTableCheck::Proceed);
        assert_eq!(
            recover_table_check("t", Some("t_renamed")),
            RecoverTableCheck::AlreadyRecovered {
                recovered_name: "t".to_owned(),
                existing_name: "t_renamed".to_owned(),
            }
        );

        assert_eq!(
            flashback_database_check(true, false),
            FlashbackDatabaseCheck::NameExists
        );
        assert_eq!(
            flashback_database_check(false, true),
            FlashbackDatabaseCheck::AlreadyRecovered
        );
        assert_eq!(
            flashback_database_check(false, false),
            FlashbackDatabaseCheck::Proceed
        );
    }

    // WRITTEN test for the statement-level dispatch and the `mysql` guard.
    #[test]
    fn dispatch_blocks_dropping_the_mysql_database() {
        let drop_mysql = DdlStmt::DropDatabase {
            if_exists: false,
            name: "MySQL".to_owned(),
        };
        assert_eq!(
            classify(&drop_mysql),
            Some(DdlAction::Rejected(DdlRejection::DropMysqlDatabase))
        );
        let drop_other = DdlStmt::DropDatabase {
            if_exists: false,
            name: "app".to_owned(),
        };
        assert_eq!(
            classify(&drop_other),
            Some(DdlAction::Forward("DropSchema"))
        );
        assert_eq!(
            classify(&DdlStmt::UnlockTables),
            Some(DdlAction::Forward("UnlockTables"))
        );
    }
}
