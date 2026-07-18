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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Per-statement session machinery: [`SessionState`] (the read-once
//! bundle of clock/system-variable/user-variable values every resolver
//! shares) and [`RelResolver`] (the `Columns` implementation that resolves
//! column references against one relation row, chains to an outer scope
//! for correlated subqueries, and serves session lookups).

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use tidb_datatype::Datum;
use tidb_expr::{Columns, MysqlRng};

use tidb_expr::EvalError;

use crate::catalog::{table_key, Column};
use crate::sequence::Sequence;
use crate::session_settings::{
    DivPrecisionIncrement, MultiStatementMode, NoopFunctionsMode, SqlSelectLimit, TimeZoneSetting,
};

/// The read-once-per-statement session values a [`RelResolver`] needs
/// beyond its own row: the fixed clock (see [`Database::now_value`]), the
/// handful of `@@variable` values [`Columns::sysvar`] serves, and a
/// snapshot of every `@variable` [`Columns::get_uservar`] serves (see
/// [`Database::session_state`]'s own doc for exactly which). Bundled into
/// ONE struct — rather than a growing list of constructor parameters —
/// so every EXISTING `RelResolver::new`/`with_outer` call site needed
/// only its `now`-producing expression swapped for a
/// `SessionState`-producing one, not a new argument threaded through.
/// `Clone`, NOT `Copy` — cloning shares `user_vars`' underlying
/// `Rc<RefCell<...>>` cell (cheap — a refcount bump, not a deep copy),
/// which is exactly what makes the inline `@x := expr` ASSIGNMENT
/// EXPRESSION (`tidb_ast::Expr::Assign`, see `Columns::set_uservar`'s
/// own doc) work: every `RelResolver` built from the SAME
/// `SessionState` — one per row, including nested subquery resolvers —
/// mutates and observes the ONE shared map, and since it is the SAME
/// `Rc` `Database::user_vars` itself holds (see that field's own doc),
/// a write made here is automatically live in `Database` too, with no
/// separate "persist after the statement" step. A handful of
/// construction sites that reuse ONE `SessionState` across a
/// loop/closure call `.clone()` explicitly where they used to rely on
/// an implicit `Copy`.
#[derive(Debug, Clone)]
pub(crate) struct SessionState {
    pub(crate) now: Option<(i64, u32, i32)>,
    /// The source-preserving @@timestamp value for this statement. Dynamic
    /// timestamp mode is materialized from the same cached clock as now.
    pub(crate) timestamp: String,
    pub(crate) autocommit: bool,
    pub(crate) time_zone: TimeZoneSetting,
    pub(crate) foreign_key_checks: bool,
    pub(crate) sql_safe_updates: bool,
    pub(crate) tx_isolation: String,
    pub(crate) tx_isolation_one_shot: String,
    pub(crate) noop_functions_mode: NoopFunctionsMode,
    pub(crate) multi_statement_mode: MultiStatementMode,
    pub(crate) tx_read_only: bool,
    /// The preceding statement's affected-row status served by `ROW_COUNT()`.
    pub(crate) previous_affected_rows: i64,
    pub(crate) previous_last_insert_id: u64,
    pub(crate) statement_last_insert_id: Option<Rc<RefCell<Option<u64>>>>,
    pub(crate) sql_select_limit: SqlSelectLimit,
    pub(crate) default_week_format: u8,
    pub(crate) div_precision_increment: DivPrecisionIncrement,
    pub(crate) rng: Option<Rc<RefCell<MysqlRng>>>,
    pub(crate) statement_rngs: Option<Rc<RefCell<BTreeMap<usize, MysqlRng>>>>,
    pub(crate) user_vars: Rc<RefCell<BTreeMap<String, Datum>>>,
    /// The live, shared sequence catalog and `LASTVAL` session state
    /// (see [`crate::Database::sequences`]/[`crate::Database::seq_lastval`]'s
    /// own docs) — `NEXTVAL`/`SETVAL` mutate through these cells during
    /// evaluation, the same interior-mutability architecture `user_vars`
    /// established.
    pub(crate) sequences: Rc<RefCell<BTreeMap<String, Sequence>>>,
    pub(crate) seq_lastval: Rc<RefCell<BTreeMap<String, i64>>>,
}

impl Default for SessionState {
    /// Real MySQL/TiDB's own out-of-the-box session defaults (`autocommit`
    /// on, `time_zone` unset/`"SYSTEM"`, `tx_isolation` at
    /// `"REPEATABLE-READ"`, `tx_isolation_one_shot` unset, every user
    /// variable unset) — used by the fully-stateless `crate::setopr::execute`
    /// path (no `Database` at all), for which
    /// these ARE the correct, deterministic answer (unlike `now`, which
    /// has no meaningful default and stays `None` there). `user_vars`
    /// gets its OWN fresh, unshared cell here (nothing else could share
    /// it — there is no `Database` to hold a matching `Rc`), so `@x :=
    /// expr` still works WITHIN one such statement (matching `gorun`'s
    /// own observed `SELECT @i := 1, @i + 1` => `1|2` even with no
    /// table/session at all) but never persists beyond it — the correct
    /// behavior, since there is nowhere for it to persist TO.
    fn default() -> Self {
        SessionState {
            now: None,
            timestamp: String::new(),
            autocommit: true,
            time_zone: TimeZoneSetting::System,
            foreign_key_checks: true,
            sql_safe_updates: false,
            tx_isolation: "REPEATABLE-READ".to_string(),
            tx_isolation_one_shot: String::new(),
            noop_functions_mode: NoopFunctionsMode::Off,
            multi_statement_mode: MultiStatementMode::Off,
            tx_read_only: false,
            previous_affected_rows: 0,
            previous_last_insert_id: 0,
            statement_last_insert_id: Some(Rc::new(RefCell::new(None))),
            sql_select_limit: SqlSelectLimit::UNLIMITED,
            default_week_format: 0,
            div_precision_increment: DivPrecisionIncrement::DEFAULT,
            rng: None,
            statement_rngs: None,
            user_vars: Rc::new(RefCell::new(BTreeMap::new())),
            // Fresh, empty cells for the same reason as `user_vars`: with
            // no `Database`, there is no sequence catalog — every
            // `NEXTVAL`/`LASTVAL`/`SETVAL` answers "unknown sequence".
            sequences: Rc::new(RefCell::new(BTreeMap::new())),
            seq_lastval: Rc::new(RefCell::new(BTreeMap::new())),
        }
    }
}

impl SessionState {
    /// Serves [`Columns::sysvar`] for `RelResolver` — see that trait
    /// method's own doc for the permanent scope boundary (only
    /// `autocommit`/`time_zone`/`tx_isolation` (also exposed as
    /// `transaction_isolation`)/`tx_isolation_one_shot`/
    /// `tidb_enable_noop_functions`/`foreign_key_checks`/`sql_safe_updates`/`sql_select_limit`/`tx_read_only` (also exposed as
    /// `transaction_read_only`)
    /// are modelled, everything else `None`). `@@GLOBAL.*` always
    /// answers with the FIXED default regardless of this session's own
    /// state (confirmed via `gorun`: `SET autocommit=0` leaves
    /// `@@global.autocommit` at `1`, and likewise `SET SESSION
    /// TRANSACTION ISOLATION LEVEL ...` leaves `@@global.tx_isolation`
    /// at `"REPEATABLE-READ"`) — this executor has no separate global-
    /// vs-session variable store, and `SET GLOBAL` is already rejected
    /// elsewhere, so "global" here just means "as if never touched".
    /// `tx_isolation_one_shot` has NO global form at all (see
    /// [`crate::transaction::TransactionState`]) — `global` is
    /// simply never consulted for it, matching real TiDB's own
    /// `@@global.tx_isolation_one_shot` genuine `ERR`.
    fn sysvar(&self, scope: Option<tidb_ast::SysVarScope>, name: &str) -> Option<Datum> {
        let global = scope == Some(tidb_ast::SysVarScope::Global);
        match name.to_ascii_lowercase().as_str() {
            "autocommit" => Some(Datum::Int(i64::from(if global {
                true
            } else {
                self.autocommit
            }))),
            "time_zone" => Some(Datum::new_string(if global {
                "SYSTEM".to_string()
            } else {
                self.time_zone.readback()
            })),
            "foreign_key_checks" => Some(Datum::Int(i64::from(if global {
                true
            } else {
                self.foreign_key_checks
            }))),
            "sql_safe_updates" => Some(Datum::Int(i64::from(if global {
                false
            } else {
                self.sql_safe_updates
            }))),
            "default_week_format" => Some(Datum::Int(i64::from(if global {
                0
            } else {
                self.default_week_format
            }))),
            // Both source variables are SESSION-only TypeUnsigned aliases
            // over `StmtCtx.PrevLastInsertID` (sysvar.go:157-165). A GLOBAL
            // reference is therefore genuinely unknown, not a fake default.
            "last_insert_id" | "identity" if !global => {
                Some(Datum::UInt(self.previous_last_insert_id))
            }
            // `sql_select_limit` is TypeUnsigned with MaxUint64 as both its
            // session/global default and its no-limit sentinel. A global read
            // uses that fixed default because this seed intentionally has no
            // mutable global-variable store; do not stringify it and lose its
            // UInt64 expression domain.
            "sql_select_limit" => Some(Datum::UInt(if global {
                u64::MAX
            } else {
                self.sql_select_limit.value()
            })),
            "div_precision_increment" => Some(Datum::Int(i64::from(if global {
                DivPrecisionIncrement::DEFAULT.value()
            } else {
                self.div_precision_increment.value()
            }))),
            // TiDB deliberately reports the literal default rather than the
            // mutable generator state for these session-only seed setters.
            // State serialization reads the internal seeds through a
            // different source hook that this executor does not expose.
            "rand_seed1" | "rand_seed2" if !global => Some(Datum::Int(0)),
            // timestamp is SESSION-only in TiDB. Its dynamic/default
            // readback was materialized from this statement's one cached
            // clock by Database::session_state, so @@timestamp and NOW()
            // cannot observe different instants.
            "timestamp" if !global => Some(Datum::new_string(self.timestamp.clone())),
            // Go registers `tx_isolation` and `transaction_isolation` as
            // mutual aliases of one system-variable value (see
            // `pkg/sessionctx/variable/sysvar.go`).  They must therefore
            // read the same session value and the same fixed global default;
            // keeping one field makes that invariant structural.
            "tx_isolation" | "transaction_isolation" => Some(Datum::new_string(if global {
                "REPEATABLE-READ".to_string()
            } else {
                self.tx_isolation.clone()
            })),
            "tx_isolation_one_shot" if !global => {
                Some(Datum::new_string(self.tx_isolation_one_shot.clone()))
            }
            // These aliases are compatibility-only variables in TiDB's own
            // `noop.go`: their value is observable, but turning one on does
            // not change DML behavior. One bool makes it impossible for the
            // aliases to diverge. This seed deliberately has no writable
            // global-variable store, so GLOBAL reads keep TiDB's default OFF.
            "tx_read_only" | "transaction_read_only" => {
                Some(Datum::Int(i64::from(!global && self.tx_read_only)))
            }
            "tidb_enable_noop_functions" => Some(Datum::new_string(
                if global {
                    NoopFunctionsMode::Off
                } else {
                    self.noop_functions_mode
                }
                .label()
                .to_string(),
            )),
            "tidb_multi_statement_mode" => Some(Datum::new_string(
                if global {
                    MultiStatementMode::Off
                } else {
                    self.multi_statement_mode
                }
                .label()
                .to_string(),
            )),
            _ => None,
        }
    }

    /// Serves [`Columns::get_uservar`] for `RelResolver` — a plain,
    /// case-insensitive lookup against the LIVE, shared
    /// [`Database::user_vars`] cell (see that field's own doc — reads
    /// whatever the most recent write, top-level `SET` or inline `:=`,
    /// left there, not a stale per-statement snapshot). Always `Some`
    /// (possibly `Datum::Null` for a name never assigned), never `None`
    /// — a real session ALWAYS tracks user variables, unlike `sysvar`'s
    /// deliberately narrower, name-restricted domain.
    fn get_uservar(&self, name: &str) -> Option<Datum> {
        Some(
            self.user_vars
                .borrow()
                .get(&name.to_ascii_lowercase())
                .cloned()
                .unwrap_or(Datum::Null),
        )
    }

    /// Serves [`Columns::set_uservar`] for `RelResolver` — writes
    /// through to the SAME shared, `Rc`-cloned cell every other
    /// `RelResolver`/`Database::user_vars` reads (see that field's own
    /// doc), so the write is immediately visible to a later select-list
    /// item in the same row, a later row in the same scan, a nested
    /// subquery's own resolver, and — once the statement finishes — the
    /// `Database` itself, all without any extra plumbing.
    fn set_uservar(&self, name: &str, value: Datum) {
        self.user_vars
            .borrow_mut()
            .insert(name.to_ascii_lowercase(), value);
    }

    fn row_count(&self) -> Option<i64> {
        Some(self.previous_affected_rows)
    }

    fn last_insert_id(&self) -> Option<u64> {
        Some(self.previous_last_insert_id)
    }

    fn set_last_insert_id(&self, value: u64) {
        if let Some(current) = &self.statement_last_insert_id {
            *current.borrow_mut() = Some(value);
        }
    }

    fn default_week_format(&self) -> i64 {
        i64::from(self.default_week_format)
    }

    fn div_precision_increment(&self) -> u32 {
        u32::from(self.div_precision_increment.value())
    }

    fn rand_next(&self) -> Option<f64> {
        Some(self.rng.as_ref()?.borrow_mut().gen())
    }

    fn rand_seeded_next(&self, key: usize, seed: i64) -> Option<f64> {
        let mut generators = self.statement_rngs.as_ref()?.borrow_mut();
        Some(
            generators
                .entry(key)
                .or_insert_with(|| MysqlRng::new_with_seed(seed))
                .gen(),
        )
    }

    /// Serves [`Columns::sequence_nextval`] for `RelResolver` — steps the
    /// named sequence's shared catalog entry and records the value for
    /// this session's `LASTVAL`. Every rule (the seek/cache/cycle
    /// mechanics, the run-out error) lives in [`Sequence::next_val`];
    /// this method only does the name lookup and `LASTVAL` bookkeeping.
    fn sequence_nextval(&self, path: &[String]) -> Result<Datum, EvalError> {
        let key = table_key(path);
        let mut seqs = self.sequences.borrow_mut();
        let seq = seqs
            .get_mut(&key)
            .ok_or(EvalError::Sequence("unknown sequence"))?;
        let v = seq.next_val().map_err(EvalError::Sequence)?;
        self.seq_lastval.borrow_mut().insert(key, v);
        Ok(Datum::Int(v))
    }

    /// Serves [`Columns::sequence_lastval`] — `NULL` until this session's
    /// first `NEXTVAL` on this sequence (a `SETVAL` alone never seeds it;
    /// confirmed via `gorun`), but an UNKNOWN sequence is still a real
    /// error, not `NULL` (also confirmed — real TiDB resolves the
    /// sequence object before consulting the session state).
    fn sequence_lastval(&self, path: &[String]) -> Result<Datum, EvalError> {
        let key = table_key(path);
        if !self.sequences.borrow().contains_key(&key) {
            return Err(EvalError::Sequence("unknown sequence"));
        }
        Ok(match self.seq_lastval.borrow().get(&key) {
            Some(v) => Datum::Int(*v),
            None => Datum::Null,
        })
    }

    /// Serves [`Columns::sequence_setval`] — rebase rules (including the
    /// fresh-cache short-circuit) live in [`Sequence::set_val`]; `None`
    /// there is the SQL `NULL` "already satisfied" answer.
    fn sequence_setval(&self, path: &[String], value: i64) -> Result<Datum, EvalError> {
        let key = table_key(path);
        let mut seqs = self.sequences.borrow_mut();
        let seq = seqs
            .get_mut(&key)
            .ok_or(EvalError::Sequence("unknown sequence"))?;
        Ok(match seq.set_val(value) {
            Some(v) => Datum::Int(v),
            None => Datum::Null,
        })
    }
}

/// Resolves column references against one relation row. A qualified reference
/// (`t.a`) matches by table and name; an unqualified one (`a`) matches the
/// first column of that name. An optional `outer` resolver provides the
/// enclosing query's row for correlated subqueries: the inner scope is tried
/// first, so it shadows the outer one. `session` (see [`SessionState`]) is
/// read fresh from `Database` at every construction site, rather than
/// falling back through `outer` — neither the fixed clock nor a system
/// variable is scope-chained through a correlated subquery's own enclosing
/// row.
pub(crate) struct RelResolver<'a> {
    cols: &'a [Column],
    row: &'a [Datum],
    outer: Option<&'a dyn Columns>,
    session: SessionState,
}

impl<'a> RelResolver<'a> {
    /// A resolver over one relation row, with no enclosing scope.
    pub(crate) fn new(cols: &'a [Column], row: &'a [Datum], session: SessionState) -> Self {
        RelResolver {
            cols,
            row,
            outer: None,
            session,
        }
    }

    /// A resolver that falls back to an enclosing query's row when a column is
    /// not found locally (correlated subquery).
    pub(crate) fn with_outer(
        cols: &'a [Column],
        row: &'a [Datum],
        outer: Option<&'a dyn Columns>,
        session: SessionState,
    ) -> Self {
        RelResolver {
            cols,
            row,
            outer,
            session,
        }
    }
}

impl Columns for RelResolver<'_> {
    fn get(&self, path: &[String]) -> Option<Datum> {
        let name = path.last()?;
        let idx = if path.len() >= 2 {
            let qual = &path[path.len() - 2];
            self.cols.iter().position(|c| {
                c.tables.iter().any(|t| t.eq_ignore_ascii_case(qual))
                    && c.name.eq_ignore_ascii_case(name)
            })
        } else {
            self.cols
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(name))
        };
        match idx {
            Some(i) => self.row.get(i).cloned(),
            // Not a local column: try the enclosing (outer) query's row.
            None => self.outer.and_then(|o| o.get(path)),
        }
    }

    fn now(&self) -> Option<(i64, u32, i32)> {
        self.session.now
    }

    fn sysvar(&self, scope: Option<tidb_ast::SysVarScope>, name: &str) -> Option<Datum> {
        self.session.sysvar(scope, name)
    }

    fn get_uservar(&self, name: &str) -> Option<Datum> {
        self.session.get_uservar(name)
    }

    fn set_uservar(&self, name: &str, value: Datum) {
        self.session.set_uservar(name, value);
    }

    fn row_count(&self) -> Option<i64> {
        self.session.row_count()
    }

    fn last_insert_id(&self) -> Option<u64> {
        self.session.last_insert_id()
    }

    fn set_last_insert_id(&self, value: u64) {
        self.session.set_last_insert_id(value);
    }

    fn default_week_format(&self) -> i64 {
        self.session.default_week_format()
    }

    fn div_precision_increment(&self) -> u32 {
        self.session.div_precision_increment()
    }

    fn rand_next(&self) -> Option<f64> {
        self.session.rand_next()
    }

    fn rand_seeded_next(&self, key: usize, seed: i64) -> Option<f64> {
        self.session.rand_seeded_next(key, seed)
    }

    fn sequence_nextval(&self, path: &[String]) -> Result<Datum, EvalError> {
        self.session.sequence_nextval(path)
    }

    fn sequence_lastval(&self, path: &[String]) -> Result<Datum, EvalError> {
        self.session.sequence_lastval(path)
    }

    fn sequence_setval(&self, path: &[String], value: i64) -> Result<Datum, EvalError> {
        self.session.sequence_setval(path, value)
    }
}
