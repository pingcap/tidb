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

//! Statement-summary admission from Go `pkg/executor/adapter.go`.
//!
//! The live session completion producer in `tidb-session::observation` uses
//! this policy before publishing to `tidb-stmtsummary`. Detailed statement
//! measurements and slow-log rule evaluation remain separate obligations.

/// The statement kinds `SummaryStmt` special-cases.
///
/// Go: `pkg/executor/adapter.go:2204, 2217`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SummaryStmtKind {
    /// Go: `*ast.PrepareStmt` — skipped; `EXECUTE` is recorded instead.
    Prepare,
    /// Go: `*ast.CommitStmt` — recorded, but only when a previous statement
    /// digest exists to attribute it to.
    Commit,
    /// Any other statement.
    Other,
}

/// Everything `SummaryStmt` consults before recording.
///
/// Go: `pkg/executor/adapter.go:2189-2225`.
#[derive(Clone, Debug)]
pub struct SummaryGate {
    /// Go: `sessVars.InRestrictedSQL`.
    pub in_restricted_sql: bool,
    /// Go: `sessVars.User.Username`, empty when `sessVars.User == nil`.
    pub user_name: String,
    /// Go: `sessVars.InExplainExplore`.
    pub in_explain_explore: bool,
    /// Go: `stmtsummaryv2.Enabled()`.
    pub summary_enabled: bool,
    /// Go: `stmtsummaryv2.EnabledInternal()`.
    pub summary_internal_enabled: bool,
    /// The statement's kind.
    pub stmt_kind: SummaryStmtKind,
    /// Go: `sessVars.GetPrevStmtDigest()`.
    pub prev_stmt_digest: String,
}

/// What `SummaryStmt` does for a given gate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SummaryAction {
    /// Go: summary disabled for this statement — `SetPrevStmtDigest("")` then
    /// return. Note this is the *only* skip path that clears the digest.
    ClearPrevDigestAndSkip,
    /// Go: `*ast.PrepareStmt` returns without touching the previous digest.
    SkipPrepare,
    /// Go: a `COMMIT` seen before any digest was recorded is ignored, again
    /// without touching the previous digest.
    SkipCommitWithoutPrevDigest,
    /// Go: the statement is recorded via `stmtsummaryv2.Add`, and the previous
    /// statement digest is replaced by this statement's digest.
    Record {
        /// Go's `isInternalSQL`, stored on `StmtExecInfo.IsInternal`.
        is_internal: bool,
        /// Go: the previous statement digest a `COMMIT` is attributed to;
        /// `None` for every other statement kind.
        attributed_prev_digest: Option<String>,
    },
}

/// Whether a statement counts as internal for statement-summary purposes.
///
/// Go: `pkg/executor/adapter.go:2198`. A statement with no user name counts as
/// internal just like restricted SQL, but `EXPLAIN EXPLORE` forces it back to
/// user-visible so its inner statements land in the summary.
#[must_use]
pub fn is_internal_sql(in_restricted_sql: bool, user_name: &str, in_explain_explore: bool) -> bool {
    (in_restricted_sql || user_name.is_empty()) && !in_explain_explore
}

/// Decides whether `SummaryStmt` records this statement.
///
/// Go: `ExecStmt.SummaryStmt` at `pkg/executor/adapter.go:2189`.
#[must_use]
pub fn decide_summary_stmt(gate: &SummaryGate) -> SummaryAction {
    let is_internal = is_internal_sql(
        gate.in_restricted_sql,
        &gate.user_name,
        gate.in_explain_explore,
    );
    if !gate.summary_enabled || (is_internal && !gate.summary_internal_enabled) {
        return SummaryAction::ClearPrevDigestAndSkip;
    }
    if gate.stmt_kind == SummaryStmtKind::Prepare {
        return SummaryAction::SkipPrepare;
    }
    let attributed_prev_digest = if gate.stmt_kind == SummaryStmtKind::Commit {
        if gate.prev_stmt_digest.is_empty() {
            return SummaryAction::SkipCommitWithoutPrevDigest;
        }
        Some(gate.prev_stmt_digest.clone())
    } else {
        None
    };
    SummaryAction::Record {
        is_internal,
        attributed_prev_digest,
    }
}

// boundary: the `StmtExecInfo` population (`pkg/executor/adapter.go:2258-2302`)
// reads ~30 session/stmt-context fields (`stmtCtx.SQLDigest`,
// `stmtCtx.CopTasksSummary`, `sessVars.MemTracker`, `GetPlanDigest`,
// `keyspace.GetKeyspaceNameBySettings`, `calculateStatementTotalRUV2`, ...).
// The summary sink lives in `tidb-stmtsummary`. The live `tidb-session`
// completion producer reuses `decide_summary_stmt`; detailed plan/RPC/phase
// measurements remain explicit integration obligations, not package acceptance.

#[cfg(test)]
mod tests {
    use super::*;

    fn summary_gate() -> SummaryGate {
        SummaryGate {
            in_restricted_sql: false,
            user_name: "root".to_owned(),
            in_explain_explore: false,
            summary_enabled: true,
            summary_internal_enabled: false,
            stmt_kind: SummaryStmtKind::Other,
            prev_stmt_digest: String::new(),
        }
    }

    #[test]
    fn missing_user_counts_as_internal() {
        assert!(is_internal_sql(false, "", false));
        assert!(is_internal_sql(true, "root", false));
        assert!(!is_internal_sql(false, "root", false));
    }

    #[test]
    fn explain_explore_forces_internal_sql_back_to_user_visible() {
        assert!(!is_internal_sql(true, "", true));
    }

    #[test]
    fn disabled_summary_clears_prev_digest() {
        let gate = SummaryGate {
            summary_enabled: false,
            ..summary_gate()
        };
        assert_eq!(
            decide_summary_stmt(&gate),
            SummaryAction::ClearPrevDigestAndSkip
        );
    }

    #[test]
    fn internal_sql_needs_the_internal_switch() {
        let gate = SummaryGate {
            in_restricted_sql: true,
            ..summary_gate()
        };
        assert_eq!(
            decide_summary_stmt(&gate),
            SummaryAction::ClearPrevDigestAndSkip
        );
        let enabled = SummaryGate {
            summary_internal_enabled: true,
            ..gate
        };
        assert_eq!(
            decide_summary_stmt(&enabled),
            SummaryAction::Record {
                is_internal: true,
                attributed_prev_digest: None,
            }
        );
    }

    #[test]
    fn prepare_is_skipped_without_clearing_the_digest() {
        let gate = SummaryGate {
            stmt_kind: SummaryStmtKind::Prepare,
            prev_stmt_digest: "abc".to_owned(),
            ..summary_gate()
        };
        assert_eq!(decide_summary_stmt(&gate), SummaryAction::SkipPrepare);
    }

    #[test]
    fn commit_without_prev_digest_is_ignored() {
        let gate = SummaryGate {
            stmt_kind: SummaryStmtKind::Commit,
            ..summary_gate()
        };
        assert_eq!(
            decide_summary_stmt(&gate),
            SummaryAction::SkipCommitWithoutPrevDigest
        );
    }

    #[test]
    fn commit_is_attributed_to_the_previous_statement() {
        let gate = SummaryGate {
            stmt_kind: SummaryStmtKind::Commit,
            prev_stmt_digest: "abc".to_owned(),
            ..summary_gate()
        };
        assert_eq!(
            decide_summary_stmt(&gate),
            SummaryAction::Record {
                is_internal: false,
                attributed_prev_digest: Some("abc".to_owned()),
            }
        );
    }
}
