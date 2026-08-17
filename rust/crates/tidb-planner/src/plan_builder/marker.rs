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

//! THE MARKER SCHEME. Ratified here; every clause builder in this crate uses
//! it, and none may invent a second one.
//!
//! # The problem, stated once
//!
//! Go's `PlanBuilder` threads eight side tables through the SELECT build:
//!
//! | Go field | `logical_plan_builder.go` | keyed by |
//! | --- | --- | --- |
//! | `aggMapper` | `resolveHavingAndOrderBy` (:2913) | `*ast.AggregateFuncExpr` |
//! | `havingMap` / `orderMap` | `havingWindowAndOrderbyExprResolver` (:2723) | `*ast.AggregateFuncExpr` |
//! | `totalMap` | `buildProjection` (:1767) | `*ast.AggregateFuncExpr` |
//! | `windowAggMap` | `buildWindowFunctions` (:6893) | `*ast.AggregateFuncExpr` |
//! | `correlatedAggMap` | `resolveCorrelatedAggregates` (:4166) | `*ast.AggregateFuncExpr` |
//! | `windowMapper` | `buildByItemsForWindow` (:6672) | `*ast.WindowFuncExpr` |
//! | `colMapper` | `resolveFromSelectFields` (:3084) | `*ast.ColumnNameExpr` |
//!
//! Every one is `map[*ast.XxxExpr]int`: the key is the ADDRESS of an AST node,
//! and the value is the index of the column the earlier pass produced for it.
//! Go can do this because its AST is a graph of stable heap pointers that the
//! whole build shares.
//!
//! Rust's AST is a tree of owned values. [`tidb_ast::Expr`] is `Clone`, moved
//! into and out of enum arms, and reallocated by `Vec` growth; its address is
//! not an identity and outlives nothing. A `HashMap<*const Expr, usize>` would
//! compile and be wrong the first time a clause was cloned.
//!
//! # The scheme, as ratified
//!
//! **A marker is a reserved column name substituted INTO the AST in place of
//! the node it stands for.** The pass that PRODUCES an output column rewrites
//! the sub-expression into `Expr::Column(vec![marker_name])`; a later pass
//! reading that clause decodes the name back to `(kind, index)` and binds it
//! to the column at that index of the producing operator's schema.
//!
//! This is not invented here. `tidb-executor`'s driver already made exactly
//! this call and has shipped on it: `driver/agg_build.rs:787`
//! `substitute_aggregates` walks a clause and replaces each aggregate call
//! with `Expr::Column(vec![name])` naming the aggregation output it just
//! appended (`agg_build.rs:662` for `GROUPING()`, and the same for every
//! `AggFunc`), and `:818` / `:830` (`hoisted_window_index`,
//! `expr_has_hoisted_window`) READ those markers back out by name. Harvesting
//! it rather than inventing a parallel mechanism keeps one scheme in the tree.
//!
//! ## Spec
//!
//! 1. The marker text is `#<kind>#<index>` — [`MARKER_SIGIL`] `#`, the kind
//!    tag from [`MarkerKind::tag`], the sigil again, and the decimal index.
//! 2. A marker occupies a WHOLE `Expr::Column` path of length one. It is never
//!    a path segment, never a table qualifier, and never nested in a name.
//! 3. `(kind, index)` is Go's `(which map, map value)`. The eight Go maps
//!    become the eight [`MarkerKind`] variants, so a marker written by one
//!    pass can never be read as another pass's.
//! 4. The index is an index into the SCHEMA of the operator the producing pass
//!    built — identical to Go, whose map values are `len(schema.Columns)` at
//!    insertion.
//! 5. Substitution happens on a clone the builder OWNS
//!    ([`PlanBuilder::clause_scratch`](super::PlanBuilder::clause_scratch)).
//!    The caller's AST is never mutated, which is what lets the builder run
//!    twice over the same statement (Go relies on `resetForReuse`).
//! 6. Decoding is total: [`PlanMarker::decode`] returns `None` for anything
//!    that is not a well-formed marker, so an ordinary column named `#x` is
//!    passed through as a column rather than misread.
//!
//! ## Collision, named honestly
//!
//! `` SELECT `#agg#0` FROM t `` parses to the same `Expr::Column` a marker
//! does. Go's pointer keys cannot collide this way. Three things bound it:
//! markers only ever appear in a clause the builder itself substituted into
//! (rule 5); a decoded marker is only BOUND when the producing operator
//! actually has a column at that index, and otherwise falls through to normal
//! name resolution; and the sigil is `#`, which requires backticks in SQL.
//! [`is_marker_name`] is exported so a name-resolution path can reject a
//! user-written identifier in this shape outright if a caller wants the
//! stricter behaviour.
//!
//! ## Instruction to later batches
//!
//! 6c (aggregation / HAVING / GROUP BY) and 6e (window functions) MUST use
//! [`MarkerKind::Agg`] / [`Having`](MarkerKind::Having) /
//! [`OrderBy`](MarkerKind::OrderBy) / [`Total`](MarkerKind::Total) /
//! [`WindowAgg`](MarkerKind::WindowAgg) /
//! [`CorrelatedAgg`](MarkerKind::CorrelatedAgg) /
//! [`Window`](MarkerKind::Window) / [`Column`](MarkerKind::Column) rather than
//! any new side table. A `HashMap` keyed on a pointer, an index into a
//! traversal order, or a re-derived structural hash of the sub-expression are
//! all rejected: the first is unsound, the second breaks the moment a pass
//! rewrites a sibling, and the third cannot distinguish two textually
//! identical aggregates that Go's pointer keys DO distinguish.

use tidb_ast::Expr;

/// The character that opens and separates a marker. Requires backticks to
/// appear in a SQL identifier; see this module's collision note.
pub const MARKER_SIGIL: char = '#';

/// Which of Go's eight `map[*ast.XxxExpr]int` side tables a marker stands for.
///
/// The variant IS the map; see this module's header table for the Go field and
/// the `logical_plan_builder.go` line each one comes from.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum MarkerKind {
    /// Go `aggMapper` (`resolveHavingAndOrderBy`): the aggregation's own
    /// output column for an aggregate written in HAVING or ORDER BY.
    Agg,
    /// Go `havingMap` (`havingWindowAndOrderbyExprResolver`).
    Having,
    /// Go `orderMap` (`havingWindowAndOrderbyExprResolver`).
    OrderBy,
    /// Go `totalMap` (`buildProjection`): the union of the above, keyed after
    /// the projection has been built.
    Total,
    /// Go `windowAggMap` (`buildWindowFunctions`): an aggregate written INSIDE
    /// a window function's arguments.
    WindowAgg,
    /// Go `correlatedAggMap` (`resolveCorrelatedAggregates`): an aggregate
    /// whose arguments belong to an OUTER query block.
    CorrelatedAgg,
    /// Go `windowMapper` (`buildByItemsForWindow`): the window function's own
    /// output column.
    Window,
    /// Go `colMapper` (`resolveFromSelectFields`): the select-list position an
    /// ORDER BY / HAVING column reference resolved to.
    Column,
}

impl MarkerKind {
    /// The text form used in the encoded marker. Stable: it is part of the
    /// on-AST representation, not a display detail.
    #[must_use]
    pub const fn tag(self) -> &'static str {
        match self {
            Self::Agg => "agg",
            Self::Having => "having",
            Self::OrderBy => "order",
            Self::Total => "total",
            Self::WindowAgg => "wagg",
            Self::CorrelatedAgg => "corragg",
            Self::Window => "win",
            Self::Column => "col",
        }
    }

    /// Whether this kind's marker INDEX is also the producing operator's
    /// schema index (spec rule 4).
    ///
    /// True for every kind but [`Self::Window`]: Go's `windowMapper` value IS
    /// `schema.Len()` at insertion, but this port binds the kind to a vector
    /// ordered by the k-th window CALL, so the marker index selects the column
    /// and the column carries its own schema position.
    #[must_use]
    pub const fn index_is_schema_index(self) -> bool {
        !matches!(self, Self::Window)
    }

    fn from_tag(tag: &str) -> Option<Self> {
        Some(match tag {
            "agg" => Self::Agg,
            "having" => Self::Having,
            "order" => Self::OrderBy,
            "total" => Self::Total,
            "wagg" => Self::WindowAgg,
            "corragg" => Self::CorrelatedAgg,
            "win" => Self::Window,
            "col" => Self::Column,
            _ => return None,
        })
    }
}

/// One `(kind, index)` pair: Go's `(side table, map value)`.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PlanMarker {
    /// Which side table this marker replaces.
    pub kind: MarkerKind,
    /// The schema index of the column the producing pass appended.
    pub index: usize,
}

impl PlanMarker {
    /// A marker for the column at `index` of the producing operator's schema.
    #[must_use]
    pub const fn new(kind: MarkerKind, index: usize) -> Self {
        Self { kind, index }
    }

    /// The reserved name, `#<tag>#<index>`.
    #[must_use]
    pub fn encode(self) -> String {
        format!(
            "{}{}{}{}",
            MARKER_SIGIL,
            self.kind.tag(),
            MARKER_SIGIL,
            self.index
        )
    }

    /// The AST node a producing pass substitutes IN, per rule 2.
    #[must_use]
    pub fn as_expr(self) -> Expr {
        Expr::Column(vec![self.encode()])
    }

    /// The inverse of [`Self::encode`]; `None` for any text that is not a
    /// well-formed marker (rule 6).
    #[must_use]
    pub fn decode(name: &str) -> Option<Self> {
        let body = name.strip_prefix(MARKER_SIGIL)?;
        let (tag, index) = body.split_once(MARKER_SIGIL)?;
        Some(Self {
            kind: MarkerKind::from_tag(tag)?,
            // `str::parse` rejects a sign or any non-digit, so `#agg#-1` and
            // `#agg#` are both passed through as ordinary names.
            index: index.parse().ok()?,
        })
    }

    /// The inverse of [`Self::as_expr`]: the marker `expr` IS, if any.
    ///
    /// Only a single-segment `Expr::Column` can be a marker (rule 2), so a
    /// qualified `` t.`#agg#0` `` is an ordinary column reference.
    #[must_use]
    pub fn from_expr(expr: &Expr) -> Option<Self> {
        match expr {
            Expr::Column(path) => match path.as_slice() {
                [name] => Self::decode(name),
                _ => None,
            },
            _ => None,
        }
    }

    /// [`Self::from_expr`] restricted to one kind, which is how a reading pass
    /// asks "is this MY map's key" (rule 3).
    #[must_use]
    pub fn index_of_kind(expr: &Expr, kind: MarkerKind) -> Option<usize> {
        Self::from_expr(expr)
            .filter(|m| m.kind == kind)
            .map(|m| m.index)
    }
}

/// Whether `name` is in the reserved marker shape at all, for a resolver that
/// wants to reject a user-written identifier of this form outright.
#[must_use]
pub fn is_marker_name(name: &str) -> bool {
    PlanMarker::decode(name).is_some()
}

/// Substitutes `marker` in place of `expr`, returning the node it replaced.
///
/// This is the producing half of the scheme, and the direct analogue of Go's
/// `mapper[node] = index` — except that the marker travels WITH the clause
/// instead of beside it, which is the whole point.
pub fn substitute(expr: &mut Expr, marker: PlanMarker) -> Expr {
    std::mem::replace(expr, marker.as_expr())
}

#[cfg(test)]
mod tests {
    use super::{is_marker_name, substitute, MarkerKind, PlanMarker};
    use tidb_ast::Expr;

    const ALL_KINDS: [MarkerKind; 8] = [
        MarkerKind::Agg,
        MarkerKind::Having,
        MarkerKind::OrderBy,
        MarkerKind::Total,
        MarkerKind::WindowAgg,
        MarkerKind::CorrelatedAgg,
        MarkerKind::Window,
        MarkerKind::Column,
    ];

    #[test]
    fn test_marker_round_trips_through_the_ast_for_every_kind() {
        // Rule 3: a marker written by one pass is never readable as another's.
        for (position, kind) in ALL_KINDS.into_iter().enumerate() {
            let marker = PlanMarker::new(kind, position * 3);
            let expr = marker.as_expr();
            assert_eq!(PlanMarker::from_expr(&expr), Some(marker));
            assert_eq!(PlanMarker::index_of_kind(&expr, kind), Some(position * 3));
            for other in ALL_KINDS {
                if other != kind {
                    assert_eq!(PlanMarker::index_of_kind(&expr, other), None);
                }
            }
        }
    }

    #[test]
    fn test_marker_survives_a_clone_where_a_pointer_key_would_not() {
        // The whole reason the scheme exists: Go keys on the node address, and
        // this clause is cloned into the builder's scratch before use.
        let original = PlanMarker::new(MarkerKind::Agg, 2).as_expr();
        let cloned = original.clone();
        drop(original);
        assert_eq!(
            PlanMarker::from_expr(&cloned),
            Some(PlanMarker::new(MarkerKind::Agg, 2))
        );
    }

    #[test]
    fn test_decode_is_total_over_non_markers() {
        assert!(PlanMarker::decode("a").is_none());
        assert!(PlanMarker::decode("#agg").is_none());
        assert!(PlanMarker::decode("#agg#").is_none());
        assert!(PlanMarker::decode("#agg#-1").is_none());
        assert!(PlanMarker::decode("#nosuch#0").is_none());
        assert!(is_marker_name("#agg#0"));
        assert!(!is_marker_name("#agg#x"));

        // Rule 2: only a single-segment column path can be a marker.
        let qualified = Expr::Column(vec!["t".to_owned(), "#agg#0".to_owned()]);
        assert!(PlanMarker::from_expr(&qualified).is_none());
        assert!(PlanMarker::from_expr(&Expr::Int("1".to_owned())).is_none());
    }

    #[test]
    fn test_substitute_returns_the_replaced_node() {
        let mut expr = Expr::Column(vec!["a".to_owned()]);
        let replaced = substitute(&mut expr, PlanMarker::new(MarkerKind::Total, 4));
        assert_eq!(replaced, Expr::Column(vec!["a".to_owned()]));
        assert_eq!(
            PlanMarker::from_expr(&expr),
            Some(PlanMarker::new(MarkerKind::Total, 4))
        );
    }
}
