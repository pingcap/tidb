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

//! Dependency-closed CNF/DNF range-condition detachment.
//!
//! `pkg/util/ranger/detacher.go` separates conditions that can contribute to
//! an access range from conditions that must remain filters.  The Go owner
//! receives full expression trees, session/collation state, and the ranger
//! checker.  Those owners are not available in this planner crate yet.  This
//! leaf therefore accepts a normalized boolean predicate whose atomic nodes
//! already carry the checker decision, then ports the source's boolean
//! traversal and access/filter reconstruction exactly.
//!
//! No expression is evaluated here, no range endpoints are built, and no
//! collation/type/session decision is guessed.  A future ranger checker can
//! lower its result to [`RangeAtom`] and consume this same detachment seam.

/// Source checker output for one normalized atom.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AccessDecision {
    /// Whether this atom can contribute to the access range.
    pub is_access: bool,
    /// Whether the atom must also be retained as a residual filter.
    pub should_reserve: bool,
}

impl AccessDecision {
    /// Creates a usable access condition with no residual filter.
    #[must_use]
    pub const fn access() -> Self {
        Self {
            is_access: true,
            should_reserve: false,
        }
    }

    /// Creates a usable approximate access condition that must be rechecked.
    #[must_use]
    pub const fn access_and_reserve() -> Self {
        Self {
            is_access: true,
            should_reserve: true,
        }
    }

    /// Creates a filter-only condition.
    #[must_use]
    pub const fn filter() -> Self {
        Self {
            is_access: false,
            should_reserve: true,
        }
    }
}

/// Syntax-only category for one range predicate atom.
///
/// The category is metadata for the eventual checker and is intentionally
/// not interpreted by this module.  In particular, `Like`/`Comparison` do
/// not imply access: callers must supply the source checker decision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RangeAtomKind {
    /// A comparison such as `=`, `<`, or `>=`.
    Comparison,
    /// An `IN`/`NOT IN` membership predicate.
    Membership,
    /// An `IS NULL`/truthness predicate.
    NullOrTruth,
    /// A bare column/constant predicate.
    Scalar,
    /// A source shape not yet owned by the normalized ranger boundary.
    Opaque(&'static str),
}

/// One normalized atom retaining identity, syntax category, and checker
/// output.  The identity is opaque to this module and lets callers compare
/// source predicates without copying expression values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangeAtom {
    identity: u32,
    kind: RangeAtomKind,
    decision: AccessDecision,
}

impl RangeAtom {
    /// Creates an atom with an opaque caller identity and checker result.
    #[must_use]
    pub const fn new(identity: u32, kind: RangeAtomKind, decision: AccessDecision) -> Self {
        Self {
            identity,
            kind,
            decision,
        }
    }

    /// Returns the caller-owned stable identity.
    #[must_use]
    pub const fn identity(&self) -> u32 {
        self.identity
    }

    /// Returns syntax-only atom metadata.
    #[must_use]
    pub const fn kind(&self) -> &RangeAtomKind {
        &self.kind
    }

    /// Returns the source checker decision supplied by the caller.
    #[must_use]
    pub const fn decision(&self) -> AccessDecision {
        self.decision
    }
}

/// A normalized boolean predicate tree.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RangePredicate {
    /// One source predicate atom.
    Atom(RangeAtom),
    /// A conjunction of source predicates.
    And(Vec<Self>),
    /// A disjunction of source predicates.
    Or(Vec<Self>),
}

impl RangePredicate {
    /// Wraps one normalized atom.
    #[must_use]
    pub const fn atom(atom: RangeAtom) -> Self {
        Self::Atom(atom)
    }

    /// Builds a conjunction while retaining caller order.
    #[must_use]
    pub fn and<I>(children: I) -> Self
    where
        I: IntoIterator<Item = Self>,
    {
        Self::And(children.into_iter().collect())
    }

    /// Builds a disjunction while retaining caller order.
    #[must_use]
    pub fn or<I>(children: I) -> Self
    where
        I: IntoIterator<Item = Self>,
    {
        Self::Or(children.into_iter().collect())
    }

    /// Returns this predicate's atom, if it is a leaf.
    #[must_use]
    pub const fn as_atom(&self) -> Option<&RangeAtom> {
        match self {
            Self::Atom(atom) => Some(atom),
            Self::And(_) | Self::Or(_) => None,
        }
    }
}

/// Access and residual conditions produced from one CNF input list.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DetachmentResult {
    access_conditions: Vec<RangePredicate>,
    filter_conditions: Vec<RangePredicate>,
}

impl DetachmentResult {
    /// Returns conditions selected for range construction, in source order.
    #[must_use]
    pub fn access_conditions(&self) -> &[RangePredicate] {
        &self.access_conditions
    }

    /// Returns conditions retained for post-range filtering, in source order.
    #[must_use]
    pub fn filter_conditions(&self) -> &[RangePredicate] {
        &self.filter_conditions
    }
}

/// The result needed while detaching one DNF branch.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DnfDetachment {
    access_conditions: Vec<RangePredicate>,
    has_residual: bool,
}

impl DnfDetachment {
    /// Returns the access part of each DNF branch.
    #[must_use]
    pub fn access_conditions(&self) -> &[RangePredicate] {
        &self.access_conditions
    }

    /// Returns whether at least one branch retained a residual filter.
    #[must_use]
    pub const fn has_residual(&self) -> bool {
        self.has_residual
    }
}

/// Detaches access predicates from a top-level CNF list.
///
/// This is the normalized equivalent of Go's
/// `detachColumnCNFConditions`.  A top-level `OR` is detached branch by
/// branch; if any branch has a residual, the original `OR` is retained as one
/// filter.  Non-`OR` nodes are checked as a whole, matching the source
/// checker contract.
#[must_use]
pub fn detach_cnf_predicates(conditions: &[RangePredicate]) -> DetachmentResult {
    let mut result = DetachmentResult::default();
    for condition in conditions {
        if matches!(condition, RangePredicate::Or(_)) {
            let dnf = detach_dnf_predicates(&flatten_or(condition));
            if dnf.has_residual {
                result.filter_conditions.push(condition.clone());
            }
            if let Some(access) = compose_or(dnf.access_conditions) {
                result.access_conditions.push(access);
            }
            continue;
        }

        let decision = checker_decision(condition);
        if !decision.is_access {
            result.filter_conditions.push(condition.clone());
            continue;
        }
        result.access_conditions.push(condition.clone());
        if decision.should_reserve {
            result.filter_conditions.push(condition.clone());
        }
    }
    result
}

/// Detaches access predicates from a top-level DNF list.
///
/// The input is the already-flattened DNF item list used by the Go caller.
/// An `AND` branch is recursively detached as CNF.  If one branch has no
/// access predicate, the entire DNF is unusable for access-range extraction,
/// and `has_residual` is set exactly as in the source implementation.
#[must_use]
pub fn detach_dnf_predicates(conditions: &[RangePredicate]) -> DnfDetachment {
    let mut result = DnfDetachment::default();
    for condition in conditions {
        if matches!(condition, RangePredicate::And(_)) {
            let cnf = detach_cnf_predicates(&flatten_and(condition));
            result.has_residual |= !cnf.filter_conditions.is_empty();
            let Some(access) = compose_and(cnf.access_conditions) else {
                return DnfDetachment {
                    access_conditions: Vec::new(),
                    has_residual: true,
                };
            };
            result.access_conditions.push(access);
            continue;
        }

        let decision = checker_decision(condition);
        if !decision.is_access {
            return DnfDetachment {
                access_conditions: Vec::new(),
                has_residual: true,
            };
        }
        result.access_conditions.push(condition.clone());
        result.has_residual |= decision.should_reserve;
    }
    result
}

fn checker_decision(predicate: &RangePredicate) -> AccessDecision {
    match predicate {
        RangePredicate::Atom(atom) => atom.decision,
        RangePredicate::And(children) | RangePredicate::Or(children) => {
            // Go's conditionChecker requires every child to be an access
            // condition and ORs the residual flag across children.
            if children.is_empty() {
                return AccessDecision::filter();
            }
            let mut should_reserve = false;
            for child in children {
                let decision = checker_decision(child);
                if !decision.is_access {
                    return AccessDecision::filter();
                }
                should_reserve |= decision.should_reserve;
            }
            AccessDecision {
                is_access: true,
                should_reserve,
            }
        }
    }
}

fn flatten_or(predicate: &RangePredicate) -> Vec<RangePredicate> {
    let mut flattened = Vec::new();
    flatten_same_or(predicate, &mut flattened);
    flattened
}

fn flatten_same_or(predicate: &RangePredicate, output: &mut Vec<RangePredicate>) {
    match predicate {
        RangePredicate::Or(children) => {
            for child in children {
                flatten_same_or(child, output);
            }
        }
        _ => output.push(predicate.clone()),
    }
}

fn flatten_and(predicate: &RangePredicate) -> Vec<RangePredicate> {
    let mut flattened = Vec::new();
    flatten_same_and(predicate, &mut flattened);
    flattened
}

fn flatten_same_and(predicate: &RangePredicate, output: &mut Vec<RangePredicate>) {
    match predicate {
        RangePredicate::And(children) => {
            for child in children {
                flatten_same_and(child, output);
            }
        }
        _ => output.push(predicate.clone()),
    }
}

fn compose_or(children: Vec<RangePredicate>) -> Option<RangePredicate> {
    match children.len() {
        0 => None,
        1 => children.into_iter().next(),
        _ => Some(RangePredicate::Or(children)),
    }
}

fn compose_and(children: Vec<RangePredicate>) -> Option<RangePredicate> {
    match children.len() {
        0 => None,
        1 => children.into_iter().next(),
        _ => Some(RangePredicate::And(children)),
    }
}
