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

//! Bounded two-relation binding for the configured read-only SQL node.
//!
//! This is the source-shaped name-resolution stage between the immutable
//! configured catalog and join planning. It binds exactly two base tables,
//! direct projections, and one-relation signed-`BIGINT` comparisons. The
//! `ON`/`USING` constraint is retained verbatim for the next planner stage;
//! no join-key classification or execution happens here.

use std::{error::Error, fmt};

use tidb_ast::{
    BinaryOp, Expr, Join, JoinNode, JoinType, QueryStmt, SelectField, SelectStatementKind,
    SelectStmt, Stmt, TableRef, UnaryOp,
};

use crate::read_only_scan::{
    configured_catalog::{ConfiguredCatalog, ConfiguredTableLookupError},
    fold_identifier, ConfiguredColumn, ConfiguredTable,
};

/// The owning input of a bound projection or local predicate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RelationSide {
    /// The first relation in source order.
    Left,
    /// The second relation in source order.
    Right,
}

/// One configured base relation with its SQL-visible qualifier.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoundRelation {
    table: ConfiguredTable,
    qualifier: String,
}

impl BoundRelation {
    /// Returns the immutable configured table descriptor.
    #[must_use]
    pub const fn table(&self) -> &ConfiguredTable {
        &self.table
    }

    /// Returns the SQL-visible alias, or the source table name when unaliased.
    #[must_use]
    pub fn qualifier(&self) -> &str {
        &self.qualifier
    }
}

/// A direct result column bound to one source relation and column offset.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoundProjection {
    side: RelationSide,
    column_offset: usize,
    output_name: String,
    explicit_alias: bool,
}

impl BoundProjection {
    /// Returns the physical relation that owns this result column.
    #[must_use]
    pub const fn side(&self) -> RelationSide {
        self.side
    }

    /// Returns the source-order column offset within the owning relation.
    #[must_use]
    pub const fn column_offset(&self) -> usize {
        self.column_offset
    }

    /// Returns the explicit result alias or original column spelling.
    #[must_use]
    pub fn output_name(&self) -> &str {
        &self.output_name
    }

    /// Returns whether the output name came from an explicit `AS` alias.
    #[must_use]
    #[allow(dead_code)] // Direct source-shard binder tests compile this module without tail lowering.
    pub const fn has_explicit_alias(&self) -> bool {
        self.explicit_alias
    }
}

/// Join syntax retained for typed classification by Stage C.
#[derive(Clone, Debug, PartialEq)]
pub enum BoundJoinConstraint {
    /// CROSS or comma syntax without a join key.
    Cross,
    /// An ON expression retained for typed classification by Stage C.
    On(Expr),
    /// USING names retained in written order for Stage C.
    Using(Vec<String>),
}

/// A local comparison and the configured relation that owns it.
#[derive(Clone, Debug, PartialEq)]
pub struct BoundLocalPredicate {
    side: RelationSide,
    expression: Expr,
}

impl BoundLocalPredicate {
    /// Returns the physical relation that owns this predicate.
    #[must_use]
    pub const fn side(&self) -> RelationSide {
        self.side
    }

    /// Returns the original validated comparison expression.
    #[must_use]
    pub const fn expression(&self) -> &Expr {
        &self.expression
    }
}

/// Fully name-bound input to the configured join planner.
#[derive(Clone, Debug, PartialEq)]
pub struct ConfiguredRelationTree {
    left: BoundRelation,
    right: BoundRelation,
    projections: Vec<BoundProjection>,
    local_predicates: Vec<BoundLocalPredicate>,
    join_constraint: BoundJoinConstraint,
}

impl ConfiguredRelationTree {
    /// Parses and binds the complete bounded query before runtime admission.
    pub fn bind_sql(sql: &str, catalog: &ConfiguredCatalog) -> Result<Self, RelationBindError> {
        let select = parse_select(sql)?;
        bind_select(&select, catalog)
    }

    /// Returns the first source relation.
    #[must_use]
    pub const fn left(&self) -> &BoundRelation {
        &self.left
    }

    /// Returns the second source relation.
    #[must_use]
    pub const fn right(&self) -> &BoundRelation {
        &self.right
    }

    /// Returns direct result projections in written field order.
    #[must_use]
    pub fn projections(&self) -> &[BoundProjection] {
        &self.projections
    }

    /// Returns local predicates in source AND traversal order.
    #[must_use]
    pub fn local_predicates(&self) -> &[BoundLocalPredicate] {
        &self.local_predicates
    }

    /// Returns the retained join syntax.
    #[must_use]
    pub const fn join_constraint(&self) -> &BoundJoinConstraint {
        &self.join_constraint
    }
}

/// Parses exactly one plain SELECT for a configured planner entrypoint.
///
/// The caller owns the parsed AST and must pass it to one of this module's
/// typed binders; it must not restore and reparse the query text.
pub(crate) fn parse_select(sql: &str) -> Result<SelectStmt, RelationBindError> {
    let stmt = tidb_parser::parse(sql).map_err(|error| RelationBindError::Parse(error.message))?;
    match stmt {
        Stmt::Query(query) => match *query {
            QueryStmt::Select(select) => Ok(*select),
            QueryStmt::SetOpr(_) => Err(RelationBindError::UnsupportedQueryShape),
        },
        _ => Err(RelationBindError::UnsupportedQueryShape),
    }
}

/// Explicit failures at the two-relation binding boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RelationBindError {
    /// The SQL parser rejected the statement.
    Parse(String),
    /// A statement or SELECT envelope lies outside the bounded query shape.
    UnsupportedQueryShape,
    /// The FROM tree does not contain exactly two plain base tables.
    ExactlyTwoBaseRelationsRequired,
    /// The join is not INNER/CROSS/comma syntax.
    UnsupportedJoin,
    /// A table uses partition, stale-read, hint, or sample syntax.
    UnsupportedTableOption,
    /// A table identifier is not one- or two-part.
    InvalidTablePath(Vec<String>),
    /// The immutable catalog could not resolve a table.
    TableLookup(ConfiguredTableLookupError),
    /// Both relations expose the same folded qualifier.
    DuplicateQualifier(String),
    /// A column identifier path has an unsupported segment count.
    InvalidColumnPath(Vec<String>),
    /// A qualified column names neither visible relation.
    UnknownQualifier(String),
    /// No configured column matches the requested path.
    UnknownColumn(Vec<String>),
    /// More than one configured column matches the requested path.
    AmbiguousColumn(Vec<String>),
    /// A result field is not a direct configured column.
    UnsupportedProjection,
    /// A WHERE term is not a bounded column-to-signed-integer comparison.
    UnsupportedPredicate,
    /// A WHERE term compares columns from both relations.
    CrossRelationWherePredicate,
}

impl fmt::Display for RelationBindError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "configured relation binding failed: {self:?}")
    }
}

impl Error for RelationBindError {}

pub(crate) fn bind_select(
    select: &SelectStmt,
    catalog: &ConfiguredCatalog,
) -> Result<ConfiguredRelationTree, RelationBindError> {
    validate_query_envelope(select)?;
    bind_select_core(select, catalog)
}

/// Binds a select whose ORDER BY/LIMIT tail is owned by a later typed stage.
///
/// This keeps base-relation and projection binding centralized while leaving
/// ordinary configured join planning strict about unsupported query tails.
#[allow(dead_code)] // Direct source-shard binder tests compile this module without tail lowering.
pub(crate) fn bind_select_with_order_limit(
    select: &SelectStmt,
    catalog: &ConfiguredCatalog,
) -> Result<ConfiguredRelationTree, RelationBindError> {
    validate_query_envelope_core(select)?;
    bind_select_core(select, catalog)
}

fn bind_select_core(
    select: &SelectStmt,
    catalog: &ConfiguredCatalog,
) -> Result<ConfiguredRelationTree, RelationBindError> {
    let from = select
        .from
        .as_ref()
        .ok_or(RelationBindError::ExactlyTwoBaseRelationsRequired)?;
    if from.tp != JoinType::Cross || from.straight || from.natural {
        return Err(RelationBindError::UnsupportedJoin);
    }
    let (left_ref, right_ref) = two_table_refs(from)?;
    let left = bind_relation(left_ref, catalog)?;
    let right = bind_relation(right_ref, catalog)?;
    if fold_identifier(left.qualifier()) == fold_identifier(right.qualifier()) {
        return Err(RelationBindError::DuplicateQualifier(
            right.qualifier().to_owned(),
        ));
    }

    let join_constraint = match (&from.on, from.using.as_slice()) {
        (Some(on), []) => BoundJoinConstraint::On(on.clone()),
        (None, []) => BoundJoinConstraint::Cross,
        (None, using) => BoundJoinConstraint::Using(using.to_vec()),
        (Some(_), _) => return Err(RelationBindError::UnsupportedJoin),
    };
    let projections = select
        .fields
        .iter()
        .map(|field| bind_projection(field, &left, &right, &from.using))
        .collect::<Result<Vec<_>, _>>()?;
    let mut local_predicates = Vec::new();
    if let Some(predicate) = &select.where_clause {
        bind_local_predicates(predicate, &left, &right, &mut local_predicates)?;
    }

    Ok(ConfiguredRelationTree {
        left,
        right,
        projections,
        local_predicates,
        join_constraint,
    })
}

fn validate_query_envelope(select: &SelectStmt) -> Result<(), RelationBindError> {
    validate_query_envelope_core(select)?;
    if !select.order_by.is_empty() || select.limit.is_some() {
        return Err(RelationBindError::UnsupportedQueryShape);
    }
    Ok(())
}

fn validate_query_envelope_core(select: &SelectStmt) -> Result<(), RelationBindError> {
    if select.kind != SelectStatementKind::Select
        || select.is_in_braces
        || select.with.is_some()
        || !select.hints.is_empty()
        || select.calc_found_rows
        || select.distinct
        || select.all
        || !select.values.is_empty()
        || !select.group_by.is_empty()
        || select.rollup
        || select.having.is_some()
        || !select.windows.is_empty()
        || select.lock.is_some()
        || select.into_outfile.is_some()
    {
        return Err(RelationBindError::UnsupportedQueryShape);
    }
    Ok(())
}

fn two_table_refs(join: &Join) -> Result<(&TableRef, &TableRef), RelationBindError> {
    let right = join
        .right
        .as_ref()
        .ok_or(RelationBindError::ExactlyTwoBaseRelationsRequired)?;
    Ok((single_table_node(&join.left)?, single_table_node(right)?))
}

fn single_table_node(node: &JoinNode) -> Result<&TableRef, RelationBindError> {
    match node {
        JoinNode::Table(table) => Ok(table),
        JoinNode::Join(join) if join.right.is_none() => single_table_node(&join.left),
        JoinNode::Join(_) | JoinNode::Derived { .. } => {
            Err(RelationBindError::ExactlyTwoBaseRelationsRequired)
        }
    }
}

fn bind_relation(
    table_ref: &TableRef,
    catalog: &ConfiguredCatalog,
) -> Result<BoundRelation, RelationBindError> {
    if !table_ref.partitions.is_empty()
        || table_ref.as_of.is_some()
        || !table_ref.hints.is_empty()
        || table_ref.sample.is_some()
    {
        return Err(RelationBindError::UnsupportedTableOption);
    }
    let (schema, table) = match table_ref.name.as_slice() {
        [table] => (None, table.as_str()),
        [schema, table] => (Some(schema.as_str()), table.as_str()),
        path => return Err(RelationBindError::InvalidTablePath(path.to_vec())),
    };
    let table = catalog
        .resolve_table(schema, table)
        .map_err(RelationBindError::TableLookup)?;
    let qualifier = table_ref
        .alias
        .as_deref()
        .filter(|alias| !alias.is_empty())
        .unwrap_or(table.table())
        .to_owned();
    Ok(BoundRelation {
        table: table.clone(),
        qualifier,
    })
}

fn bind_projection(
    field: &SelectField,
    left: &BoundRelation,
    right: &BoundRelation,
    using: &[String],
) -> Result<BoundProjection, RelationBindError> {
    let SelectField::Expr {
        expr: Expr::Column(path),
        alias,
    } = field
    else {
        return Err(RelationBindError::UnsupportedProjection);
    };
    let (side, offset, column) = match resolve_column(path, left, right) {
        Ok(resolved) => resolved,
        Err(RelationBindError::AmbiguousColumn(_))
            if path.len() == 1
                && using.iter().any(|name| {
                    fold_identifier(name) == fold_identifier(&path[0])
                        && find_column(left, name).is_some()
                        && find_column(right, name).is_some()
                }) =>
        {
            let (offset, column) = find_column(left, &path[0])
                .expect("USING projection guard verified the left column");
            (RelationSide::Left, offset, column)
        }
        Err(error) => return Err(error),
    };
    Ok(BoundProjection {
        side,
        column_offset: offset,
        output_name: alias
            .as_deref()
            .filter(|alias| !alias.is_empty())
            .unwrap_or(column.name())
            .to_owned(),
        explicit_alias: alias.as_deref().is_some_and(|alias| !alias.is_empty()),
    })
}

fn bind_local_predicates(
    predicate: &Expr,
    left: &BoundRelation,
    right: &BoundRelation,
    output: &mut Vec<BoundLocalPredicate>,
) -> Result<(), RelationBindError> {
    match strip_parens(predicate) {
        Expr::Binary(BinaryOp::LogicAnd, lhs, rhs) => {
            bind_local_predicates(lhs, left, right, output)?;
            bind_local_predicates(rhs, left, right, output)
        }
        expression @ Expr::Binary(
            BinaryOp::Eq | BinaryOp::Ne | BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge,
            lhs,
            rhs,
        ) => {
            let side = match (bound_side(lhs, left, right)?, bound_side(rhs, left, right)?) {
                (Some(side), None) | (None, Some(side)) => side,
                (Some(left_side), Some(right_side)) if left_side != right_side => {
                    return Err(RelationBindError::CrossRelationWherePredicate);
                }
                _ => return Err(RelationBindError::UnsupportedPredicate),
            };
            output.push(BoundLocalPredicate {
                side,
                expression: expression.clone(),
            });
            Ok(())
        }
        _ => Err(RelationBindError::UnsupportedPredicate),
    }
}

fn bound_side(
    expression: &Expr,
    left: &BoundRelation,
    right: &BoundRelation,
) -> Result<Option<RelationSide>, RelationBindError> {
    match strip_parens(expression) {
        Expr::Column(path) => Ok(Some(resolve_column(path, left, right)?.0)),
        expr if parse_signed_integer(expr).is_some() => Ok(None),
        _ => Err(RelationBindError::UnsupportedPredicate),
    }
}

pub(crate) fn resolve_column<'a>(
    path: &[String],
    left: &'a BoundRelation,
    right: &'a BoundRelation,
) -> Result<(RelationSide, usize, &'a ConfiguredColumn), RelationBindError> {
    let (qualifier, name) = match path {
        [name] => (None, name.as_str()),
        [qualifier, name] => (Some(qualifier.as_str()), name.as_str()),
        [schema, qualifier, name] => {
            let side = relation_by_qualifier(qualifier, left, right)?;
            if fold_identifier(side.1.table().schema()) != fold_identifier(schema) {
                return Err(RelationBindError::UnknownQualifier(format!(
                    "{schema}.{qualifier}"
                )));
            }
            return column_on_side(path, name, side.0, side.1);
        }
        _ => return Err(RelationBindError::InvalidColumnPath(path.to_vec())),
    };
    if let Some(qualifier) = qualifier {
        let (side, relation) = relation_by_qualifier(qualifier, left, right)?;
        return column_on_side(path, name, side, relation);
    }

    let left_match = find_column(left, name);
    let right_match = find_column(right, name);
    match (left_match, right_match) {
        (Some((offset, column)), None) => Ok((RelationSide::Left, offset, column)),
        (None, Some((offset, column))) => Ok((RelationSide::Right, offset, column)),
        (None, None) => Err(RelationBindError::UnknownColumn(path.to_vec())),
        (Some(_), Some(_)) => Err(RelationBindError::AmbiguousColumn(path.to_vec())),
    }
}

fn relation_by_qualifier<'a>(
    qualifier: &str,
    left: &'a BoundRelation,
    right: &'a BoundRelation,
) -> Result<(RelationSide, &'a BoundRelation), RelationBindError> {
    if fold_identifier(left.qualifier()) == fold_identifier(qualifier) {
        Ok((RelationSide::Left, left))
    } else if fold_identifier(right.qualifier()) == fold_identifier(qualifier) {
        Ok((RelationSide::Right, right))
    } else {
        Err(RelationBindError::UnknownQualifier(qualifier.to_owned()))
    }
}

fn column_on_side<'a>(
    path: &[String],
    name: &str,
    side: RelationSide,
    relation: &'a BoundRelation,
) -> Result<(RelationSide, usize, &'a ConfiguredColumn), RelationBindError> {
    find_column(relation, name)
        .map(|(offset, column)| (side, offset, column))
        .ok_or_else(|| RelationBindError::UnknownColumn(path.to_vec()))
}

fn find_column<'a>(
    relation: &'a BoundRelation,
    name: &str,
) -> Option<(usize, &'a ConfiguredColumn)> {
    relation
        .table()
        .columns()
        .iter()
        .enumerate()
        .find(|(_, column)| fold_identifier(column.name()) == fold_identifier(name))
}

fn strip_parens(mut expression: &Expr) -> &Expr {
    while let Expr::Paren(inner) = expression {
        expression = inner;
    }
    expression
}

fn parse_signed_integer(expression: &Expr) -> Option<i64> {
    fn parse(expression: &Expr) -> Option<i128> {
        match expression {
            Expr::Int(text) => text.parse().ok(),
            Expr::Paren(inner) | Expr::Unary(UnaryOp::Plus, inner) => parse(inner),
            Expr::Unary(UnaryOp::Minus, inner) => parse(inner)?.checked_neg(),
            _ => None,
        }
    }
    i64::try_from(parse(expression)?).ok()
}
