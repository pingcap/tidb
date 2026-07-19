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

//! Typed two-relation plan over the immutable configured catalog.
//!
//! The plan retains TiDB's physical `FullSchema` order (`left` then `right`),
//! lowers each relation through the existing range/Selection scan planner,
//! and admits only one non-null signed-`BIGINT` cross-side equality. `USING`
//! coalesces visible metadata while keeping both physical input columns.

use std::{error::Error, fmt};

use tidb_ast::{BinaryOp, Expr, UnaryOp};

use crate::{
    configured_relation_tree::{
        BoundJoinConstraint, BoundRelation, ConfiguredRelationTree, RelationBindError, RelationSide,
    },
    join_condition::{
        ColumnSpec, EqualitySemantics, JoinCondition, JoinEquality, JoinSchema,
        UnsupportedJoinCondition,
    },
    read_only_scan::{
        configured_catalog::ConfiguredCatalog, fold_identifier, ConfiguredColumn,
        ReadOnlyScanError, ReadOnlyScanPlan,
    },
};

/// One physical source column in flattened `[left..., right...]` order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FullSchemaColumn {
    side: RelationSide,
    side_offset: usize,
    full_offset: usize,
    table_id: i64,
    column_id: i64,
    qualifier: String,
    name: String,
}

impl FullSchemaColumn {
    /// Returns the physical input that owns this column.
    #[must_use]
    pub const fn side(&self) -> RelationSide {
        self.side
    }

    /// Returns this column's source-order offset within its input.
    #[must_use]
    pub const fn side_offset(&self) -> usize {
        self.side_offset
    }

    /// Returns this column's offset in `[left..., right...]` FullSchema.
    #[must_use]
    pub const fn full_offset(&self) -> usize {
        self.full_offset
    }

    /// Returns the stable physical TiDB table ID.
    #[must_use]
    pub const fn table_id(&self) -> i64 {
        self.table_id
    }

    /// Returns the stable TiDB column ID.
    #[must_use]
    pub const fn column_id(&self) -> i64 {
        self.column_id
    }

    /// Returns the SQL-visible relation qualifier.
    #[must_use]
    pub fn qualifier(&self) -> &str {
        &self.qualifier
    }

    /// Returns the original configured column spelling.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Query-visible projection metadata in written field order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JoinProjection {
    output_name: String,
    full_offset: usize,
}

impl JoinProjection {
    /// Returns the MySQL-visible output name.
    #[must_use]
    pub fn output_name(&self) -> &str {
        &self.output_name
    }

    /// Returns the projected column's FullSchema offset.
    #[must_use]
    pub const fn full_offset(&self) -> usize {
        self.full_offset
    }
}

/// Complete bounded planner output consumed by the multi-read join runtime.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfiguredJoinPlan {
    left_scan: ReadOnlyScanPlan,
    right_scan: ReadOnlyScanPlan,
    full_schema: Vec<FullSchemaColumn>,
    visible_full_offsets: Vec<usize>,
    projections: Vec<JoinProjection>,
    equality: Option<JoinEquality>,
}

impl ConfiguredJoinPlan {
    /// Binds and lowers one configured two-relation query.
    pub fn lower(sql: &str, catalog: &ConfiguredCatalog) -> Result<Self, ConfiguredJoinPlanError> {
        let relation_tree = bind_with_using_projection_coalescing(sql, catalog)?;
        let join_schema = join_schema(&relation_tree);
        let bound_join = bind_join_constraint(&relation_tree, &join_schema)?;

        let left_scan = lower_relation_scan(&relation_tree, RelationSide::Left)?;
        let right_scan = lower_relation_scan(&relation_tree, RelationSide::Right)?;
        let full_schema = full_schema(&relation_tree);
        let left_width = relation_tree.left().table().columns().len();
        let visible_full_offsets = match bound_join.using_full_offsets {
            Some((left_key, right_key)) => std::iter::once(left_key)
                .chain((0..left_width).filter(|offset| *offset != left_key))
                .chain((left_width..full_schema.len()).filter(|offset| *offset != right_key))
                .collect(),
            None => (0..full_schema.len()).collect(),
        };
        let projections = relation_tree
            .projections()
            .iter()
            .map(|projection| JoinProjection {
                output_name: projection.output_name().to_owned(),
                full_offset: match projection.side() {
                    RelationSide::Left => projection.column_offset(),
                    RelationSide::Right => left_width + projection.column_offset(),
                },
            })
            .collect();

        Ok(Self {
            left_scan,
            right_scan,
            full_schema,
            visible_full_offsets,
            projections,
            equality: bound_join.equality,
        })
    }

    /// Returns the left physical scan plan.
    #[must_use]
    pub const fn left_scan(&self) -> &ReadOnlyScanPlan {
        &self.left_scan
    }

    /// Returns the right physical scan plan.
    #[must_use]
    pub const fn right_scan(&self) -> &ReadOnlyScanPlan {
        &self.right_scan
    }

    /// Returns physical columns in stable `[left..., right...]` order.
    #[must_use]
    pub fn full_schema(&self) -> &[FullSchemaColumn] {
        &self.full_schema
    }

    /// Returns visible join-schema columns as indices into `full_schema`.
    #[must_use]
    pub fn visible_full_offsets(&self) -> &[usize] {
        &self.visible_full_offsets
    }

    /// Returns direct query projections in written field order.
    #[must_use]
    pub fn projections(&self) -> &[JoinProjection] {
        &self.projections
    }

    /// Returns the typed join key, or `None` for CROSS/comma syntax.
    #[must_use]
    pub const fn equality(&self) -> Option<&JoinEquality> {
        self.equality.as_ref()
    }
}

/// Explicit rejection at the configured join-plan boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfiguredJoinPlanError {
    /// Stage B rejected the relation tree or name binding.
    RelationBinding(RelationBindError),
    /// The existing single-table planner rejected the left input.
    LeftScan(ReadOnlyScanError),
    /// The existing single-table planner rejected the right input.
    RightScan(ReadOnlyScanError),
    /// The join expression is outside the direct equality boundary.
    UnsupportedJoinCondition(UnsupportedJoinCondition),
    /// Null-safe equality is excluded from the non-null `BIGINT` milestone.
    NullSafeEquality,
    /// The first USING boundary admits exactly one key.
    ExactlyOneUsingColumnRequired,
    /// Stage B supplied a predicate outside its promised local grammar.
    InvalidLocalPredicate,
}

impl fmt::Display for ConfiguredJoinPlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "configured join planning failed: {self:?}")
    }
}

impl Error for ConfiguredJoinPlanError {}

fn bind_with_using_projection_coalescing(
    sql: &str,
    catalog: &ConfiguredCatalog,
) -> Result<ConfiguredRelationTree, ConfiguredJoinPlanError> {
    match ConfiguredRelationTree::bind_sql(sql, catalog) {
        Ok(tree) => Ok(tree),
        Err(RelationBindError::AmbiguousColumn(path)) if path.len() == 1 => {
            let stmt = tidb_parser::parse(sql).map_err(|error| {
                ConfiguredJoinPlanError::RelationBinding(RelationBindError::Parse(error.message))
            })?;
            let rewritten = rewrite_using_projection(stmt, &path[0]).ok_or({
                ConfiguredJoinPlanError::RelationBinding(RelationBindError::AmbiguousColumn(path))
            })?;
            ConfiguredRelationTree::bind_sql(&rewritten, catalog)
                .map_err(ConfiguredJoinPlanError::RelationBinding)
        }
        Err(error) => Err(ConfiguredJoinPlanError::RelationBinding(error)),
    }
}

fn rewrite_using_projection(mut stmt: tidb_ast::Stmt, ambiguous: &str) -> Option<String> {
    let tidb_ast::Stmt::Query(query) = &mut stmt else {
        return None;
    };
    let tidb_ast::QueryStmt::Select(select) = query.as_mut() else {
        return None;
    };
    let join = select.from.as_ref()?;
    if join.using.len() != 1 || fold_identifier(&join.using[0]) != fold_identifier(ambiguous) {
        return None;
    }
    let qualifier = left_qualifier(&join.left)?;
    let mut changed = false;
    for field in &mut select.fields {
        if let tidb_ast::SelectField::Expr {
            expr: Expr::Column(path),
            ..
        } = field
        {
            if path.len() == 1 && fold_identifier(&path[0]) == fold_identifier(ambiguous) {
                *path = vec![qualifier.clone(), path[0].clone()];
                changed = true;
            }
        }
    }
    changed.then(|| stmt.restore())
}

fn left_qualifier(node: &tidb_ast::JoinNode) -> Option<String> {
    match node {
        tidb_ast::JoinNode::Table(table) => Some(
            table
                .alias
                .as_deref()
                .filter(|alias| !alias.is_empty())
                .or_else(|| table.name.last().map(String::as_str))?
                .to_owned(),
        ),
        tidb_ast::JoinNode::Join(join) if join.right.is_none() => left_qualifier(&join.left),
        tidb_ast::JoinNode::Join(_) | tidb_ast::JoinNode::Derived { .. } => None,
    }
}

fn join_schema(tree: &ConfiguredRelationTree) -> JoinSchema {
    JoinSchema::new(schema_columns(tree.left()), schema_columns(tree.right()))
}

fn schema_columns(relation: &BoundRelation) -> Vec<ColumnSpec> {
    relation
        .table()
        .columns()
        .iter()
        .map(|column| {
            ColumnSpec::with_qualifiers(
                fold_identifier(column.name()),
                [fold_identifier(relation.qualifier())],
                false,
            )
        })
        .collect()
}

struct BoundJoin {
    equality: Option<JoinEquality>,
    using_full_offsets: Option<(usize, usize)>,
}

fn bind_join_constraint(
    tree: &ConfiguredRelationTree,
    schema: &JoinSchema,
) -> Result<BoundJoin, ConfiguredJoinPlanError> {
    match tree.join_constraint() {
        BoundJoinConstraint::Cross => Ok(BoundJoin {
            equality: None,
            using_full_offsets: None,
        }),
        BoundJoinConstraint::On(expression) => {
            let normalized = normalize_on_equality(expression, tree)?;
            let condition = schema.classify_on(&normalized);
            let equality = require_equality(condition)?;
            Ok(BoundJoin {
                equality: Some(equality),
                using_full_offsets: None,
            })
        }
        BoundJoinConstraint::Using(names) => {
            let [name] = names.as_slice() else {
                return Err(ConfiguredJoinPlanError::ExactlyOneUsingColumnRequired);
            };
            let left_offset = find_column_offset(tree.left(), name).ok_or_else(|| {
                ConfiguredJoinPlanError::UnsupportedJoinCondition(
                    UnsupportedJoinCondition::UnknownColumn {
                        path: vec![name.clone()],
                    },
                )
            })?;
            let right_side_offset = find_column_offset(tree.right(), name).ok_or_else(|| {
                ConfiguredJoinPlanError::UnsupportedJoinCondition(
                    UnsupportedJoinCondition::UnknownColumn {
                        path: vec![name.clone()],
                    },
                )
            })?;
            let normalized = fold_identifier(name);
            let mut conditions = schema.bind_using([normalized]);
            let equality = require_equality(conditions.remove(0))?;
            let right_offset = tree.left().table().columns().len() + right_side_offset;
            Ok(BoundJoin {
                equality: Some(equality),
                using_full_offsets: Some((left_offset, right_offset)),
            })
        }
    }
}

fn require_equality(condition: JoinCondition) -> Result<JoinEquality, ConfiguredJoinPlanError> {
    match condition {
        JoinCondition::Equality(equality)
            if equality.semantics() == EqualitySemantics::ThreeValued =>
        {
            Ok(equality)
        }
        JoinCondition::Equality(_) => Err(ConfiguredJoinPlanError::NullSafeEquality),
        JoinCondition::Unsupported(error) => {
            Err(ConfiguredJoinPlanError::UnsupportedJoinCondition(error))
        }
    }
}

fn normalize_on_equality(
    expression: &Expr,
    tree: &ConfiguredRelationTree,
) -> Result<Expr, ConfiguredJoinPlanError> {
    let Expr::Binary(BinaryOp::Eq, lhs, rhs) = strip_parens(expression) else {
        return if matches!(
            strip_parens(expression),
            Expr::Binary(BinaryOp::NullEq, _, _)
        ) {
            Err(ConfiguredJoinPlanError::NullSafeEquality)
        } else {
            Err(ConfiguredJoinPlanError::UnsupportedJoinCondition(
                UnsupportedJoinCondition::Other,
            ))
        };
    };
    let (left_side, left_column) = resolve_join_column(lhs, tree)?;
    let (right_side, right_column) = resolve_join_column(rhs, tree)?;
    if left_side == right_side {
        return Err(ConfiguredJoinPlanError::UnsupportedJoinCondition(
            UnsupportedJoinCondition::SameSide {
                side: match left_side {
                    RelationSide::Left => crate::join_condition::JoinSide::Left,
                    RelationSide::Right => crate::join_condition::JoinSide::Right,
                },
            },
        ));
    }
    let canonical = |side: RelationSide, column: &ConfiguredColumn| {
        let relation = match side {
            RelationSide::Left => tree.left(),
            RelationSide::Right => tree.right(),
        };
        Expr::Column(vec![
            fold_identifier(relation.qualifier()),
            fold_identifier(column.name()),
        ])
    };
    Ok(Expr::Binary(
        BinaryOp::Eq,
        Box::new(canonical(left_side, left_column)),
        Box::new(canonical(right_side, right_column)),
    ))
}

fn resolve_join_column<'a>(
    expression: &Expr,
    tree: &'a ConfiguredRelationTree,
) -> Result<(RelationSide, &'a ConfiguredColumn), ConfiguredJoinPlanError> {
    let Expr::Column(path) = strip_parens(expression) else {
        return Err(ConfiguredJoinPlanError::UnsupportedJoinCondition(
            UnsupportedJoinCondition::NonColumnOperand,
        ));
    };
    let (qualifier, name) = match path.as_slice() {
        [name] => (None, name.as_str()),
        [qualifier, name] | [_, qualifier, name] => (Some(qualifier.as_str()), name.as_str()),
        _ => {
            return Err(ConfiguredJoinPlanError::UnsupportedJoinCondition(
                UnsupportedJoinCondition::InvalidColumnPath,
            ))
        }
    };
    let candidates = [
        (RelationSide::Left, tree.left()),
        (RelationSide::Right, tree.right()),
    ]
    .into_iter()
    .filter(|(_, relation)| {
        qualifier.is_none_or(|qualifier| {
            fold_identifier(relation.qualifier()) == fold_identifier(qualifier)
        })
    })
    .filter_map(|(side, relation)| find_column(relation, name).map(|column| (side, column)))
    .collect::<Vec<_>>();
    match candidates.as_slice() {
        [candidate] => Ok(*candidate),
        [] => Err(ConfiguredJoinPlanError::UnsupportedJoinCondition(
            UnsupportedJoinCondition::UnknownColumn { path: path.clone() },
        )),
        _ => Err(ConfiguredJoinPlanError::UnsupportedJoinCondition(
            UnsupportedJoinCondition::AmbiguousColumn { path: path.clone() },
        )),
    }
}

fn lower_relation_scan(
    tree: &ConfiguredRelationTree,
    side: RelationSide,
) -> Result<ReadOnlyScanPlan, ConfiguredJoinPlanError> {
    let relation = match side {
        RelationSide::Left => tree.left(),
        RelationSide::Right => tree.right(),
    };
    let mut sql = String::from("SELECT ");
    for (index, column) in relation.table().columns().iter().enumerate() {
        if index > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&quoted(column.name()));
    }
    sql.push_str(" FROM ");
    sql.push_str(&quoted(relation.table().schema()));
    sql.push('.');
    sql.push_str(&quoted(relation.table().table()));
    sql.push_str(" AS ");
    sql.push_str(&quoted(relation.qualifier()));

    let predicates = tree
        .local_predicates()
        .iter()
        .filter(|predicate| predicate.side() == side)
        .map(|predicate| render_local_expression(predicate.expression()))
        .collect::<Result<Vec<_>, _>>()?;
    if !predicates.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&predicates.join(" AND "));
    }
    ReadOnlyScanPlan::lower(&sql, relation.table()).map_err(|error| match side {
        RelationSide::Left => ConfiguredJoinPlanError::LeftScan(error),
        RelationSide::Right => ConfiguredJoinPlanError::RightScan(error),
    })
}

fn render_local_expression(expression: &Expr) -> Result<String, ConfiguredJoinPlanError> {
    match expression {
        Expr::Column(path) => Ok(path
            .iter()
            .map(|part| quoted(part))
            .collect::<Vec<_>>()
            .join(".")),
        Expr::Int(text) => Ok(text.clone()),
        Expr::Unary(UnaryOp::Plus, inner) => Ok(format!("+{}", render_local_expression(inner)?)),
        Expr::Unary(UnaryOp::Minus, inner) => Ok(format!("-{}", render_local_expression(inner)?)),
        Expr::Paren(inner) => Ok(format!("({})", render_local_expression(inner)?)),
        Expr::Binary(operator, lhs, rhs) => {
            let operator = match operator {
                BinaryOp::Eq => "=",
                BinaryOp::Ne => "!=",
                BinaryOp::Lt => "<",
                BinaryOp::Le => "<=",
                BinaryOp::Gt => ">",
                BinaryOp::Ge => ">=",
                _ => return Err(ConfiguredJoinPlanError::InvalidLocalPredicate),
            };
            Ok(format!(
                "{} {operator} {}",
                render_local_expression(lhs)?,
                render_local_expression(rhs)?
            ))
        }
        _ => Err(ConfiguredJoinPlanError::InvalidLocalPredicate),
    }
}

fn quoted(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}

fn full_schema(tree: &ConfiguredRelationTree) -> Vec<FullSchemaColumn> {
    let left_width = tree.left().table().columns().len();
    relation_full_schema(tree.left(), RelationSide::Left, 0)
        .chain(relation_full_schema(
            tree.right(),
            RelationSide::Right,
            left_width,
        ))
        .collect()
}

fn relation_full_schema<'a>(
    relation: &'a BoundRelation,
    side: RelationSide,
    base: usize,
) -> impl Iterator<Item = FullSchemaColumn> + 'a {
    relation
        .table()
        .columns()
        .iter()
        .enumerate()
        .map(move |(side_offset, column)| FullSchemaColumn {
            side,
            side_offset,
            full_offset: base + side_offset,
            table_id: relation.table().table_id(),
            column_id: column.id(),
            qualifier: relation.qualifier().to_owned(),
            name: column.name().to_owned(),
        })
}

fn find_column_offset(relation: &BoundRelation, name: &str) -> Option<usize> {
    relation
        .table()
        .columns()
        .iter()
        .position(|column| fold_identifier(column.name()) == fold_identifier(name))
}

fn find_column<'a>(relation: &'a BoundRelation, name: &str) -> Option<&'a ConfiguredColumn> {
    find_column_offset(relation, name).map(|offset| &relation.table().columns()[offset])
}

fn strip_parens(mut expression: &Expr) -> &Expr {
    while let Expr::Paren(inner) = expression {
        expression = inner;
    }
    expression
}
