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

//! Typed ORDER BY/LIMIT lowering for the configured two-relation node.
//!
//! This is deliberately a narrow bridge from one parsed `SELECT` into the
//! existing relation binder and join-plan lowering. It resolves only direct
//! signed-`BIGINT` FullSchema columns, explicit aliases, and positive
//! projection ordinals. Expressions, coercions, collation, partition TopN,
//! storage pushdown, and general physical planning stay outside this boundary.

use std::{error::Error, fmt};

use tidb_ast::{Expr, Limit, OrderItem, SelectStmt};

use crate::{
    configured_join_plan::{ConfiguredJoinPlan, ConfiguredJoinPlanError},
    configured_order_limit_contract::{
        ConfiguredLimitWindow, ConfiguredLimitWindowError, ConfiguredOrderDirection,
        ConfiguredOrderKey, ConfiguredOrderLimitSpec, ConfiguredOrderLimitSpecError,
    },
    configured_relation_tree::{
        bind_select_with_order_limit, parse_select, resolve_column, ConfiguredRelationTree,
        RelationBindError, RelationSide,
    },
    read_only_scan::{configured_catalog::ConfiguredCatalog, fold_identifier},
};

/// The executable tail shape admitted by the configured planner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfiguredOrderLimit {
    /// A checked LIMIT with no ordering requirement.
    Limit(ConfiguredLimitWindow),
    /// A checked bounded TopN over resolved physical FullSchema keys.
    TopN(ConfiguredOrderLimitSpec),
}

/// A configured join plus its executable ORDER BY/LIMIT tail.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfiguredOrderedJoinPlan {
    /// The fully bound join shape, retained even for a planner-known empty
    /// result so the protocol layer can emit its normal column metadata.
    join: ConfiguredJoinPlan,
    order_limit: Option<ConfiguredOrderLimit>,
    /// Whether execution must open physical readers for this result.
    requires_input: bool,
}

impl ConfiguredOrderedJoinPlan {
    /// Parses once, binds the typed core and tail, then lowers the join.
    pub fn lower(
        sql: &str,
        catalog: &ConfiguredCatalog,
    ) -> Result<Self, ConfiguredOrderLimitError> {
        let select = parse_select(sql).map_err(|error| match error {
            // The parser rejects unsigned literals beyond its token domain
            // before it can construct `Limit`. That is still the configured
            // tail's typed invalid-literal contract, not a relation failure.
            RelationBindError::Parse(message) if message == "LIMIT value out of range" => {
                ConfiguredOrderLimitError::InvalidLimitLiteral
            }
            error => ConfiguredOrderLimitError::RelationBinding(error),
        })?;
        Self::lower_select(&select, catalog)
    }

    /// Lowers an already-parsed SELECT without restoring or reparsing SQL.
    pub fn lower_select(
        select: &SelectStmt,
        catalog: &ConfiguredCatalog,
    ) -> Result<Self, ConfiguredOrderLimitError> {
        let tree = bind_select_with_order_limit(select, catalog)
            .map_err(ConfiguredOrderLimitError::RelationBinding)?;
        let order_limit = bind_order_limit(select, &tree)?;

        let requires_input = !order_limit.as_ref().is_some_and(|tail| match tail {
            ConfiguredOrderLimit::Limit(limit) => limit.is_empty(),
            ConfiguredOrderLimit::TopN(spec) => spec.limit().is_empty(),
        });
        let join = ConfiguredJoinPlan::lower_relation_tree(&tree)
            .map_err(ConfiguredOrderLimitError::JoinPlanning)?;
        Ok(Self {
            join,
            order_limit,
            requires_input,
        })
    }

    /// Returns no execution join plan when LIMIT 0 made the result
    /// planner-known empty.
    ///
    /// Use [`Self::metadata_join`] for the fully bound shape that still
    /// supplies ordinary result metadata to the MySQL protocol layer.
    #[must_use]
    pub const fn join(&self) -> Option<&ConfiguredJoinPlan> {
        if self.requires_input {
            Some(&self.join)
        } else {
            None
        }
    }

    /// Returns the typed join shape used for normal metadata, including
    /// planner-known empty `LIMIT 0` results.
    #[must_use]
    pub const fn metadata_join(&self) -> &ConfiguredJoinPlan {
        &self.join
    }

    /// Returns the checked executable tail, if the SELECT supplied one.
    #[must_use]
    pub const fn order_limit(&self) -> Option<&ConfiguredOrderLimit> {
        self.order_limit.as_ref()
    }

    /// Returns whether this plan is empty before any scan or PD work.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        !self.requires_input
    }
}

/// Explicit rejection at the configured ORDER BY/LIMIT boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfiguredOrderLimitError {
    /// Parsing or relation binding rejected the non-tail query shape.
    RelationBinding(RelationBindError),
    /// The existing typed join lowering rejected the bounded core.
    JoinPlanning(ConfiguredJoinPlanError),
    /// ORDER BY is not executable without an explicit checked LIMIT.
    OrderRequiresLimit,
    /// LIMIT is absent, parameterized, negative, non-integral, or too large.
    InvalidLimitLiteral,
    /// The checked offset plus count cannot fit the runtime index domain.
    LimitWindow(ConfiguredLimitWindowError),
    /// An ORDER BY item is neither a direct column, an alias, nor an ordinal.
    UnsupportedOrderExpression,
    /// A direct configured ORDER BY column failed relation binding.
    OrderColumn(RelationBindError),
    /// More than one explicit projection alias has the requested name.
    AmbiguousOrderAlias(String),
    /// An ordinal is zero or lies outside the configured projection list.
    InvalidOrderOrdinal,
    /// The shared TopN contract rejected this otherwise bound tail.
    OrderLimitSpec(ConfiguredOrderLimitSpecError),
}

impl fmt::Display for ConfiguredOrderLimitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "configured ORDER BY/LIMIT planning failed: {self:?}"
        )
    }
}

impl Error for ConfiguredOrderLimitError {}

fn bind_order_limit(
    select: &SelectStmt,
    tree: &ConfiguredRelationTree,
) -> Result<Option<ConfiguredOrderLimit>, ConfiguredOrderLimitError> {
    let Some(limit) = &select.limit else {
        return if select.order_by.is_empty() {
            Ok(None)
        } else {
            Err(ConfiguredOrderLimitError::OrderRequiresLimit)
        };
    };
    let limit = bind_limit(limit)?;
    if select.order_by.is_empty() {
        return Ok(Some(ConfiguredOrderLimit::Limit(limit)));
    }

    let keys = select
        .order_by
        .iter()
        .map(|item| bind_order_item(item, tree))
        .collect::<Result<Vec<_>, _>>()?;
    let spec = ConfiguredOrderLimitSpec::new(keys, limit)
        .map_err(ConfiguredOrderLimitError::OrderLimitSpec)?;
    Ok(Some(ConfiguredOrderLimit::TopN(spec)))
}

fn bind_limit(limit: &Limit) -> Result<ConfiguredLimitWindow, ConfiguredOrderLimitError> {
    let offset = limit
        .offset
        .as_ref()
        .map(parse_nonnegative_limit_literal)
        .transpose()?
        .unwrap_or(0);
    let count = parse_nonnegative_limit_literal(&limit.count)?;
    ConfiguredLimitWindow::new(offset, count).map_err(ConfiguredOrderLimitError::LimitWindow)
}

fn parse_nonnegative_limit_literal(expression: &Expr) -> Result<usize, ConfiguredOrderLimitError> {
    let expression = strip_parens(expression);
    let Expr::Int(text) = expression else {
        return Err(ConfiguredOrderLimitError::InvalidLimitLiteral);
    };
    text.parse::<u128>()
        .ok()
        .and_then(|value| usize::try_from(value).ok())
        .ok_or(ConfiguredOrderLimitError::InvalidLimitLiteral)
}

fn bind_order_item(
    item: &OrderItem,
    tree: &ConfiguredRelationTree,
) -> Result<ConfiguredOrderKey, ConfiguredOrderLimitError> {
    let full_offset = bind_order_expression(&item.expr, tree)?;
    Ok(ConfiguredOrderKey::new(
        full_offset,
        ConfiguredOrderDirection::from_descending(item.desc),
    ))
}

fn bind_order_expression(
    expression: &Expr,
    tree: &ConfiguredRelationTree,
) -> Result<usize, ConfiguredOrderLimitError> {
    let expression = strip_parens(expression);
    if let Expr::Int(text) = expression {
        let ordinal = text
            .parse::<u128>()
            .ok()
            .and_then(|value| usize::try_from(value).ok())
            .and_then(|value| value.checked_sub(1))
            .ok_or(ConfiguredOrderLimitError::InvalidOrderOrdinal)?;
        return tree
            .projections()
            .get(ordinal)
            .map(|projection| full_offset(tree, projection.side(), projection.column_offset()))
            .ok_or(ConfiguredOrderLimitError::InvalidOrderOrdinal);
    }

    let Expr::Column(path) = expression else {
        return Err(ConfiguredOrderLimitError::UnsupportedOrderExpression);
    };
    if let [name] = path.as_slice() {
        let aliases = tree
            .projections()
            .iter()
            .filter(|projection| {
                projection.has_explicit_alias()
                    && fold_identifier(projection.output_name()) == fold_identifier(name)
            })
            .collect::<Vec<_>>();
        match aliases.as_slice() {
            [projection] => {
                return Ok(full_offset(
                    tree,
                    projection.side(),
                    projection.column_offset(),
                ));
            }
            [] => {}
            _ => return Err(ConfiguredOrderLimitError::AmbiguousOrderAlias(name.clone())),
        }
    }

    let (side, side_offset, _) = resolve_column(path, tree.left(), tree.right())
        .map_err(ConfiguredOrderLimitError::OrderColumn)?;
    Ok(full_offset(tree, side, side_offset))
}

fn full_offset(tree: &ConfiguredRelationTree, side: RelationSide, side_offset: usize) -> usize {
    match side {
        RelationSide::Left => side_offset,
        RelationSide::Right => tree.left().table().columns().len() + side_offset,
    }
}

fn strip_parens(mut expression: &Expr) -> &Expr {
    while let Expr::Paren(inner) = expression {
        expression = inner;
    }
    expression
}
