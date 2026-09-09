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

//! Borrow unchanged AST adapters, like Go's rules retain unchanged plan nodes.

use std::borrow::Cow;
use tidb_ast::{Join, JoinNode, QueryStmt, SelectStmt};

/// Rewrite the derived SELECT children, left to right. The rule owns recursion
/// inside each derived SELECT and decides whether to replace it. Set operations
/// remain outside these SELECT rules, as in their original traversal.
pub(super) fn rewrite_derived_selects<'a>(
    select: &'a SelectStmt,
    rewrite: &mut impl FnMut(&SelectStmt) -> Option<SelectStmt>,
) -> Cow<'a, SelectStmt> {
    let Some(from) = select
        .from
        .as_ref()
        .and_then(|from| rewrite_join(from, rewrite))
    else {
        return Cow::Borrowed(select);
    };
    Cow::Owned(replace_from(select, from))
}

/// Replace a proven-rewritten FROM without first copying its obsolete tree.
pub(super) fn replace_from(select: &SelectStmt, from: Join) -> SelectStmt {
    SelectStmt {
        from: Some(from),
        kind: select.kind,
        is_in_braces: select.is_in_braces,
        with: select.with.clone(),
        hints: select.hints.clone(),
        priority: select.priority,
        sql_small_result: select.sql_small_result,
        sql_big_result: select.sql_big_result,
        sql_buffer_result: select.sql_buffer_result,
        sql_no_cache: select.sql_no_cache,
        straight_join: select.straight_join,
        calc_found_rows: select.calc_found_rows,
        distinct: select.distinct,
        all: select.all,
        fields: select.fields.clone(),
        values: select.values.clone(),
        where_clause: select.where_clause.clone(),
        group_by: select.group_by.clone(),
        rollup: select.rollup,
        having: select.having.clone(),
        windows: select.windows.clone(),
        order_by: select.order_by.clone(),
        limit: select.limit.clone(),
        lock: select.lock.clone(),
        into_outfile: select.into_outfile.clone(),
        into_vars: select.into_vars.clone(),
    }
}

fn rewrite_join(
    join: &Join,
    rewrite: &mut impl FnMut(&SelectStmt) -> Option<SelectStmt>,
) -> Option<Join> {
    let left = rewrite_node(&join.left, rewrite);
    let right = join
        .right
        .as_ref()
        .and_then(|node| rewrite_node(node, rewrite));
    if left.is_none() && right.is_none() {
        return None;
    }
    Some(Join {
        left: left.unwrap_or_else(|| join.left.clone()),
        right: right.or_else(|| join.right.clone()),
        tp: join.tp,
        straight: join.straight,
        on: join.on.clone(),
        using: join.using.clone(),
        natural: join.natural,
        explicit_parens: join.explicit_parens,
    })
}

fn rewrite_node(
    node: &JoinNode,
    rewrite: &mut impl FnMut(&SelectStmt) -> Option<SelectStmt>,
) -> Option<JoinNode> {
    match node {
        JoinNode::Table(_) => None,
        JoinNode::Join(join) => {
            rewrite_join(join, rewrite).map(|join| JoinNode::Join(Box::new(join)))
        }
        JoinNode::Derived {
            subquery,
            alias,
            lateral,
            column_names,
        } => {
            let QueryStmt::Select(select) = &**subquery else {
                return None;
            };
            let mut replacement =
                tidb_ast::NodeBox::new(QueryStmt::Select(Box::new(rewrite(select)?)));
            *replacement.node_text_mut() = subquery.node_text().clone();
            Some(JoinNode::Derived {
                subquery: replacement,
                alias: alias.clone(),
                lateral: *lateral,
                column_names: column_names.clone(),
            })
        }
    }
}
