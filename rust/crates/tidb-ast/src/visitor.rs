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

//! Package-wide mutable AST traversal transcreated from Go's `Node.Accept`.

use std::any::Any;

/// Receives every AST node in pre-order and post-order.
///
/// `enter` may mutate or replace the concrete value through [`Any`]. Returning
/// `true` skips its children but still calls `leave`, matching Go's visitor
/// contract. Returning `false` from `leave` stops the complete traversal.
pub trait Visitor {
    /// Called before a node's children. Return `true` to skip the children.
    fn enter(&mut self, node: &mut dyn Any) -> bool;

    /// Called after the children, or immediately after a skipped `enter`.
    /// Return `false` to stop traversal.
    fn leave(&mut self, node: &mut dyn Any) -> bool;
}

/// A node whose complete mutable child graph can accept a [`Visitor`].
pub trait Visitable: Any {
    /// Traverses this node and returns `false` when the visitor stops early.
    fn accept<V: Visitor>(&mut self, visitor: &mut V) -> bool;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Expr, ProcedureStatement};

    #[derive(Default)]
    struct ExprCounter {
        entered: usize,
        left: usize,
    }

    impl Visitor for ExprCounter {
        fn enter(&mut self, node: &mut dyn Any) -> bool {
            if node.is::<Expr>() {
                self.entered += 1;
            }
            false
        }

        fn leave(&mut self, node: &mut dyn Any) -> bool {
            if node.is::<Expr>() {
                self.left += 1;
            }
            true
        }
    }

    fn expression_tree() -> Expr {
        Expr::Case {
            value: Some(Box::new(Expr::Int("0".to_string()))),
            when_clauses: vec![(
                Expr::Int("1".to_string()),
                Expr::Binary(
                    crate::BinaryOp::Plus,
                    Box::new(Expr::Int("2".to_string())),
                    Box::new(Expr::Int("3".to_string())),
                ),
            )],
            else_clause: Some(Box::new(Expr::Int("4".to_string()))),
        }
    }

    /// Transcreates the expression/function visitor coverage assertions from
    /// `expressions_test.go` and `functions_test.go`.
    #[test]
    fn expression_children_receive_balanced_enter_and_leave_calls() {
        let mut expression = expression_tree();
        let mut visitor = ExprCounter::default();
        assert!(expression.accept(&mut visitor));
        assert_eq!(visitor.entered, 7);
        assert_eq!(visitor.left, 7);
    }

    struct SkipCase {
        entered: usize,
        left: usize,
    }

    impl Visitor for SkipCase {
        fn enter(&mut self, node: &mut dyn Any) -> bool {
            if let Some(expression) = node.downcast_mut::<Expr>() {
                self.entered += 1;
                return matches!(expression, Expr::Case { .. });
            }
            false
        }

        fn leave(&mut self, node: &mut dyn Any) -> bool {
            if node.is::<Expr>() {
                self.left += 1;
            }
            true
        }
    }

    /// Go calls `Leave` even when `Enter` skips a node's children.
    #[test]
    fn skip_children_still_leaves_the_node() {
        let mut expression = expression_tree();
        let mut visitor = SkipCase {
            entered: 0,
            left: 0,
        };
        assert!(expression.accept(&mut visitor));
        assert_eq!((visitor.entered, visitor.left), (1, 1));
    }

    struct ReplaceInteger;

    impl Visitor for ReplaceInteger {
        fn enter(&mut self, node: &mut dyn Any) -> bool {
            if let Some(expression @ Expr::Int(_)) = node.downcast_mut::<Expr>() {
                *expression = Expr::String("replaced".to_string());
            }
            false
        }

        fn leave(&mut self, _node: &mut dyn Any) -> bool {
            true
        }
    }

    /// Rust replaces expression variants in place, preserving Go's allowance
    /// for `Leave`/`Enter` to return a different concrete expression type.
    #[test]
    fn visitor_can_replace_expression_variants() {
        let mut expression = Expr::Row(vec![Expr::Int("1".to_string())]);
        assert!(expression.accept(&mut ReplaceInteger));
        assert_eq!(expression.restore(), "ROW(_UTF8MB4'replaced')");
    }

    /// Transcreates the stored-procedure visitor coverage: expressions nested
    /// below procedure control-flow nodes participate in the same traversal.
    #[test]
    fn procedure_control_flow_reaches_nested_expressions() {
        let mut statement = ProcedureStatement::While {
            condition: Expr::Binary(
                crate::BinaryOp::Lt,
                Box::new(Expr::Column(vec!["id".to_string()])),
                Box::new(Expr::Int("10".to_string())),
            ),
            body: Vec::new(),
        };
        let mut visitor = ExprCounter::default();
        assert!(statement.accept(&mut visitor));
        assert_eq!((visitor.entered, visitor.left), (3, 3));
    }

    struct StopAtFirstInteger {
        left_expressions: usize,
    }

    impl Visitor for StopAtFirstInteger {
        fn enter(&mut self, _node: &mut dyn Any) -> bool {
            false
        }

        fn leave(&mut self, node: &mut dyn Any) -> bool {
            if let Some(Expr::Int(_)) = node.downcast_mut::<Expr>() {
                return false;
            }
            if node.is::<Expr>() {
                self.left_expressions += 1;
            }
            true
        }
    }

    /// A false `Leave` result stops siblings and suppresses parent `Leave`,
    /// matching Go's `ok == false` propagation.
    #[test]
    fn false_leave_stops_the_complete_traversal() {
        let mut expression =
            Expr::Row(vec![Expr::Int("1".to_string()), Expr::Int("2".to_string())]);
        let mut visitor = StopAtFirstInteger {
            left_expressions: 0,
        };
        assert!(!expression.accept(&mut visitor));
        assert_eq!(visitor.left_expressions, 0);
    }
}
