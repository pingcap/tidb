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

//! TiFlash plan telemetry classification from `pkg/planner/core/telemetry.go`.
//!
//! This leaf preserves `IsTiFlashContained`'s source traversal over an opaque
//! plan tree: Explain nodes unwrap their target, non-physical nodes stop the
//! walk, TableReader nodes classify store/exchange metadata, and physical
//! children are visited in order. Session process state, concrete physical
//! plans, and MPP execution remain external boundaries.

/// Storage engine identity used by a source TableReader.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum StoreType {
    /// TiKV or another non-TiFlash store.
    Other,
    /// TiFlash store.
    TiFlash,
}

/// The source table-reader child plan identity relevant to telemetry.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TablePlanKind {
    /// A regular table plan.
    Other,
    /// An ExchangeSender child.
    ExchangeSender,
}

/// Dependency-closed physical/non-physical plan shape for telemetry.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum PlanNode {
    /// An Explain wrapper with an optional target plan.
    Explain(Option<Box<Self>>),
    /// A physical TableReader and the child plan type inspected by source.
    TableReader {
        /// Store selected by the TableReader.
        store: StoreType,
        /// Child plan classification used for ExchangeSender detection.
        table_plan: TablePlanKind,
    },
    /// A generic physical node with ordered children.
    Physical(Vec<Self>),
    /// A logical or otherwise non-physical plan.
    NonPhysical,
}

/// Classifies TiFlash pushdown and TiFlash exchange containment.
///
/// The first result corresponds to `tiFlashPushDown`; the second corresponds
/// to `tiFlashExchangePushDown`.
#[must_use]
pub fn is_tiflash_contained(plan: Option<&PlanNode>) -> (bool, bool) {
    let Some(plan) = plan else {
        return (false, false);
    };

    fn visit(plan: &PlanNode, result: &mut (bool, bool)) {
        let current = match plan {
            PlanNode::Explain(Some(target)) => target.as_ref(),
            PlanNode::Explain(None) => return,
            other => other,
        };

        match current {
            PlanNode::TableReader { store, table_plan } => {
                result.0 = *store == StoreType::TiFlash;
                if result.0 && *table_plan == TablePlanKind::ExchangeSender {
                    result.1 = true;
                }
            }
            PlanNode::Physical(children) => {
                for child in children {
                    visit(child, result);
                    if result.0 {
                        return;
                    }
                }
            }
            PlanNode::Explain(_) | PlanNode::NonPhysical => {}
        }
    }

    let mut result = (false, false);
    visit(plan, &mut result);
    result
}

#[cfg(test)]
mod tests {
    use super::{is_tiflash_contained, PlanNode, StoreType, TablePlanKind};

    #[test]
    fn nil_nonphysical_and_empty_explain_are_not_tiflash() {
        assert_eq!(is_tiflash_contained(None), (false, false));
        assert_eq!(
            is_tiflash_contained(Some(&PlanNode::NonPhysical)),
            (false, false)
        );
        let explain = PlanNode::Explain(None);
        assert_eq!(is_tiflash_contained(Some(&explain)), (false, false));
    }

    #[test]
    fn table_reader_classifies_store_and_exchange() {
        let tikv = PlanNode::TableReader {
            store: StoreType::Other,
            table_plan: TablePlanKind::ExchangeSender,
        };
        assert_eq!(is_tiflash_contained(Some(&tikv)), (false, false));

        let tiflash = PlanNode::TableReader {
            store: StoreType::TiFlash,
            table_plan: TablePlanKind::Other,
        };
        assert_eq!(is_tiflash_contained(Some(&tiflash)), (true, false));

        let tiflash_exchange = PlanNode::TableReader {
            store: StoreType::TiFlash,
            table_plan: TablePlanKind::ExchangeSender,
        };
        assert_eq!(is_tiflash_contained(Some(&tiflash_exchange)), (true, true));
    }

    #[test]
    fn explain_unwraps_target_before_classification() {
        let target = PlanNode::TableReader {
            store: StoreType::TiFlash,
            table_plan: TablePlanKind::ExchangeSender,
        };
        let explain = PlanNode::Explain(Some(Box::new(target)));
        assert_eq!(is_tiflash_contained(Some(&explain)), (true, true));
    }

    #[test]
    fn physical_children_are_visited_in_order() {
        let tree = PlanNode::Physical(vec![
            PlanNode::TableReader {
                store: StoreType::Other,
                table_plan: TablePlanKind::Other,
            },
            PlanNode::Physical(vec![PlanNode::TableReader {
                store: StoreType::TiFlash,
                table_plan: TablePlanKind::ExchangeSender,
            }]),
        ]);
        assert_eq!(is_tiflash_contained(Some(&tree)), (true, true));
    }
}
