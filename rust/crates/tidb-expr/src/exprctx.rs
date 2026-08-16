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

//! SEED of Go `pkg/expression/exprctx`, covering `optional.go` in full plus
//! `context.go`'s plan-column-ID allocator.
//!
//! This is a seed, not a completed package. `context.go`'s `EvalContext` and
//! `BuildContext` umbrella interfaces and the `CtxWithHandleTruncateErrLevel`
//! override built on them are NOT here: `EvalContext` requires
//! `variable.UserVarsReader` from `pkg/sessionctx/variable`, which this
//! workspace models only partially, so modeling the interfaces now would pin
//! a shape the session port has not settled. What is here is closed and
//! carries its own upstream tests.
//!
//! The optional-property machinery is how an `EvalContext` advertises which
//! optional providers it carries: each property has a key, the keys index a
//! descriptor table, and a `u64` bitset says which are present. The providers
//! those keys stand for — Go's `pkg/expression/expropt` — live in
//! [`crate::expropt`], which is complete; the only piece of `EvalContext` it
//! needs, `GetOptionalPropProvider`, it declares there as a boundary trait
//! until the umbrella interfaces above land here.

use std::sync::atomic::{AtomicI64, Ordering};

/// Go `OptionalEvalPropKey`: the key of one optional evaluation property.
///
/// Go declares these as an `iota` run and relies on the key doubling as an
/// index into [`optional_property_desc_list`]; the discriminants here are
/// that same run, so the index relationship holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(i32)]
pub enum OptionalEvalPropKey {
    /// Go `OptPropCurrentUser`.
    CurrentUser = 0,
    /// Go `OptPropSessionVars`.
    SessionVars = 1,
    /// Go `OptPropInfoSchema`.
    InfoSchema = 2,
    /// Go `OptPropKVStore`.
    KvStore = 3,
    /// Go `OptPropSQLExecutor`.
    SqlExecutor = 4,
    /// Go `OptPropSequenceOperator`.
    SequenceOperator = 5,
    /// Go `OptPropAdvisoryLock`.
    AdvisoryLock = 6,
    /// Go `OptPropDDLOwnerInfo`.
    DdlOwnerInfo = 7,
    /// Go `OptPropPrivilegeChecker`.
    PrivilegeChecker = 8,
}

/// Go `OptPropsCnt`: the number of optional properties.
pub const OPT_PROPS_CNT: usize = 9;

/// Go's private `allOptPropsMask`.
const ALL_OPT_PROPS_MASK: u64 = (1 << OPT_PROPS_CNT) - 1;

/// Go `OptionalEvalPropDesc`: the description of one optional property.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OptionalEvalPropDesc {
    key: OptionalEvalPropKey,
    str: &'static str,
}

impl OptionalEvalPropDesc {
    /// Go `OptionalEvalPropDesc.Key`.
    #[must_use]
    pub const fn key(&self) -> OptionalEvalPropKey {
        self.key
    }

    /// The description's display string.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        self.str
    }
}

/// Go's `optionalPropertyDescList`, indexed by key.
///
/// Note `SequenceOperator`'s string: the source spells it `OptPropDDLOwnerInfo`,
/// the same string `DdlOwnerInfo` carries. That is a source typo, and it is
/// reproduced because the string is what `OptionalEvalPropKey.String` returns.
static OPTIONAL_PROPERTY_DESC_LIST: [OptionalEvalPropDesc; OPT_PROPS_CNT] = [
    OptionalEvalPropDesc {
        key: OptionalEvalPropKey::CurrentUser,
        str: "OptPropCurrentUser",
    },
    OptionalEvalPropDesc {
        key: OptionalEvalPropKey::SessionVars,
        str: "OptPropSessionVars",
    },
    OptionalEvalPropDesc {
        key: OptionalEvalPropKey::InfoSchema,
        str: "OptPropInfoSchema",
    },
    OptionalEvalPropDesc {
        key: OptionalEvalPropKey::KvStore,
        str: "OptPropKVStore",
    },
    OptionalEvalPropDesc {
        key: OptionalEvalPropKey::SqlExecutor,
        str: "OptPropSQLExecutor",
    },
    OptionalEvalPropDesc {
        key: OptionalEvalPropKey::SequenceOperator,
        str: "OptPropDDLOwnerInfo",
    },
    OptionalEvalPropDesc {
        key: OptionalEvalPropKey::AdvisoryLock,
        str: "OptPropAdvisoryLock",
    },
    OptionalEvalPropDesc {
        key: OptionalEvalPropKey::DdlOwnerInfo,
        str: "OptPropDDLOwnerInfo",
    },
    OptionalEvalPropDesc {
        key: OptionalEvalPropKey::PrivilegeChecker,
        str: "OptPropPrivilegeChecker",
    },
];

/// Go's `optionalPropertyDescList` as a slice.
#[must_use]
pub fn optional_property_desc_list() -> &'static [OptionalEvalPropDesc; OPT_PROPS_CNT] {
    &OPTIONAL_PROPERTY_DESC_LIST
}

impl OptionalEvalPropKey {
    /// Every key, in source order.
    pub const ALL: [Self; OPT_PROPS_CNT] = [
        Self::CurrentUser,
        Self::SessionVars,
        Self::InfoSchema,
        Self::KvStore,
        Self::SqlExecutor,
        Self::SequenceOperator,
        Self::AdvisoryLock,
        Self::DdlOwnerInfo,
        Self::PrivilegeChecker,
    ];

    /// The key's index, Go's `int(key)`.
    #[must_use]
    pub const fn index(self) -> usize {
        self as usize
    }

    /// Go `OptionalEvalPropKey.AsPropKeySet`: the singleton set.
    #[must_use]
    pub const fn as_prop_key_set(self) -> OptionalEvalPropKeySet {
        OptionalEvalPropKeySet(1 << self.index())
    }

    /// Go `OptionalEvalPropKey.Desc`.
    #[must_use]
    pub fn desc(self) -> &'static OptionalEvalPropDesc {
        &OPTIONAL_PROPERTY_DESC_LIST[self.index()]
    }
}

impl std::fmt::Display for OptionalEvalPropKey {
    /// Go `OptionalEvalPropKey.String`. Go also has an
    /// `UnknownOptionalEvalPropKey(%d)` arm for an out-of-range key; a Rust
    /// enum has no out-of-range value, so that arm is unreachable here.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.desc().as_str())
    }
}

/// Go `OptionalEvalPropProvider`: something that supplies one optional
/// property, identified by its description.
pub trait OptionalEvalPropProvider {
    /// Go `OptionalEvalPropProvider.Desc`.
    fn desc(&self) -> &'static OptionalEvalPropDesc;
}

/// Go `OptionalEvalPropKeySet`: a bitmap of which optional properties a
/// context provides.
///
/// Bits above [`OPT_PROPS_CNT`] are ignored by every query, which is what
/// Go's `allOptPropsMask` achieves and what `TestOptionalPropKeySetWithUnusedBits`
/// pins.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OptionalEvalPropKeySet(pub u64);

impl OptionalEvalPropKeySet {
    /// Go `OptionalEvalPropKeySet.Add`.
    #[must_use]
    pub const fn add(self, key: OptionalEvalPropKey) -> Self {
        Self(self.0 | key.as_prop_key_set().0)
    }

    /// Go `OptionalEvalPropKeySet.Remove`.
    #[must_use]
    pub const fn remove(self, key: OptionalEvalPropKey) -> Self {
        Self(self.0 & !key.as_prop_key_set().0)
    }

    /// Go `OptionalEvalPropKeySet.Contains`.
    #[must_use]
    pub const fn contains(self, key: OptionalEvalPropKey) -> bool {
        self.0 & key.as_prop_key_set().0 != 0
    }

    /// Go `OptionalEvalPropKeySet.IsEmpty`: no *known* property is set, so
    /// bits outside the mask do not count.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.0 & ALL_OPT_PROPS_MASK == 0
    }

    /// Go `OptionalEvalPropKeySet.IsFull`: every known property is set.
    #[must_use]
    pub const fn is_full(self) -> bool {
        self.0 & ALL_OPT_PROPS_MASK == ALL_OPT_PROPS_MASK
    }
}

/// Go `PlanColumnIDAllocator`.
pub trait PlanColumnIdAllocator {
    /// Go `AllocPlanColumnID`.
    fn alloc_plan_column_id(&self) -> i64;
    /// Go `GetLastPlanColumnID`.
    fn last_plan_column_id(&self) -> i64;
}

/// Go `SimplePlanColumnIDAllocator`: an atomic counter handing out plan
/// column IDs. Go pre-increments, so the first ID is `offset + 1`.
#[derive(Debug)]
pub struct SimplePlanColumnIdAllocator {
    id: AtomicI64,
}

impl SimplePlanColumnIdAllocator {
    /// Go `NewSimplePlanColumnIDAllocator`.
    #[must_use]
    pub const fn new(offset: i64) -> Self {
        Self {
            id: AtomicI64::new(offset),
        }
    }
}

impl PlanColumnIdAllocator for SimplePlanColumnIdAllocator {
    fn alloc_plan_column_id(&self) -> i64 {
        self.id.fetch_add(1, Ordering::SeqCst) + 1
    }

    fn last_plan_column_id(&self) -> i64 {
        self.id.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Go `TestOptionalPropKeySet`.
    #[test]
    fn optional_prop_key_set() {
        let key_set = OptionalEvalPropKeySet::default();
        assert!(key_set.is_empty());
        assert!(!key_set.is_full());
        assert!(!key_set.contains(OptionalEvalPropKey::CurrentUser));

        // Add one key.
        let key_set2 = key_set.add(OptionalEvalPropKey::CurrentUser);
        assert!(key_set2.contains(OptionalEvalPropKey::CurrentUser));
        assert!(!key_set2.is_empty());
        assert!(!key_set2.is_full());

        // The old set is a value and is unaffected.
        assert!(key_set.is_empty());

        // Add a second key.
        let key_set3 = key_set2.add(OptionalEvalPropKey::DdlOwnerInfo);
        assert!(key_set3.contains(OptionalEvalPropKey::CurrentUser));
        assert!(key_set3.contains(OptionalEvalPropKey::DdlOwnerInfo));
        assert!(!key_set3.is_empty());
        assert!(!key_set3.is_full());
        assert!(!key_set2.contains(OptionalEvalPropKey::DdlOwnerInfo));

        // Remove one key.
        let key_set4 = key_set3.remove(OptionalEvalPropKey::CurrentUser);
        assert!(!key_set4.contains(OptionalEvalPropKey::CurrentUser));
        assert!(key_set4.contains(OptionalEvalPropKey::DdlOwnerInfo));
        assert!(!key_set4.is_full());
        assert!(!key_set4.is_empty());

        // Add all the other keys.
        let key_set4 = key_set3
            .add(OptionalEvalPropKey::SessionVars)
            .add(OptionalEvalPropKey::InfoSchema)
            .add(OptionalEvalPropKey::KvStore)
            .add(OptionalEvalPropKey::SqlExecutor)
            .add(OptionalEvalPropKey::SequenceOperator)
            .add(OptionalEvalPropKey::AdvisoryLock)
            .add(OptionalEvalPropKey::PrivilegeChecker);
        assert!(key_set4.is_full());
        assert!(!key_set4.is_empty());
    }

    // Go `TestOptionalPropKeySetWithUnusedBits`: bits above the known
    // properties never make a set non-empty or full.
    #[test]
    fn optional_prop_key_set_with_unused_bits() {
        const { assert!(OPT_PROPS_CNT < 64) };
        let full = OptionalEvalPropKeySet(u64::MAX);

        let mut bits = OptionalEvalPropKeySet(full.0 << OPT_PROPS_CNT);
        assert!(bits.is_empty());
        assert!(!bits.contains(OptionalEvalPropKey::CurrentUser));
        assert!(!bits.contains(OptionalEvalPropKey::DdlOwnerInfo));
        bits = bits.add(OptionalEvalPropKey::CurrentUser);
        assert!(bits.contains(OptionalEvalPropKey::CurrentUser));

        let mut bits = OptionalEvalPropKeySet(full.0 >> (64 - OPT_PROPS_CNT));
        assert!(bits.is_full());
        assert!(bits.contains(OptionalEvalPropKey::CurrentUser));
        assert!(bits.contains(OptionalEvalPropKey::DdlOwnerInfo));
        bits = bits.remove(OptionalEvalPropKey::CurrentUser);
        assert!(!bits.contains(OptionalEvalPropKey::CurrentUser));
    }

    // Go `TestOptionalPropKey`: a key's singleton set contains it and nothing
    // else, and the descriptor table is indexed by the key.
    #[test]
    fn optional_prop_key() {
        for key in OptionalEvalPropKey::ALL {
            let key_set = key.as_prop_key_set();
            assert!(key_set.contains(key));
            assert_eq!(optional_property_desc_list()[key.index()].key(), key);
            assert_eq!(key.desc(), &optional_property_desc_list()[key.index()]);

            for other in OptionalEvalPropKey::ALL {
                if other != key {
                    assert!(!key_set.contains(other));
                }
            }

            assert!(key_set.remove(key).is_empty());
        }
    }

    // Go's `init` checks the descriptor list length and index alignment, and
    // that the property count fits the bitset. Rust checks the length at
    // compile time; the rest is asserted here.
    #[test]
    fn descriptor_list_is_aligned_with_its_keys() {
        assert_eq!(OptionalEvalPropKey::ALL.len(), OPT_PROPS_CNT);
        for (index, desc) in optional_property_desc_list().iter().enumerate() {
            assert_eq!(desc.key().index(), index);
        }
        assert_eq!(ALL_OPT_PROPS_MASK, (1 << OPT_PROPS_CNT) - 1);
        // The source's own typo, kept because it is the rendered string.
        assert_eq!(
            OptionalEvalPropKey::SequenceOperator.to_string(),
            "OptPropDDLOwnerInfo"
        );
        assert_eq!(
            OptionalEvalPropKey::CurrentUser.to_string(),
            "OptPropCurrentUser"
        );
    }

    // Go `SimplePlanColumnIDAllocator` pre-increments: the first allocation
    // from offset n is n+1.
    #[test]
    fn plan_column_ids_are_allocated_after_the_offset() {
        let allocator = SimplePlanColumnIdAllocator::new(10);
        assert_eq!(allocator.last_plan_column_id(), 10);
        assert_eq!(allocator.alloc_plan_column_id(), 11);
        assert_eq!(allocator.alloc_plan_column_id(), 12);
        assert_eq!(allocator.last_plan_column_id(), 12);
    }
}
