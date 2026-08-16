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

//! Go `pkg/ddl/placement` lands as a complete package: the placement-rule
//! bundles TiDB derives from `PLACEMENT POLICY` options and hands to PD.
//!
//! File mapping (one Rust module per Go file):
//! - `bundle.rs` <- `bundle.go`
//! - `rule.rs` <- `rule.go`
//! - `constraints.rs` <- `constraints.go`
//! - `constraint.rs` <- `constraint.go`
//! - `common.rs` <- `common.go`
//! - `errors.rs` <- `errors.go`
//!
//! Narrowings, each named at its own definition site:
//! - [`pd`] redeclares the `github.com/tikv/pd/client/http` value types
//!   (`pd.Rule`, `pd.LabelConstraint`, `pd.PeerRoleType`,
//!   `pd.LabelConstraintOp`) as local JSON DTOs with byte-identical field
//!   names and `omitempty` behavior. Go imports that client purely for these
//!   types; no call in this package speaks to PD, so no client comes across.
//!   Go's `pd.GroupBundle` is not redeclared separately: Go's own
//!   `type Bundle pd.GroupBundle` is transcreated directly as [`Bundle`].
//! - The private `yaml_lite` module replaces `gopkg.in/yaml.v2`'s
//!   `UnmarshalStrict` for the only two decode targets this package uses,
//!   `[]string` and `map[string]int`. No YAML crate is reachable from this
//!   offline workspace, so the go-yaml behaviors the package's semantics
//!   depend on — flow plain scalars that swallow a `:` not followed by a
//!   blank, first-document-only decoding, empty input as a no-op, and strict
//!   duplicate-key rejection — are implemented directly there.
//! - Go's `errors.New` sentinels plus `errors.Is` become
//!   [`PlacementErrorKind`] carried inside [`PlacementError`]; wrapping keeps
//!   the identity and only appends the Go message text.
//! - Go's `failpoint.Inject("MockMarshalFailure")` in `(*Bundle).String`
//!   becomes a test-only thread-local in `bundle.rs`: serializing [`Bundle`]
//!   cannot fail, so there is nothing for a failpoint registry to flip.
//!
//! `pd.Rule`'s Go `Clone` is a shallow struct copy that shares the constraint
//! slices; the Rust clone is deep. No caller mutates a shared element in place
//! — every mutation replaces the whole slice — so the two are observationally
//! identical.
//!
//! Go's `newRulesWithDictConstraints` iterates a `map[string]int`, so the rules
//! it emits arrive in a random order; the Rust port keeps the source order of
//! the dict. `Tidy` sorts by rule ID, and Go's own tests compare rule sets
//! order-insensitively, so nothing observable depends on the difference.

mod bundle;
mod common;
mod constraint;
mod constraints;
mod errors;
pub mod pd;
mod rule;
mod yaml_lite;

pub use bundle::{
    get_range_start_and_end_key_hex, new_bundle, new_bundle_from_constraints_options,
    new_bundle_from_options, new_bundle_from_sugar_options, new_full_table_bundles,
    new_partition_bundle, new_partition_list_bundles, new_table_bundle, Bundle, PolicyGetter,
};
pub use common::{
    group_id, BUNDLE_ID_PREFIX, DC_LABEL_KEY, DEFAULT_KWD, ENGINE_LABEL_KEY, ENGINE_LABEL_TIFLASH,
    ENGINE_LABEL_TIFLASH_COMPUTE, ENGINE_LABEL_TIKV, ENGINE_ROLE_LABEL_KEY,
    ENGINE_ROLE_LABEL_WRITE, KEY_RANGE_GLOBAL, KEY_RANGE_META, PD_BUNDLE_ID,
    RULE_INDEX_KEY_RANGE_FOR_GLOBAL, RULE_INDEX_KEY_RANGE_FOR_META, RULE_INDEX_PARTITION,
    RULE_INDEX_TABLE, RULE_INDEX_TIFLASH, TIDB_BUNDLE_RANGE_PREFIX_FOR_GLOBAL,
    TIDB_BUNDLE_RANGE_PREFIX_FOR_META, TIFLASH_RULE_GROUP_ID,
};
pub use constraint::{
    constraint_compatible_with, new_constraint, new_constraint_direct, restore_constraint,
    ConstraintCompatibility,
};
pub use constraints::{
    add_constraint, constraints_finger_print, new_constraints, new_constraints_direct,
    new_constraints_from_yaml, restore_constraints,
};
pub use errors::{PlacementError, PlacementErrorKind};
pub use rule::{new_rule, RuleBuilder};
