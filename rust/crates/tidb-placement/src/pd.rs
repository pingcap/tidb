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

//! boundary: local redeclaration of the `github.com/tikv/pd/client/http` JSON
//! data-transfer types that Go `pkg/ddl/placement` builds and marshals.
//!
//! Go imports the PD HTTP client purely for its `pd.Rule`,
//! `pd.LabelConstraint`, `pd.PeerRoleType`, and `pd.LabelConstraintOp` value
//! types; no call in this package speaks to PD. The types are redeclared here
//! with byte-identical JSON field names, ordering, and `omitempty` behavior so
//! the rendered bundle stays wire-compatible, and no network client is pulled
//! into the transcreation.
//!
//! Go `pd.GroupBundle` is not redeclared here: Go's own
//! `type Bundle pd.GroupBundle` is transcreated directly as [`crate::Bundle`],
//! which carries the same four JSON fields.
//!
//! Only the `pd.Rule` fields this package reads or writes are declared. The
//! omitted fields (`override`, `isolation_level`, `create_timestamp`, and the
//! `json:"-"` raw key byte slices) all carry `omitempty` or are unmarshaled
//! only, so the JSON this package emits is unchanged by their absence.

use std::borrow::Cow;

use serde::{Serialize, Serializer};

/// Go `pd.PeerRoleType`: the Raft role a placement rule selects peers for.
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PeerRoleType(pub Cow<'static, str>);

impl PeerRoleType {
    /// Go `pd.Voter`.
    pub const VOTER: Self = Self(Cow::Borrowed("voter"));
    /// Go `pd.Leader`.
    pub const LEADER: Self = Self(Cow::Borrowed("leader"));
    /// Go `pd.Follower`.
    pub const FOLLOWER: Self = Self(Cow::Borrowed("follower"));
    /// Go `pd.Learner`.
    pub const LEARNER: Self = Self(Cow::Borrowed("learner"));

    /// The underlying Go `string` conversion.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&'static str> for PeerRoleType {
    fn from(value: &'static str) -> Self {
        Self(Cow::Borrowed(value))
    }
}

impl Serialize for PeerRoleType {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

/// Go `pd.LabelConstraintOp`: how a label constraint matches store labels.
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LabelConstraintOp(pub Cow<'static, str>);

impl LabelConstraintOp {
    /// Go `pd.In`.
    pub const IN: Self = Self(Cow::Borrowed("in"));
    /// Go `pd.NotIn`.
    pub const NOT_IN: Self = Self(Cow::Borrowed("notIn"));
    /// Go `pd.Exists`.
    pub const EXISTS: Self = Self(Cow::Borrowed("exists"));
    /// Go `pd.NotExists`.
    pub const NOT_EXISTS: Self = Self(Cow::Borrowed("notExists"));

    /// The underlying Go `string` conversion.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&'static str> for LabelConstraintOp {
    fn from(value: &'static str) -> Self {
        Self(Cow::Borrowed(value))
    }
}

impl Serialize for LabelConstraintOp {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

/// Go `pd.LabelConstraint`: one `{+|-}key=value` store-label predicate.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct LabelConstraint {
    /// The store label key.
    #[serde(rename = "key", skip_serializing_if = "String::is_empty")]
    pub key: String,
    /// The match operator.
    #[serde(rename = "op", skip_serializing_if = "op_is_empty")]
    pub op: LabelConstraintOp,
    /// The accepted (or rejected) label values.
    #[serde(rename = "values", skip_serializing_if = "Vec::is_empty")]
    pub values: Vec<String>,
}

fn op_is_empty(op: &LabelConstraintOp) -> bool {
    op.0.is_empty()
}

/// Go `pd.Rule`: one placement rule inside a rule group.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Rule {
    /// The owning rule group's ID.
    #[serde(rename = "group_id")]
    pub group_id: String,
    /// The rule ID, unique within the group.
    #[serde(rename = "id")]
    pub id: String,
    /// The rule's priority index within the group.
    #[serde(rename = "index", skip_serializing_if = "is_zero_index")]
    pub index: i64,
    /// The hex-encoded inclusive range start.
    #[serde(rename = "start_key")]
    pub start_key_hex: String,
    /// The hex-encoded exclusive range end.
    #[serde(rename = "end_key")]
    pub end_key_hex: String,
    /// The Raft role the selected peers take.
    #[serde(rename = "role")]
    pub role: PeerRoleType,
    /// Whether the peers are witnesses.
    #[serde(rename = "is_witness")]
    pub is_witness: bool,
    /// How many peers the rule requires.
    #[serde(rename = "count")]
    pub count: i64,
    /// The store-label predicates the peers must satisfy.
    #[serde(rename = "label_constraints", skip_serializing_if = "Vec::is_empty")]
    pub label_constraints: Vec<LabelConstraint>,
    /// The isolation labels PD spreads the peers across.
    #[serde(rename = "location_labels", skip_serializing_if = "Vec::is_empty")]
    pub location_labels: Vec<String>,
}

// serde's `skip_serializing_if` predicate signature takes a reference.
fn is_zero_index(index: &i64) -> bool {
    *index == 0
}

impl Rule {
    /// Go `(*pd.Rule).Clone`.
    ///
    /// Go's copy is shallow: the clone shares the label-constraint and
    /// location-label backing arrays with its source. Neither this package nor
    /// its callers mutate those elements in place — every mutation replaces the
    /// whole slice — so the deep Rust clone is observationally identical.
    #[must_use]
    pub fn clone_rule(&self) -> Self {
        self.clone()
    }
}
