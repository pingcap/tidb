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

//! `SchemaState` from `pkg/meta/model/job.go`: the lifecycle state of a
//! schema element (used pervasively across DDL and metadata). Kept in its
//! own module here; in Go it lives inside `job.go`.

/// Go `SchemaState` (a `byte`): the state of a schema element during an
/// online DDL. Modelled as a newtype over `u8` so any stored value
/// round-trips and [`Display`](std::fmt::Display) yields `"none"` for the
/// zero/unknown value, matching Go's `switch` default.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SchemaState(pub u8);

impl SchemaState {
    /// The element is absent and can't be used (Go `StateNone`, zero value).
    pub const NONE: SchemaState = SchemaState(0);
    /// Only delete operations are allowed (Go `StateDeleteOnly`).
    pub const DELETE_ONLY: SchemaState = SchemaState(1);
    /// Any write is allowed, but readers can't see the change yet
    /// (Go `StateWriteOnly`).
    pub const WRITE_ONLY: SchemaState = SchemaState(2);
    /// Re-organizing all data after the write-only state
    /// (Go `StateWriteReorganization`).
    pub const WRITE_REORGANIZATION: SchemaState = SchemaState(3);
    /// Re-organizing all data after the delete-only state
    /// (Go `StateDeleteReorganization`).
    pub const DELETE_REORGANIZATION: SchemaState = SchemaState(4);
    /// The element is usable for all reads and writes (Go `StatePublic`).
    pub const PUBLIC: SchemaState = SchemaState(5);
    /// Waiting for the TiFlash replica to finish (Go `StateReplicaOnly`).
    pub const REPLICA_ONLY: SchemaState = SchemaState(6);
    /// Only global transactions may operate on the element
    /// (Go `StateGlobalTxnOnly`).
    pub const GLOBAL_TXN_ONLY: SchemaState = SchemaState(7);
}

// Go's `SchemaState` is a `byte` with no `MarshalJSON`, so `encoding/json`
// emits it as a bare JSON number; these impls do the same.
impl serde::Serialize for SchemaState {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u8(self.0)
    }
}

impl<'de> serde::Deserialize<'de> for SchemaState {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(SchemaState(<u8 as serde::Deserialize>::deserialize(
            deserializer,
        )?))
    }
}

impl std::fmt::Display for SchemaState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match *self {
            SchemaState::DELETE_ONLY => "delete only",
            SchemaState::WRITE_ONLY => "write only",
            SchemaState::WRITE_REORGANIZATION => "write reorganization",
            SchemaState::DELETE_REORGANIZATION => "delete reorganization",
            SchemaState::PUBLIC => "public",
            SchemaState::REPLICA_ONLY => "replica only",
            SchemaState::GLOBAL_TXN_ONLY => "global txn only",
            // StateNone and any unknown value.
            _ => "none",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Go TestSchemaState: every non-none state has a non-empty string.
    #[test]
    fn state_strings_non_empty() {
        for state in [
            SchemaState::DELETE_ONLY,
            SchemaState::WRITE_ONLY,
            SchemaState::WRITE_REORGANIZATION,
            SchemaState::DELETE_REORGANIZATION,
            SchemaState::PUBLIC,
            SchemaState::GLOBAL_TXN_ONLY,
        ] {
            assert!(!state.to_string().is_empty());
        }
    }

    // The exact strings and the none/unknown default, matching Go's switch.
    #[test]
    fn state_strings_exact() {
        assert_eq!(SchemaState::NONE.to_string(), "none");
        assert_eq!(SchemaState::DELETE_ONLY.to_string(), "delete only");
        assert_eq!(SchemaState::WRITE_ONLY.to_string(), "write only");
        assert_eq!(
            SchemaState::WRITE_REORGANIZATION.to_string(),
            "write reorganization"
        );
        assert_eq!(
            SchemaState::DELETE_REORGANIZATION.to_string(),
            "delete reorganization"
        );
        assert_eq!(SchemaState::PUBLIC.to_string(), "public");
        assert_eq!(SchemaState::REPLICA_ONLY.to_string(), "replica only");
        assert_eq!(SchemaState::GLOBAL_TXN_ONLY.to_string(), "global txn only");
        assert_eq!(SchemaState(200).to_string(), "none");
        assert_eq!(SchemaState::default(), SchemaState::NONE);
    }

    // Go marshals a `byte`-typed named state as a bare JSON number.
    #[test]
    fn json_is_a_number() {
        assert_eq!(
            serde_json::to_string(&SchemaState::PUBLIC).unwrap(),
            "5",
            "SchemaState must marshal as a number, like Go"
        );
        assert_eq!(
            serde_json::from_str::<SchemaState>("3").unwrap(),
            SchemaState::WRITE_REORGANIZATION
        );
        assert_eq!(
            serde_json::from_str::<SchemaState>("0").unwrap(),
            SchemaState::NONE
        );
    }
}
