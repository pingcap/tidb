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

//! Go `pkg/util/schemacmp` lands as a complete package: schema comparison
//! and joining over a join-semilattice model, used to reconcile upstream
//! table schemas in shard-merge replication.
//!
//! File mapping (one Rust module per Go file):
//! - `lattice.rs` <- `lattice.go`
//! - `table.rs` <- `table.go`
//! - `typ.rs` <- `type.go`
//! - `charset_collation.rs` <- `charset_collation.go`
//! - `util.rs` <- `util.go`
//!
//! Go's `Lattice` interface becomes the [`Lattice`] trait; Go's dynamic
//! `interface{}` payloads become the explicit [`Value`] domain. Exported Go
//! constructors of unexported struct types (`Singleton`, `FieldTp`, `Maybe`,
//! `Map`, ...) become free functions; Go `Type` is [`Typ::new`], and Go `Map`
//! is [`map_lattice`]. Error messages are byte-exact renderings of the Go
//! `ErrMsg*` templates.

mod charset_collation;
mod lattice;
mod table;
mod typ;
mod util;

pub use charset_collation::{charset, collation, CharsetLattice, CollationLattice};
pub use lattice::{
    combine_compare_result, equality_singleton, field_tp, map_lattice, maybe,
    maybe_singleton_interface, maybe_singleton_string, singleton, BitSet, Bool, Byte, Equality,
    IncompatibleError, Int, Int64, Lattice, LatticeMap, StringList, Tuple, Uint, Value,
    ERR_MSG_AT_MAP_KEY, ERR_MSG_AT_TUPLE_INDEX, ERR_MSG_CONTRADICTING_ORDERS,
    ERR_MSG_DISTINCT_SINGLETONS, ERR_MSG_INCOMPATIBLE_CHARSET, ERR_MSG_INCOMPATIBLE_COLLATION,
    ERR_MSG_INCOMPATIBLE_TYPE, ERR_MSG_NON_INCLUSIVE_BIT_SETS, ERR_MSG_STRING_LIST_ELEM_MISMATCH,
    ERR_MSG_TUPLE_LENGTH_MISMATCH, ERR_MSG_TYPE_MISMATCH,
};
pub use table::{decode_column_field_types, encode, Table};
pub use typ::{Typ, ERR_MSG_AUTO_TYPE_WITHOUT_KEY};
