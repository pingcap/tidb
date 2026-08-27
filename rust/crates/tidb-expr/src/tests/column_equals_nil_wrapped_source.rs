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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Remaining `pkg/expression/column_test.go` surface on `origin/master` whose
//! main tables are carried by `column.rs`'s own test module: the nil-interface
//! arms of `TestColumnEqualsWithNilWrappedInAny` (:484). The same/different
//! UniqueID halves of that Go test are already pinned by
//! `column.rs::column_hash_equals_matches_source`.

/// `pkg/expression/column_test.go:484 TestColumnEqualsWithNilWrappedInAny`
/// pins Go's `Equals(any)` INTERFACE contract: `col.Equals(nil)` is false, a
/// nil `*Column` wrapped in `any` compares false against a real column, two
/// nils compare TRUE, and both value arms delegate to field comparison. Rust's
/// `Column::equals(&Column)` takes a typed reference, so no caller can hand it
/// a nil at all -- the interface-shaped rows have no Rust-level subject.
#[test]
#[ignore = "go-parity-gap: Go Equals(any)'s nil-receiver/nil-wrapped arms are type-system surface with no equivalent call shape next to Column::equals(&Column)"]
fn test_column_equals_with_nil_wrapped_in_any() {}
