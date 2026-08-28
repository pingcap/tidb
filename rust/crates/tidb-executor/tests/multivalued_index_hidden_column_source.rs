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

//! Port of Go `pkg/ddl/tests/multivaluedindex/multi_valued_index_test.go`
//! (item 800 of the pkg/ddl `Test*` enumeration), plus the package's
//! `main_test.go::TestMain`.

/// Go `TestCreateMultiValuedIndexHasBinaryCollation`
/// (`pkg/ddl/tests/multivaluedindex/multi_valued_index_test.go:27`):
/// `create table test.t (pk varchar(4) primary key clustered, j json, str
/// varchar(255), value int, key idx((cast(j as char(100) array)), str))`
/// must succeed, and the table must carry a HIDDEN column (the expression
/// index's `_V$` rewrite, Go `pkg/ddl/executor.go` expressionIndexPrefix)
/// whose FieldType answers `IsArray()` == true with charset `binary` and
/// collation `binary` -- the multi-valued ARRAY part's stored type.
///
/// Documented divergence: this tier declines the whole shape at
/// expression-index check time with 1105 (the CAST-AS-ARRAY arm is written
/// to Go's rule in `src/expression_index.rs:158-163` and unreachable until
/// multi-valued indexes land), so the hidden column -- and its binary
/// FieldType -- never comes into existence. Nothing is approximated.
// go-parity-gap: multi-valued index creation is refused (1105) by this
// tier; no hidden-array-column carrier exists to inspect.
#[test]
#[ignore]
fn create_multi_valued_index_has_binary_collation() {
}
