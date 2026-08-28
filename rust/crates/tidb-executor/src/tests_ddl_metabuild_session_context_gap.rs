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

//! Port of `pkg/ddl/metabuild_test.go::TestNewMetaBuildContextWithSctx`
//! (read from the origin/master snapshot).
//!
//! The unit under test is `NewMetaBuildContextWithSctx`
//! (pkg/ddl/metabuild.go:24): the ADAPTER that copies a session context's
//! facts into a `metabuild.Context` — expr ctx identity, SQL mode,
//! `DefaultCollationForUTF8MB4`, `EnableAutoIncrementInGenerated`,
//! `PrimaryKeyRequired` forced FALSE whenever `InRestrictedSQL` is true,
//! `EnableClusteredIndex`, `ShardRowIDBits`, `PreSplitRegions`, the latest
//! infoschema — plus the option-override arms (`WithSuppressTooLongIndexErr`)
//! and the warning/note accumulation visible through the expression
//! context. The adapter itself (and this tier's session-variable axis it
//! reads: `PrimaryKeyRequired`, `InRestrictedSQL`, `ShardRowIDBits`,
//! `PreSplitRegions`, `EnableClusteredIndex`) is not transcreated: the
//! executor's `StmtContext` carries only the parser-facing `sql_mode`
/// flags. The underlying `metabuild.Context` getter/option mapping the
/// adapter feeds IS transcreated and tested in
/// `tidb-expr/src/metabuild.rs` (its `mod tests` ports Go
/// `pkg/meta/metabuild/context_test.go::TestMetaBuildContext`), so the
/// getter half of Go's table is already pinned there.

/// Go `metabuild_test.go:30::TestNewMetaBuildContextWithSctx`: the
/// session-to-metabuild field flow, per-field subtests `exprCtx`,
/// `enableAutoIncrementInGenerated`, `primaryKeyRequired` (including the
/// `InRestrictedSQL` force-false arm), `clusteredIndexDefMode`,
/// `shardRowIDBits`, `preSplitRegions`, `suppressTooLongIndexErr`, and
/// `is`, closed by Go's `deeptest.AssertRecursivelyNotEqual` guard that the
/// table names every `Context` field.
// go-parity-gap: NewMetaBuildContextWithSctx (pkg/ddl/metabuild.go:24) and
// the session-vars axis it reads are not transcreated; the Context getter
// half is covered by tidb-expr/src/metabuild.rs tests.
#[test]
#[ignore]
fn new_meta_build_context_with_sctx_flows_session_facts_into_the_context() {
}
