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

//! go-parity-gap carriers for two `pkg/expression/util_test.go` entries that
//! pin infrastructure this workspace replaced by design (batch part11 items
//! 649-650).
//!
//! Go keeps one `builtinFunc` OBJECT per scalar-function signature; every sig
//! struct implements `evalInt`/`evalReal`/... over the embedded
//! `baseBuiltinFunc`, and `Clone()` must preserve each concrete type. The
//! Rust side is name-keyed instead ([`crate::scalar_function`]'s BRIDGE
//! DECISION: `ScalarFunction` holds `func_name` + args, dispatch happens in
//! [`crate::scalar_function::eval`] / the builtin modules), so neither the
//! bare-base guard nor the per-sig clone table has an analogue to drive.

/// go-parity-gap: `TestBaseBuiltin` (`util_test.go:33`) builds
/// `newBaseBuiltinFuncWithTp(ctx, "", nil, types.ETTimestamp)` and requires
/// ALL SEVEN typed entrypoints (`evalInt`..`evalJSON`) of the bare
/// `baseBuiltinFunc` to error with "should never be called"
/// (`pkg/expression/builtin.go:353-378`). The name-keyed port has no base
/// builtin struct whose seven typed methods could be exercised.
#[test]
#[ignore = "go-parity-gap: bare baseBuiltinFunc's seven never-call eval entrypoints (builtin.go:353-378) need the per-signature object model this crate replaced"]
fn test_base_builtin() {}

/// go-parity-gap: `TestClone` (`util_test.go:53`) instantiates roughly 300
/// distinct concrete signature structs (`builtinArithmeticPlusIntSig`,
/// `builtinCastStringAsTimeSig`, ...) and requires each `Clone()` to preserve
/// the CONCRETE type (`require.IsType`). Signatures are not reified as types
/// here — evaluation routes through name-keyed dispatch — so there is no
/// per-sig clone table; the node-level clone contract (`ScalarFunction`
/// derives `Clone`) is pinned by tests/scalar_function_semantics_source.rs.
#[test]
#[ignore = "go-parity-gap: TestClone's ~300 concrete *Sig Clone()/IsType rows need reified signature structs (name-keyed dispatch instead); node-level Clone is pinned in scalar_function_semantics_source"]
fn test_clone() {}
