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

//! `pkg/util/chunk`: the columnar row container that every executor produces
//! and every expression `Eval*` consumes.
//!
//! SEED SCOPE (grown incrementally): [`column`] ports the `Column` columnar
//! storage (fixed-length int/real + variable-length string/bytes + the
//! `getFixedLen`/`NewColumn` type dispatch); [`chunk`] the `Chunk` batch; and
//! [`row`] the `Row` cursor that expression evaluation reads. DEFERRED
//! (documented per module): the typed appends/getters for
//! `VectorFloat32`, `Reset(EvalType)`, the growth/pool/disk paths,
//! and a `str`-typed `GetString`.

pub mod chunk;
pub mod column;
pub mod row;
