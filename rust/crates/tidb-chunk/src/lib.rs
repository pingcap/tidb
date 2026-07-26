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
//! SEED SCOPE (grown incrementally): [`column`] ports the fixed-length core of
//! the columnar `Column` -- the null bitmap plus the `int64`/`uint64`/`float32`/
//! `float64` append/get path, which is what simple integer/real queries need.
//! DEFERRED (documented in `column.rs`): variable-length storage (string/bytes
//! via `offsets`), the typed appends/resizes for Time/Duration/Decimal/JSON/
//! Enum/Set/VectorFloat32, `NewColumn(FieldType)`/`getFixedLen` type dispatch,
//! and the `Chunk`/`Row` containers built on top of `Column`.

pub mod column;
