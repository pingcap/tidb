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

//! Every Go package's error prototypes (`ErrFoo = ClassBar.NewStd(...)`),
//! one module per package (`catalog`). Not a Go package: each prototype
//! belongs to its package's crate, and moves there as that crate's callers
//! allow. The error machinery itself is its Go packages' crates: tidb-errno,
//! tidb-terror, tidb-errctx, tidb-plannererrors, tidb-server-err, and
//! tidb-mysql's `errors`.

pub mod catalog;


