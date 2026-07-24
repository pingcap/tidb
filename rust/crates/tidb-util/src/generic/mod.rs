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

//! Complete transcreation of Go `pkg/util/generic` (`sync_map.go`,
//! `bounded_min_heap.go`).
//!
//! Go's type-parameterized helpers map directly onto Rust generics. The two
//! production files become two modules re-exported here so the public surface
//! (`SyncMap`, `BoundedMinHeap`) mirrors the Go package.

mod bounded_min_heap;
mod sync_map;

pub use bounded_min_heap::BoundedMinHeap;
pub use sync_map::SyncMap;
