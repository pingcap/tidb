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

//! Go `pkg/dxf/framework/testutil`: helpers the framework's tests share.
//! The store and session setup (Go `InitTableTest`) belongs to the test
//! that builds a node; these helpers need only a `TaskManager`.

mod table_util;
mod task_util;

pub use table_util::*;
pub use task_util::*;
