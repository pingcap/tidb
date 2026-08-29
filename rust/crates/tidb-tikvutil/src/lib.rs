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

//! Process-wide TiKV client settings owned by `pkg/util/tikvutil`.

use std::sync::atomic::AtomicI32;

/// Go `CommitterConcurrency`.
pub static COMMITTER_CONCURRENCY: AtomicI32 = AtomicI32::new(128);
