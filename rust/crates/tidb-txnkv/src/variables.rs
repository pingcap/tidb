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

//! KV client variables re-exported by `pkg/kv/variables.go`.

pub use tikv_client::kv::{
    Variables as KvVariables, DEF_BACKOFF_LOCK_FAST as DEFAULT_BACKOFF_LOCK_FAST,
    DEF_BACKOFF_WEIGHT as DEFAULT_BACKOFF_WEIGHT,
};
