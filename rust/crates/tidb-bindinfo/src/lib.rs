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


//! Go `pkg/bindinfo`: SQL plan bindings -- their normalization and hint
//! transfer (`binding`), the global binding cache (`binding_cache`) and the
//! storage helpers (`binding_utils`).

pub mod binding;
pub mod binding_cache;
pub mod binding_utils;
pub mod metrics;
