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

//! Go `rmclient` (the PD client's `resource_group/controller`): the types
//! TiDB reads from it.

/// boundary: Go `rmclient.RUVersion` from the PD client's
/// `resource_group/controller`, an integer enum whose zero value means
/// "unspecified".
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct RuVersion(pub i32);

impl RuVersion {
    /// The Go zero value, which [`normalize_ru_version`] maps to the default.
    pub const UNSPECIFIED: Self = Self(0);
    /// Go `rmclient.RUVersionV1`.
    pub const V1: Self = Self(1);
    /// Go `rmclient.RUVersionV2`.
    pub const V2: Self = Self(2);
}
