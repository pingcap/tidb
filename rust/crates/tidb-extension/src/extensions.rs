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

//! Go `pkg/extension/extensions.go`.

use crate::manifest::{AccessCheckFunc, Manifest};

/// Go `Extensions`: the extensions set up.
#[derive(Clone, Default)]
pub struct Extensions {
    pub(crate) manifests: Vec<Manifest>,
}

impl Extensions {
    /// Go `Manifests`.
    #[must_use]
    pub fn manifests(&self) -> Vec<Manifest> {
        self.manifests.clone()
    }

    /// Go `GetAccessCheckFuncs`.
    #[must_use]
    pub fn get_access_check_funcs(&self) -> Vec<AccessCheckFunc> {
        self.manifests
            .iter()
            .filter_map(|m| m.access_check_func.clone())
            .collect()
    }
}
