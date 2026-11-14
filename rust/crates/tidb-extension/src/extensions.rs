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
    /// Go `Extensions.Bootstrap`: manifest order, stopping at the first error.
    pub fn bootstrap(&self, context: &dyn crate::BootstrapContext) -> tidb_sqlexec::Result<()> {
        for m in &self.manifests {
            if let Some(bootstrap) = &m.bootstrap {
                bootstrap(context)?;
            }
        }
        Ok(())
    }

    /// Go `Extensions.NewSessionExtensions`.
    pub fn new_session_extensions(&self) -> crate::SessionExtensions {
        crate::SessionExtensions::new(self)
    }

    /// Go `Extensions.GetAuthPlugins`.
    pub fn get_auth_plugins(&self) -> std::collections::HashMap<String, crate::AuthPlugin> {
        self.manifests
            .iter()
            .flat_map(|m| m.auth_plugins.iter().flatten())
            .map(|p| (p.name.clone(), p.clone()))
            .collect()
    }

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
