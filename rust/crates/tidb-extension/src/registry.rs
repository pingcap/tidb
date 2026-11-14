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

//! Go `pkg/extension/registry.go`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tidb_exprctx::EvalError;

use crate::extensions::Extensions;
use crate::manifest::{new_manifest_with_setup, ExtensionOption};
use crate::util::{ClearFunc, ClearFuncBuilder};

type Factory = Arc<dyn Fn() -> Result<Vec<ExtensionOption>, EvalError> + Send + Sync>;

/// Go `registry`.
#[derive(Default)]
struct Registry {
    factories: HashMap<String, Factory>,
    extension_names: Vec<String>,
    setup: bool,
    extensions: Option<Arc<Extensions>>,
    close: Option<ClearFunc>,
}

impl Registry {
    /// Go `RegisterFactory`.
    fn register_factory(&mut self, name: &str, factory: Factory) -> Result<(), EvalError> {
        if self.setup {
            return Err(EvalError::Raw(
                "Cannot register new extension because registry has already been setup".to_owned(),
            ));
        }
        if name.is_empty() {
            return Err(EvalError::Raw("extension name should not be empty".to_owned()));
        }
        if self.factories.contains_key(name) {
            return Err(EvalError::Raw(format!("extension with name '{name}' already registered")));
        }
        self.factories.insert(name.to_owned(), factory);
        self.extension_names.push(name.to_owned());
        self.extension_names.sort();
        Ok(())
    }

    /// Go `doSetup`.
    fn do_setup(&mut self) -> Result<Option<Arc<Extensions>>, EvalError> {
        if self.setup {
            return Ok(self.extensions.clone());
        }
        if self.factories.is_empty() {
            self.extensions = None;
            self.setup = true;
            return Ok(None);
        }
        let mut clear_builder = ClearFuncBuilder::default();
        let mut manifests = Vec::with_capacity(self.factories.len());
        for name in &self.extension_names {
            let factory = &self.factories[name];
            let collected = clear_builder.do_with_collect_clear(|| {
                let (m, clear) = new_manifest_with_setup(name, factory.as_ref())?;
                manifests.push(m);
                Ok::<_, EvalError>(Some(clear))
            });
            if let Err(error) = collected {
                clear_builder.build()();
                return Err(error);
            }
        }
        self.extensions = Some(Arc::new(Extensions { manifests }));
        self.setup = true;
        self.close = Some(clear_builder.build());
        Ok(self.extensions.clone())
    }

    /// Go `Reset`.
    fn reset(&mut self) {
        if let Some(close) = self.close.take() {
            close();
        }
        self.factories.clear();
        self.extension_names.clear();
        self.extensions = None;
        self.setup = false;
    }
}

/// Go `globalRegistry`.
static GLOBAL_REGISTRY: Mutex<Option<Registry>> = Mutex::new(None);

fn with_registry<T>(f: impl FnOnce(&mut Registry) -> T) -> T {
    let mut registry = GLOBAL_REGISTRY.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    f(registry.get_or_insert_with(Registry::default))
}

/// Go `RegisterFactory`: registers a new extension with a factory.
///
/// # Errors
///
/// Go's: the registry is set up, the name is empty or already registered.
pub fn register_factory(
    name: &str,
    factory: impl Fn() -> Result<Vec<ExtensionOption>, EvalError> + Send + Sync + 'static,
) -> Result<(), EvalError> {
    with_registry(|registry| registry.register_factory(name, Arc::new(factory)))
}

/// Go `Register`: registers a new extension with options.
///
/// # Errors
///
/// See [`register_factory`].
pub fn register(name: &str, options: Vec<ExtensionOption>) -> Result<(), EvalError> {
    register_factory(name, move || Ok(options.clone()))
}

/// Go `Setup`: sets every registered extension up.
///
/// # Errors
///
/// The first extension's setup error; every setup step is undone.
pub fn setup() -> Result<(), EvalError> {
    with_registry(|registry| registry.do_setup().map(|_| ()))
}

/// Go `GetExtensions`: the extensions, set up first if they are not;
/// `None` when none is registered (Go's nil `*Extensions`).
///
/// # Errors
///
/// See [`setup`].
pub fn get_extensions() -> Result<Option<Arc<Extensions>>, EvalError> {
    with_registry(Registry::do_setup)
}

/// Go `Reset` (test only): closes the extensions and forgets them.
pub fn reset() {
    with_registry(Registry::reset);
}
