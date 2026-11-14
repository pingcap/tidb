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

//! Go `pkg/extension/manifest.go`.

use std::sync::{Arc, RwLock};

use tidb_exprctx::EvalError;
use tidb_mysql::privilege::PrivilegeType;

use crate::function::FunctionDef;
use crate::util::{ClearFunc, ClearFuncBuilder};

/// Go `Option`: one setting of an extension's manifest.
pub type ExtensionOption = Arc<dyn Fn(&mut Manifest) + Send + Sync>;

/// Go `AccessCheckFunc`: the dynamic privileges an access to a database,
/// table and column with `priv` needs, given whether SEM is enabled.
pub type AccessCheckFunc =
    Arc<dyn Fn(&str, &str, &str, PrivilegeType, bool) -> Vec<String> + Send + Sync>;

/// Go `WithCustomDynPrivs`.
#[must_use]
pub fn with_custom_dyn_privs(privs: Vec<String>) -> ExtensionOption {
    Arc::new(move |m: &mut Manifest| m.dyn_privs.clone_from(&privs))
}

/// Go `WithCustomFunctions`.
#[must_use]
pub fn with_custom_functions(funcs: Vec<FunctionDef>) -> ExtensionOption {
    Arc::new(move |m: &mut Manifest| m.funcs.clone_from(&funcs))
}

/// Go `WithCustomAccessCheck`.
#[must_use]
pub fn with_custom_access_check(f: AccessCheckFunc) -> ExtensionOption {
    Arc::new(move |m: &mut Manifest| m.access_check_func = Some(Arc::clone(&f)))
}

/// Go `WithClose`: the function `Reset` invokes.
#[must_use]
pub fn with_close(f: Arc<dyn Fn() + Send + Sync>) -> ExtensionOption {
    Arc::new(move |m: &mut Manifest| m.close = Some(Arc::clone(&f)))
}

/// Go `Manifest`: an extension's manifest.
#[derive(Clone, Default)]
pub struct Manifest {
    name: String,
    dyn_privs: Vec<String>,
    funcs: Vec<FunctionDef>,
    pub(crate) access_check_func: Option<AccessCheckFunc>,
    close: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl Manifest {
    /// Go `Name`.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Go `newManifestWithSetup`: the manifest the factory's options build,
/// its dynamic privileges and functions registered; on an error every step
/// done so far is undone.
pub(crate) fn new_manifest_with_setup(
    name: &str,
    factory: &(dyn Fn() -> Result<Vec<ExtensionOption>, EvalError> + Send + Sync),
) -> Result<(Manifest, ClearFunc), EvalError> {
    let mut clear_builder = ClearFuncBuilder::default();
    match setup_manifest(name, factory, &mut clear_builder) {
        Ok(m) => Ok((m, clear_builder.build())),
        Err(error) => {
            clear_builder.build()();
            Err(error)
        }
    }
}

fn setup_manifest(
    name: &str,
    factory: &(dyn Fn() -> Result<Vec<ExtensionOption>, EvalError> + Send + Sync),
    clear_builder: &mut ClearFuncBuilder,
) -> Result<Manifest, EvalError> {
    // new manifest with factory
    let mut m = Manifest {
        name: name.to_owned(),
        ..Manifest::default()
    };
    clear_builder.do_with_collect_clear(|| {
        for opt in factory()? {
            opt(&mut m);
        }
        Ok::<_, EvalError>(m.close.clone().map(|close| Box::new(move || close()) as ClearFunc))
    })?;
    // setup dynamic privileges
    for privilege in &m.dyn_privs {
        clear_builder.do_with_collect_clear(|| {
            hook(&REGISTER_DYNAMIC_PRIVILEGE)(privilege).map_err(EvalError::Raw)?;
            let privilege = privilege.clone();
            Ok::<_, EvalError>(Some(Box::new(move || {
                hook(&REMOVE_DYNAMIC_PRIVILEGE)(&privilege);
            }) as ClearFunc))
        })?;
    }
    // setup functions
    for def in &m.funcs {
        clear_builder.do_with_collect_clear(|| {
            hook(&REGISTER_EXTENSION_FUNC)(def)?;
            let name = def.name.clone();
            Ok::<_, EvalError>(Some(Box::new(move || {
                hook(&REMOVE_EXTENSION_FUNC)(&name);
            }) as ClearFunc))
        })?;
    }
    Ok(m)
}

/// Go `RegisterDynamicPrivilege`, which the privileges package sets (a
/// dependency cycle otherwise).
pub static REGISTER_DYNAMIC_PRIVILEGE: RwLock<Option<fn(&str) -> Result<(), String>>> =
    RwLock::new(None);
/// Go `RemoveDynamicPrivilege`.
pub static REMOVE_DYNAMIC_PRIVILEGE: RwLock<Option<fn(&str) -> bool>> = RwLock::new(None);
/// Go `RegisterExtensionFunc`, which the expression package sets.
pub static REGISTER_EXTENSION_FUNC: RwLock<Option<fn(&FunctionDef) -> Result<(), EvalError>>> =
    RwLock::new(None);
/// Go `RemoveExtensionFunc`.
pub static REMOVE_EXTENSION_FUNC: RwLock<Option<fn(&str)>> = RwLock::new(None);

/// The function a hook holds; Go calls the variable as its owner's `init`
/// set it, and a hook nothing set is a nil-function call.
fn hook<F: Copy>(slot: &RwLock<Option<F>>) -> F {
    slot.read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .expect("the extension hook is set by its owning package's init")
}
