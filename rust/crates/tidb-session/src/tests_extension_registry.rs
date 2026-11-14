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

//! Go `pkg/extension/registry_test.go`: registering and setting extensions
//! up.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use tidb_exprctx::EvalError;

/// Go `defer extension.Reset()`.
struct ResetOnDrop;

impl Drop for ResetOnDrop {
    fn drop(&mut self) {
        tidb_extension::reset();
    }
}

fn error_text(result: Result<(), EvalError>) -> String {
    match result {
        Err(EvalError::Raw(message)) => message,
        other => panic!("{other:?}"),
    }
}

fn manifest_names() -> Vec<String> {
    tidb_extension::get_extensions()
        .unwrap()
        .map(|extensions| extensions.manifests().iter().map(|m| m.name().to_owned()).collect())
        .unwrap_or_default()
}

/// Go `TestSetupExtensions`.
#[test]
fn setup_extensions() {
    let _reset = ResetOnDrop;

    tidb_extension::reset();
    tidb_extension::setup().unwrap();
    assert!(manifest_names().is_empty());

    tidb_extension::reset();
    tidb_extension::register("test1", Vec::new()).unwrap();
    tidb_extension::register("test2", Vec::new()).unwrap();
    tidb_extension::setup().unwrap();
    assert_eq!(manifest_names(), ["test1", "test2"]);
}

/// Go `TestExtensionRegisterName`.
#[test]
fn extension_register_name() {
    let _reset = ResetOnDrop;

    // test empty name
    tidb_extension::reset();
    assert_eq!(error_text(tidb_extension::register("", Vec::new())), "extension name should not be empty");

    // test dup name
    tidb_extension::reset();
    tidb_extension::register("test", Vec::new()).unwrap();
    assert_eq!(
        error_text(tidb_extension::register("test", Vec::new())),
        "extension with name 'test' already registered"
    );
}

/// Go `TestRegisterExtensionWithClose`.
#[test]
fn register_extension_with_close() {
    let _reset = ResetOnDrop;

    // normal register
    tidb_extension::reset();
    let cnt = Arc::new(AtomicI64::new(0));
    let close = {
        let cnt = Arc::clone(&cnt);
        tidb_extension::with_close(Arc::new(move || {
            cnt.fetch_add(1, Ordering::SeqCst);
        }))
    };
    tidb_extension::register("test1", vec![close.clone()]).unwrap();
    tidb_extension::setup().unwrap();
    assert_eq!(cnt.load(Ordering::SeqCst), 0);

    // reset will call close
    tidb_extension::reset();
    assert_eq!(cnt.load(Ordering::SeqCst), 1);

    // reset again has no effect
    tidb_extension::reset();
    assert_eq!(cnt.load(Ordering::SeqCst), 1);

    // Auto close when error
    cnt.store(0, Ordering::SeqCst);
    tidb_extension::reset();
    tidb_extension::register("test1", vec![close]).unwrap();
    tidb_extension::register_factory("test2", || Err(EvalError::Raw("error abc".to_owned()))).unwrap();
    assert_eq!(error_text(tidb_extension::setup()), "error abc");
    assert_eq!(cnt.load(Ordering::SeqCst), 1);
}

/// Go `TestRegisterExtensionWithDyncPrivs`.
#[test]
fn register_extension_with_dync_privs() {
    crate::init_extension_hooks();
    let _reset = ResetOnDrop;
    let privs = |names: &[&str]| {
        tidb_extension::with_custom_dyn_privs(names.iter().map(|name| (*name).to_owned()).collect())
    };

    let orig_dyn_privs = tidb_privilege::get_dynamic_privileges();

    tidb_extension::reset();
    tidb_extension::register("test", vec![privs(&["priv1", "priv2"])]).unwrap();
    tidb_extension::setup().unwrap();
    let registered = tidb_privilege::get_dynamic_privileges();
    assert_eq!(orig_dyn_privs, registered[..orig_dyn_privs.len()]);
    assert_eq!(registered[orig_dyn_privs.len()..], ["PRIV1", "PRIV2"]);

    // test for empty dynamic privilege name
    tidb_extension::reset();
    tidb_extension::register("test", vec![privs(&["priv1", ""])]).unwrap();
    assert_eq!(error_text(tidb_extension::setup()), "privilege name should not be empty");
    assert_eq!(orig_dyn_privs, tidb_privilege::get_dynamic_privileges());

    // test for duplicate name with builtin
    tidb_extension::reset();
    tidb_extension::register("test", vec![privs(&["priv1", "ROLE_ADMIN"])]).unwrap();
    assert_eq!(error_text(tidb_extension::setup()), "privilege is already registered");
    assert_eq!(orig_dyn_privs, tidb_privilege::get_dynamic_privileges());

    // test for duplicate name with other extension
    tidb_extension::reset();
    tidb_extension::register("test1", vec![privs(&["priv1"])]).unwrap();
    tidb_extension::register("test2", vec![privs(&["priv2", "priv1"])]).unwrap();
    assert_eq!(error_text(tidb_extension::setup()), "privilege is already registered");
    assert_eq!(orig_dyn_privs, tidb_privilege::get_dynamic_privileges());
}
