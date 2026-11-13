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


//! Go `pkg/extension/function_test.go`: an extension's functions through
//! SQL. Go registers them with `extension.Register(..., WithCustomFunctions,
//! WithCustomDynPrivs)` and `extension.Setup`, whose manifest calls
//! `RegisterExtensionFunc` / `RegisterDynamicPrivilege` for each; these
//! tests call those two directly and remove what they registered after.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use tidb_datatype::EvalType;
use tidb_exprctx::EvalError;
use tidb_extension::{FunctionContext, FunctionDef};

use crate::tests_support::*;
use crate::*;

/// Go `defer extension.Reset()`: removes the functions and privileges a
/// test registered.
struct Registered {
    funcs: Vec<String>,
    privs: Vec<String>,
}

impl Registered {
    fn new(defs: &[FunctionDef], privs: &[&str]) -> Self {
        for privilege in privs {
            tidb_privilege::register_dynamic_privilege(privilege).unwrap();
        }
        for def in defs {
            tidb_expr::extension::register_extension_func(def).unwrap();
        }
        Self {
            funcs: defs.iter().map(|def| def.name.clone()).collect(),
            privs: privs.iter().map(|privilege| (*privilege).to_owned()).collect(),
        }
    }
}

impl Drop for Registered {
    fn drop(&mut self) {
        for name in &self.funcs {
            tidb_expr::extension::remove_extension_func(name);
        }
        for privilege in &self.privs {
            tidb_privilege::remove_dynamic_privilege(privilege);
        }
    }
}

fn string_func(
    name: &str,
    arg_tps: Vec<EvalType>,
    eval: impl Fn(&dyn FunctionContext, tidb_chunk::row::Row<'_>) -> Result<(String, bool), EvalError>
        + Send
        + Sync
        + 'static,
) -> FunctionDef {
    FunctionDef {
        name: name.to_owned(),
        eval_tp: EvalType::String,
        arg_tps,
        eval_string_func: Some(Arc::new(eval)),
        ..FunctionDef::default()
    }
}

fn int_func(
    name: &str,
    arg_tps: Vec<EvalType>,
    eval: impl Fn(&dyn FunctionContext, tidb_chunk::row::Row<'_>) -> Result<(i64, bool), EvalError>
        + Send
        + Sync
        + 'static,
) -> FunctionDef {
    FunctionDef {
        name: name.to_owned(),
        eval_tp: EvalType::Int,
        arg_tps,
        eval_int_func: Some(Arc::new(eval)),
        ..FunctionDef::default()
    }
}

/// Go `customFunc1`.
fn custom_func1() -> FunctionDef {
    string_func("custom_func1", vec![EvalType::Int, EvalType::String], |ctx, row| {
        let args = ctx.eval_args(row)?;
        let text = String::from_utf8_lossy(&args[1].sql_bytes().unwrap()).into_owned();
        if text == "error" {
            return Err(EvalError::Raw("custom error".to_owned()));
        }
        Ok((format!("{},{text}", args[0].as_int().unwrap()), false))
    })
}

/// Go `customFunc2`.
fn custom_func2() -> FunctionDef {
    int_func("custom_func2", vec![EvalType::Int, EvalType::Int], |ctx, row| {
        let args = ctx.eval_args(row)?;
        Ok((args[0].as_int().unwrap() * 100 + args[1].as_int().unwrap(), false))
    })
}

/// Go `checkFuncList`.
fn check_func_list(org_list: &[String], custom_funcs: &[&str]) {
    for name in org_list {
        assert!(!name.starts_with("custom_"), "{name}");
    }
    let mut check_list = org_list.to_vec();
    check_list.extend(custom_funcs.iter().map(|name| (*name).to_owned()));
    check_list.sort();
    assert_eq!(check_list, tidb_expr::builtin_registry::get_builtin_list());
}

/// Go `TestExtensionFuncCtx`, without the `ConnectionInfo` this port's
/// session does not keep.
#[test]
fn extension_func_ctx() {
    type Seen = (Option<tidb_parser::auth::UserIdentity>, String, Vec<tidb_parser::auth::RoleIdentity>);
    let seen: Arc<Mutex<Option<Seen>>> = Arc::default();
    let record = Arc::clone(&seen);
    let _registered = Registered::new(
        &[string_func("custom_get_ctx", Vec::new(), move |ctx, _| {
            let mut seen = record.lock().unwrap();
            assert!(seen.is_none());
            *seen = Some((ctx.user(), ctx.current_db(), ctx.active_roles()));
            Ok(("done".to_owned(), false))
        })],
        &[],
    );

    let mut tk = session_with_privileges();
    tk.run("use test").unwrap();
    tk.run("create user u1@localhost").unwrap();
    tk.run("create role r1").unwrap();
    tk.run("grant r1 to u1@localhost").unwrap();
    tk.run("grant ALL ON test.* to u1@localhost").unwrap();

    let registry = tk.privileges.clone().unwrap();
    let mut tk1 = session_as(&registry, tk.catalog.clone(), "u1", "localhost");
    tk1.run("set role r1").unwrap();
    tk1.run("use test").unwrap();

    assert_eq!(row_text(tk1.run("select custom_get_ctx()")), [["done"]]);

    let (user, current_db, active_roles) = seen.lock().unwrap().take().expect("invoked");
    let user = user.expect("a user");
    assert_eq!((user.username.as_str(), user.hostname.as_str()), ("u1", "localhost"));
    assert_eq!(
        (user.auth_username.as_str(), user.auth_hostname.as_str()),
        ("u1", "localhost")
    );
    assert_eq!(current_db, "test");
    assert_eq!(
        active_roles,
        [tidb_parser::auth::RoleIdentity { username: "r1".to_owned(), hostname: "%".to_owned() }]
    );
}

/// Go `TestInvokeExtensionFunc`.
#[test]
fn invoke_extension_func() {
    let org_func_list = tidb_expr::builtin_registry::get_builtin_list();
    check_func_list(&org_func_list, &[]);
    {
        let _registered = Registered::new(&[custom_func1(), custom_func2()], &[]);
        check_func_list(&org_func_list, &["custom_func1", "custom_func2"]);

        let mut tk = Session::new();
        assert_eq!(row_text(tk.run("select custom_func1(1, 'abc')")), [["1,abc"]]);
        assert_eq!(row_text(tk.run("select custom_func2(7, 8)")), [["708"]]);
        assert_err(tk.run("select custom_func1(1, 'error')"), 1105, "custom error");
        assert_err(
            tk.run("select custom_func1(1)"),
            1582,
            "Incorrect parameter count in the call to native function 'custom_func1'",
        );
    }
    check_func_list(&org_func_list, &[]);
    let mut tk2 = Session::new();
    tk2.run("use test").unwrap();
    assert_err(tk2.run("select custom_func1(1, 'abc')"), 1305, "FUNCTION test.custom_func1 does not exist");
    assert_err(tk2.run("select custom_func2(1, 2)"), 1305, "FUNCTION test.custom_func2 does not exist");
}

/// Go `TestExtensionFuncDynamicArgLen`.
#[test]
fn extension_func_dynamic_arg_len() {
    let def = FunctionDef {
        optional_args_len: 1,
        ..int_func("dynamic_arg_func", vec![EvalType::Int; 4], |ctx, row| {
            let args = ctx.eval_args(row)?;
            Ok((args.iter().fold(0, |result, arg| result * 10 + arg.as_int().unwrap()), false))
        })
    };
    let _registered = Registered::new(&[def], &[]);

    let mut tk = Session::new();
    assert_eq!(row_text(tk.run("select dynamic_arg_func(1, 2, 3)")), [["123"]]);
    assert_eq!(row_text(tk.run("select dynamic_arg_func(1, 2, 3, 4)")), [["1234"]]);
    let expected = "Incorrect parameter count in the call to native function 'dynamic_arg_func'";
    assert_err(tk.run("select dynamic_arg_func()"), 1582, expected);
    assert_err(tk.run("select dynamic_arg_func(1)"), 1582, expected);
    assert_err(tk.run("select dynamic_arg_func(1, 2)"), 1582, expected);
}

/// Go `TestRegisterExtensionFunc`'s refusals, each leaving the function
/// list as it was (Go's nil-definition case has no Rust spelling).
#[test]
fn register_extension_func() {
    use tidb_expr::extension::{register_extension_func, remove_extension_func};
    let org_func_list = tidb_expr::builtin_registry::get_builtin_list();
    let refused = |def: &FunctionDef| match register_extension_func(def) {
        Err(EvalError::Raw(message)) => message,
        other => panic!("{other:?}"),
    };

    // dup name with builtin
    let def = FunctionDef { name: "substring".to_owned(), ..custom_func1() };
    assert_eq!(refused(&def), "extension function name 'substring' conflict with builtin");
    check_func_list(&org_func_list, &[]);

    // empty func name
    let def = FunctionDef { name: String::new(), ..custom_func1() };
    assert_eq!(refused(&def), "extension function name should not be empty");
    check_func_list(&org_func_list, &[]);

    // dup name with other func
    register_extension_func(&custom_func1()).unwrap();
    assert_eq!(refused(&custom_func1()), "duplicated extension function name 'custom_func1'");
    remove_extension_func("custom_func1");
    check_func_list(&org_func_list, &[]);

    // Go `newExtensionFuncClass`'s refusals.
    let def = FunctionDef { eval_string_func: None, ..custom_func1() };
    assert_eq!(refused(&def), "eval function is nil");
    let def = FunctionDef { eval_tp: EvalType::Real, ..custom_func1() };
    assert_eq!(refused(&def), "unsupported extension function ret type: 'Real'");
    check_func_list(&org_func_list, &[]);
}

/// Go `testExtensionFuncPrivilege` under SEM v1. Go runs it under SEM v2
/// too, with `compatibleSEMV2Config`, which is not ported.
#[test]
fn extension_func_privilege() {
    let privileged = |name: &str, eval_tp: EvalType, value: &'static str, require: fn(bool) -> Vec<String>| FunctionDef {
        require_dynamic_privileges: Some(Arc::new(require)),
        ..match eval_tp {
            EvalType::Int => int_func(name, Vec::new(), move |_, _| Ok((value.parse().unwrap(), false))),
            _ => string_func(name, Vec::new(), move |_, _| Ok((value.to_owned(), false))),
        }
    };
    let both = |sem: bool| {
        if sem {
            vec!["RESTRICTED_CUSTOM_DYN_PRIV_2".to_owned()]
        } else {
            vec!["CUSTOM_DYN_PRIV_1".to_owned()]
        }
    };
    let _registered = Registered::new(
        &[
            string_func("custom_no_priv_func", Vec::new(), |_, _| Ok(("zzz".to_owned(), false))),
            privileged("custom_only_dyn_priv_func", EvalType::String, "abc", |_| {
                vec!["CUSTOM_DYN_PRIV_1".to_owned()]
            }),
            privileged("custom_only_sem_dyn_priv_func", EvalType::String, "def", |sem| {
                if sem {
                    vec!["RESTRICTED_CUSTOM_DYN_PRIV_2".to_owned()]
                } else {
                    Vec::new()
                }
            }),
            privileged("custom_both_dyn_priv_func", EvalType::String, "ghi", both),
            privileged("custom_eval_int_func", EvalType::Int, "1", both),
        ],
        &["CUSTOM_DYN_PRIV_1", "RESTRICTED_CUSTOM_DYN_PRIV_2"],
    );

    let mut tk = session_with_privileges();
    tk.run("use test").unwrap();
    tk.run("create user u1@localhost").unwrap();
    tk.run("create user u2@localhost").unwrap();
    tk.run("GRANT CUSTOM_DYN_PRIV_1 on *.* TO u2@localhost").unwrap();
    tk.run("create user u3@localhost").unwrap();
    tk.run("GRANT RESTRICTED_CUSTOM_DYN_PRIV_2 on *.* TO u3@localhost").unwrap();
    tk.run("create user u4@localhost").unwrap();
    tk.run("GRANT CUSTOM_DYN_PRIV_1, RESTRICTED_CUSTOM_DYN_PRIV_2 on *.* TO u4@localhost").unwrap();
    let registry = tk.privileges.clone().unwrap();
    // Go's `Auth` matches root@localhost to the root@% account, the
    // identity this port's session carries.
    let login = |user: &str| {
        let host = if user == "root" { "%" } else { "localhost" };
        session_as(&registry, tk.catalog.clone(), user, host)
    };

    let denied = |privilege: &str| {
        format!("Access denied; you need (at least one of) the {privilege} privilege(s) for this operation")
    };
    // (user, the five functions' results in order; `Err` names the
    // privilege a denial reports).
    let check = |tk1: &mut Session, expected: [Result<&str, &str>; 5]| {
        let funcs = [
            "custom_no_priv_func",
            "custom_only_dyn_priv_func",
            "custom_only_sem_dyn_priv_func",
            "custom_both_dyn_priv_func",
            "custom_eval_int_func",
        ];
        for (func, expected) in funcs.iter().zip(expected) {
            let sql = format!("select {func}()");
            match expected {
                Ok(value) => assert_eq!(row_text(tk1.run(&sql)), [[value]], "{sql}"),
                Err(privilege) => assert_err(tk1.run(&sql), 1227, &denied(privilege)),
            }
        }
    };
    let all = [Ok("zzz"), Ok("abc"), Ok("def"), Ok("ghi"), Ok("1")];

    // root has all privileges by default
    check(&mut login("root"), all);
    // u1 in non-sem
    let mut u1 = login("u1");
    let super_or_1 = Err("SUPER or CUSTOM_DYN_PRIV_1");
    check(&mut u1, [Ok("zzz"), super_or_1, Ok("def"), super_or_1, super_or_1]);
    // prepare should check privilege
    assert_err(u1.run("prepare stmt1 from 'select custom_only_dyn_priv_func()'"), 1227, &denied("SUPER or CUSTOM_DYN_PRIV_1"));
    assert_err(u1.run("prepare stmt2 from 'select custom_eval_int_func()'"), 1227, &denied("SUPER or CUSTOM_DYN_PRIV_1"));
    // u2, u3, u4 in non-sem
    check(&mut login("u2"), all);
    check(&mut login("u3"), [Ok("zzz"), super_or_1, Ok("def"), super_or_1, super_or_1]);
    check(&mut login("u4"), all);

    tidb_sem::enable();
    struct SemOff;
    impl Drop for SemOff {
        fn drop(&mut self) {
            tidb_sem::disable();
        }
    }
    let _sem_off = SemOff;
    let restricted_2 = Err("RESTRICTED_CUSTOM_DYN_PRIV_2");
    let only_1 = Err("CUSTOM_DYN_PRIV_1");
    // root in sem
    check(&mut login("root"), [Ok("zzz"), Ok("abc"), restricted_2, restricted_2, restricted_2]);
    // u1 in sem
    check(&mut login("u1"), [Ok("zzz"), only_1, restricted_2, restricted_2, restricted_2]);
    // u2 in sem
    check(&mut login("u2"), [Ok("zzz"), Ok("abc"), restricted_2, restricted_2, restricted_2]);
    // u3 in sem
    check(&mut login("u3"), [Ok("zzz"), only_1, Ok("def"), Ok("ghi"), Ok("1")]);
    // u4 in sem
    let mut u4 = login("u4");
    check(&mut u4, all);

    // The privilege is checked again when a prepared statement evaluates,
    // after a REVOKE: Go's `Fix49736` forces the plan cache so the cached
    // signature is evaluated.
    u4.run("SET tidb_opt_fix_control = '49736:ON'").unwrap();
    u4.run("prepare s1 from 'select custom_both_dyn_priv_func()'").unwrap();
    u4.run("prepare s2 from 'select custom_eval_int_func()'").unwrap();
    assert_eq!(row_text(u4.run("execute s1")), [["ghi"]]);
    assert_eq!(row_text(u4.run("execute s2")), [["1"]]);
    tk.run("REVOKE RESTRICTED_CUSTOM_DYN_PRIV_2 on *.* FROM u4@localhost").unwrap();
    assert_err(u4.run("execute s1"), 1227, &denied("RESTRICTED_CUSTOM_DYN_PRIV_2"));
    assert_err(u4.run("execute s2"), 1227, &denied("RESTRICTED_CUSTOM_DYN_PRIV_2"));
}

/// Go `TestShouldNotOptimizeExtensionFunc`.
#[test]
fn should_not_optimize_extension_func() {
    static CNT: AtomicI64 = AtomicI64::new(0);
    let _registered = Registered::new(
        &[
            int_func("my_func1", Vec::new(), |_, _| Ok((CNT.fetch_add(1, Ordering::SeqCst) + 1, false))),
            string_func("my_func2", Vec::new(), |_, _| {
                let value = CNT.fetch_add(1, Ordering::SeqCst) + 1;
                Ok((if value % 2 == 0 { "abc" } else { "def" }.to_owned(), false))
            }),
        ],
        &[],
    );

    let mut tk = Session::new();
    tk.run("use test").unwrap();
    tk.run("create table t1(a int primary key)").unwrap();
    tk.run("insert into t1 values(1000), (2000)").unwrap();

    // Not folded: called once per row.
    assert_eq!(
        row_text(tk.run("select a, my_func1() from t1 order by a")),
        [["1000", "1"], ["2000", "2"]]
    );
    assert_eq!(CNT.load(Ordering::SeqCst), 2);

    // Not a constant: my_func2 gives each row its own pattern.
    CNT.store(0, Ordering::SeqCst);
    assert_eq!(
        row_text(tk.run("select a, 'abc' regexp my_func2() from t1 order by a")),
        [["1000", "0"], ["2000", "1"]]
    );
}

/// An extension function skips the plan cache (Go's `getFunction`:
/// "extension function should not be cached"), and Go's `Fix49736` forces
/// the cache anyway, `PlanCacheTracker.SetSkipPlanCache` only warning --
/// which `testExtensionFuncPrivilege` relies on to evaluate a cached
/// signature after a REVOKE.
#[test]
fn forced_plan_cache_keeps_an_extension_function_plan() {
    let _registered = Registered::new(
        &[string_func("custom_cached_func", Vec::new(), |_, _| Ok(("zzz".to_owned(), false)))],
        &[],
    );
    let mut tk = Session::new();
    tk.run("prepare s from 'select custom_cached_func()'").unwrap();
    assert_eq!(row_text(tk.run("execute s")), [["zzz"]]);
    assert_eq!(row_text(tk.run("execute s")), [["zzz"]]);
    assert_eq!(row_text(tk.run("select @@last_plan_from_cache")), [["0"]]);

    tk.run("SET tidb_opt_fix_control = '49736:ON'").unwrap();
    tk.run("prepare s from 'select custom_cached_func()'").unwrap();
    assert_eq!(row_text(tk.run("execute s")), [["zzz"]]);
    assert_eq!(
        row_text(tk.run("show warnings")),
        [[
            "Warning",
            "1105",
            "force plan-cache: may use risky cached plan: extension function should not be cached"
        ]]
    );
    assert_eq!(row_text(tk.run("execute s")), [["zzz"]]);
    assert_eq!(row_text(tk.run("select @@last_plan_from_cache")), [["1"]]);
}
