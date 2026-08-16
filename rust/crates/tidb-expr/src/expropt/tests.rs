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

//! Go `pkg/expression/expropt/optional_test.go`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tidb_parser::auth::{RoleIdentity, UserIdentity};

use super::*;
use crate::metabuild::MetaOnlyInfoSchema;

/// Go's `mockEvalCtx`, which embeds an `EvalContext` and answers only
/// `GetOptionalPropProvider` from its own provider array. Go also overrides
/// `Location`, solely to satisfy the `intest` assertion inside
/// `GetSessionVars` that this port does not carry.
#[derive(Default)]
struct MockEvalCtx {
    props: OptionalEvalPropProviders,
}

impl EvalPropContext for MockEvalCtx {
    fn get_optional_prop_provider(
        &self,
        key: OptionalEvalPropKey,
    ) -> Option<Arc<dyn DynOptionalEvalPropProvider>> {
        self.props.get(key)
    }
}

/// Go rebinds the test's `mockErr` variable in place; the closure captures a
/// shared cell instead.
fn set_mock_err(cell: &Arc<Mutex<Option<ExprOptError>>>, message: Option<&str>) {
    *cell.lock().expect("not poisoned") = message.map(ExprOptError::new);
}

/// Go's `assertReaderFuncReturnErr`.
fn assert_reader_func_return_err<T>(result: Result<T, ExprOptError>) {
    let err = result
        .err()
        .unwrap_or_else(|| panic!("reader must fail while the provider is absent"));
    assert!(
        err.message().contains("not exists in EvalContext"),
        "unexpected error: {err}"
    );
}

/// The message of an error the *provider* itself raised. Go uses
/// `require.EqualError`; the Ok values here have no `Debug`, so the error is
/// matched out by hand.
fn provider_err_message<T>(result: Result<T, ExprOptError>) -> String {
    match result {
        Ok(_) => panic!("the provider must fail"),
        Err(err) => err.message().to_owned(),
    }
}

/// Go's `assertReaderFuncValue`.
fn assert_reader_func_value<T>(result: Result<T, ExprOptError>) -> T {
    result.unwrap_or_else(|err| panic!("reader must succeed: {err}"))
}

/// The `// before add` block of Go's loop body.
fn assert_before_add(
    ctx: &MockEvalCtx,
    key: OptionalEvalPropKey,
    provider: &dyn DynOptionalEvalPropProvider,
    required: OptionalEvalPropKeySet,
) {
    let before_key_set = ctx.props.prop_key_set();
    assert_eq!(
        before_key_set,
        OptionalEvalPropKeySet((1u64 << key.index()) - 1)
    );
    assert!(!before_key_set.contains(key));
    assert!(!ctx.props.contains(key));
    assert_eq!(provider.desc().key().as_prop_key_set(), required);
}

/// The `// after add` block of Go's loop body, returning the stored provider.
fn assert_after_add(
    ctx: &MockEvalCtx,
    key: OptionalEvalPropKey,
) -> Arc<dyn DynOptionalEvalPropProvider> {
    let after_key_set = ctx.props.prop_key_set();
    assert_eq!(
        after_key_set,
        OptionalEvalPropKeySet((1u64 << (key.index() + 1)) - 1)
    );
    assert!(after_key_set.contains(key));
    assert!(ctx.props.contains(key));
    ctx.props
        .get(key)
        .unwrap_or_else(|| panic!("provider for {key} must be present"))
}

struct MockInfoSchema(i64);

impl MetaOnlyInfoSchema for MockInfoSchema {
    fn schema_meta_version(&self) -> i64 {
        self.0
    }
}

struct MockKvStore;
impl KvStorage for MockKvStore {}

struct MockSqlExecutor;
impl SqlExecutor for MockSqlExecutor {}

struct MockSessionVars;
impl SessionVars for MockSessionVars {}

struct MockSessionVarsProvider {
    vars: Arc<dyn SessionVars>,
}

impl SessionVarsProvider for MockSessionVarsProvider {
    fn get_session_vars(&self) -> Arc<dyn SessionVars> {
        Arc::clone(&self.vars)
    }
}

struct MockSequenceOperator;

impl SequenceOperator for MockSequenceOperator {
    fn get_sequence_id(&self) -> i64 {
        1
    }

    fn get_sequence_next_val(&self) -> Result<i64, ExprOptError> {
        Ok(2)
    }

    fn set_sequence_val(&self, new_val: i64) -> Result<(i64, bool), ExprOptError> {
        Ok((new_val, false))
    }
}

struct MockAdvisoryLockContext;

impl AdvisoryLockContext for MockAdvisoryLockContext {
    fn get_advisory_lock(&self, _name: &str, _timeout: i64) -> Result<(), ExprOptError> {
        Ok(())
    }

    fn is_used_advisory_lock(&self, _name: &str) -> u64 {
        0
    }

    fn release_advisory_lock(&self, _name: &str) -> bool {
        false
    }

    fn release_all_advisory_locks(&self) -> i64 {
        0
    }
}

struct MockPrivilegeChecker;

impl PrivilegeChecker for MockPrivilegeChecker {
    fn request_verification(
        &self,
        _db: &str,
        _table: &str,
        _column: &str,
        _privilege: tidb_mysql::privilege::PrivilegeType,
    ) -> bool {
        true
    }

    fn request_dynamic_verification(&self, _priv_name: &str, _grantable: bool) -> bool {
        true
    }
}

/// Go `TestOptionalEvalPropProviders`.
///
/// Go drives one loop over every key with `p`/`reader`/`verifyNoProvider`/
/// `verifyProvider` rebound per iteration; Rust cannot hold those in a single
/// typed variable, so the iterations are spelled out in key order. The
/// per-iteration invariants — the key set is exactly the keys added so far,
/// before and after the `Add` — are shared through `assert_before_add` and
/// `assert_after_add`, and run in the same places.
#[test]
#[allow(clippy::too_many_lines)]
fn optional_eval_prop_providers() {
    let mut ctx = MockEvalCtx::default();
    assert!(ctx.props.prop_key_set().is_empty());

    // OptPropCurrentUser.
    {
        let key = OptionalEvalPropKey::CurrentUser;
        let user = Arc::new(UserIdentity {
            username: "u1".to_owned(),
            hostname: "h1".to_owned(),
            ..UserIdentity::default()
        });
        let roles = vec![
            Arc::new(RoleIdentity {
                username: "u2".to_owned(),
                hostname: "h2".to_owned(),
            }),
            Arc::new(RoleIdentity {
                username: "u3".to_owned(),
                hostname: "h3".to_owned(),
            }),
        ];
        let (captured_user, captured_roles) = (Arc::clone(&user), roles.clone());
        let provider = Arc::new(CurrentUserPropProvider::new(move || {
            (Some(Arc::clone(&captured_user)), captured_roles.clone())
        }));
        let reader = CurrentUserPropReader;

        assert_before_add(
            &ctx,
            key,
            provider.as_ref(),
            reader.required_optional_eval_props(),
        );
        assert_reader_func_return_err(reader.current_user(&ctx));
        assert_reader_func_return_err(reader.active_roles(&ctx));

        ctx.props.add(provider);
        let val = assert_after_add(&ctx, key);

        let got = val
            .as_any()
            .downcast_ref::<CurrentUserPropProvider>()
            .expect("provider must keep its concrete type");
        let (user2, roles2) = got.call();
        assert_eq!(user2.as_deref(), Some(user.as_ref()));
        assert_eq!(roles2, roles);

        assert_eq!(
            assert_reader_func_value(reader.current_user(&ctx)).as_deref(),
            Some(user.as_ref())
        );
        assert_eq!(assert_reader_func_value(reader.active_roles(&ctx)), roles);
    }

    // OptPropSessionVars.
    {
        let key = OptionalEvalPropKey::SessionVars;
        let vars: Arc<dyn SessionVars> = Arc::new(MockSessionVars);
        let provider = Arc::new(new_session_vars_provider(Arc::new(
            MockSessionVarsProvider {
                vars: Arc::clone(&vars),
            },
        )));
        let reader = SessionVarsPropReader;

        assert_before_add(
            &ctx,
            key,
            provider.as_ref(),
            reader.required_optional_eval_props(),
        );
        assert_reader_func_return_err(reader.get_session_vars(&ctx));

        ctx.props.add(provider);
        assert_after_add(&ctx, key);

        let got = assert_reader_func_value(reader.get_session_vars(&ctx));
        assert!(Arc::ptr_eq(&vars, &got));
    }

    // OptPropInfoSchema.
    {
        let key = OptionalEvalPropKey::InfoSchema;
        let is1: Arc<dyn MetaOnlyInfoSchema + Send + Sync> = Arc::new(MockInfoSchema(1));
        let is2: Arc<dyn MetaOnlyInfoSchema + Send + Sync> = Arc::new(MockInfoSchema(2));
        let (captured1, captured2) = (Arc::clone(&is1), Arc::clone(&is2));
        let provider = Arc::new(InfoSchemaPropProvider::new(move |is_domain| {
            if is_domain {
                Arc::clone(&captured1)
            } else {
                Arc::clone(&captured2)
            }
        }));
        let reader = InfoSchemaPropReader;

        assert_before_add(
            &ctx,
            key,
            provider.as_ref(),
            reader.required_optional_eval_props(),
        );
        assert_reader_func_return_err(reader.get_session_info_schema(&ctx));
        assert_reader_func_return_err(reader.get_latest_info_schema(&ctx));

        ctx.props.add(provider);
        let val = assert_after_add(&ctx, key);

        let got = val
            .as_any()
            .downcast_ref::<InfoSchemaPropProvider>()
            .expect("provider must keep its concrete type");
        assert!(Arc::ptr_eq(&is1, &got.call(true)));
        assert!(Arc::ptr_eq(&is2, &got.call(false)));

        assert!(Arc::ptr_eq(
            &is1,
            &assert_reader_func_value(reader.get_latest_info_schema(&ctx))
        ));
        assert!(Arc::ptr_eq(
            &is2,
            &assert_reader_func_value(reader.get_session_info_schema(&ctx))
        ));
    }

    // OptPropKVStore.
    {
        let key = OptionalEvalPropKey::KvStore;
        let store: Arc<dyn KvStorage> = Arc::new(MockKvStore);
        let captured = Arc::clone(&store);
        let provider = Arc::new(KvStorePropProvider::new(move || Arc::clone(&captured)));
        let reader = KvStorePropReader;

        assert_before_add(
            &ctx,
            key,
            provider.as_ref(),
            reader.required_optional_eval_props(),
        );
        assert_reader_func_return_err(reader.get_kv_store(&ctx));

        ctx.props.add(provider);
        let val = assert_after_add(&ctx, key);

        let got = val
            .as_any()
            .downcast_ref::<KvStorePropProvider>()
            .expect("provider must keep its concrete type");
        assert!(Arc::ptr_eq(&store, &got.call()));
        assert!(Arc::ptr_eq(
            &store,
            &assert_reader_func_value(reader.get_kv_store(&ctx))
        ));
    }

    // OptPropSQLExecutor.
    {
        let key = OptionalEvalPropKey::SqlExecutor;
        let executor: Arc<dyn SqlExecutor> = Arc::new(MockSqlExecutor);
        let mock_err: Arc<Mutex<Option<ExprOptError>>> = Arc::new(Mutex::new(None));
        let (captured, captured_err) = (Arc::clone(&executor), Arc::clone(&mock_err));
        let provider = Arc::new(SqlExecutorPropProvider::new(move || {
            match captured_err.lock().expect("not poisoned").clone() {
                Some(err) => Err(err),
                None => Ok(Arc::clone(&captured)),
            }
        }));
        let reader = SqlExecutorPropReader;

        assert_before_add(
            &ctx,
            key,
            provider.as_ref(),
            reader.required_optional_eval_props(),
        );
        assert_reader_func_return_err(reader.get_sql_executor(&ctx));

        ctx.props.add(provider);
        let val = assert_after_add(&ctx, key);
        let got = val
            .as_any()
            .downcast_ref::<SqlExecutorPropProvider>()
            .expect("provider must keep its concrete type");

        assert!(Arc::ptr_eq(&executor, &got.call().expect("no error yet")));

        set_mock_err(&mock_err, Some("mockErr1"));
        assert_eq!(provider_err_message(got.call()), "mockErr1");

        set_mock_err(&mock_err, None);
        assert!(Arc::ptr_eq(
            &executor,
            &assert_reader_func_value(reader.get_sql_executor(&ctx))
        ));

        set_mock_err(&mock_err, Some("mockErr2"));
        assert_eq!(
            provider_err_message(reader.get_sql_executor(&ctx)),
            "mockErr2"
        );
        set_mock_err(&mock_err, None);
    }

    // OptPropSequenceOperator.
    {
        let key = OptionalEvalPropKey::SequenceOperator;
        let operator: Arc<dyn SequenceOperator> = Arc::new(MockSequenceOperator);
        let mock_err: Arc<Mutex<Option<ExprOptError>>> = Arc::new(Mutex::new(None));
        let (captured, captured_err) = (Arc::clone(&operator), Arc::clone(&mock_err));
        let provider = Arc::new(SequenceOperatorProvider::new(move |db, name| {
            assert_eq!(db, "db1");
            assert_eq!(name, "name1");
            match captured_err.lock().expect("not poisoned").clone() {
                Some(err) => Err(err),
                None => Ok(Arc::clone(&captured)),
            }
        }));
        let reader = SequenceOperatorPropReader;

        assert_before_add(
            &ctx,
            key,
            provider.as_ref(),
            reader.required_optional_eval_props(),
        );
        assert_reader_func_return_err(reader.get_sequence_operator(&ctx, "db1", "name1"));

        ctx.props.add(provider);
        let val = assert_after_add(&ctx, key);
        let got = val
            .as_any()
            .downcast_ref::<SequenceOperatorProvider>()
            .expect("provider must keep its concrete type");

        assert!(Arc::ptr_eq(
            &operator,
            &got.call("db1", "name1").expect("no error yet")
        ));

        set_mock_err(&mock_err, Some("mockErr1"));
        assert_eq!(provider_err_message(got.call("db1", "name1")), "mockErr1");

        set_mock_err(&mock_err, None);
        assert!(Arc::ptr_eq(
            &operator,
            &assert_reader_func_value(reader.get_sequence_operator(&ctx, "db1", "name1"))
        ));

        set_mock_err(&mock_err, Some("mockErr2"));
        assert_eq!(
            provider_err_message(reader.get_sequence_operator(&ctx, "db1", "name1")),
            "mockErr2"
        );
        set_mock_err(&mock_err, None);
    }

    // OptPropAdvisoryLock.
    {
        let key = OptionalEvalPropKey::AdvisoryLock;
        let lock_ctx: Arc<dyn AdvisoryLockContext> = Arc::new(MockAdvisoryLockContext);
        let provider = Arc::new(AdvisoryLockPropProvider::new(Arc::clone(&lock_ctx)));
        let reader = AdvisoryLockPropReader;

        assert_before_add(
            &ctx,
            key,
            provider.as_ref(),
            reader.required_optional_eval_props(),
        );
        assert_reader_func_return_err(reader.advisory_lock_ctx(&ctx));

        let stored: Arc<dyn DynOptionalEvalPropProvider> = Arc::clone(&provider) as _;
        ctx.props.add(stored);
        let val = assert_after_add(&ctx, key);

        let got = val
            .as_any()
            .downcast_ref::<AdvisoryLockPropProvider>()
            .expect("provider must keep its concrete type");
        assert!(Arc::ptr_eq(&lock_ctx, got.advisory_lock_context()));
        assert!(Arc::ptr_eq(
            &provider,
            &assert_reader_func_value(reader.advisory_lock_ctx(&ctx))
        ));
    }

    // OptPropDDLOwnerInfo.
    {
        let key = OptionalEvalPropKey::DdlOwnerInfo;
        let is_owner = Arc::new(AtomicBool::new(false));
        let captured = Arc::clone(&is_owner);
        let provider = Arc::new(DdlOwnerInfoProvider::new(move || {
            captured.load(Ordering::SeqCst)
        }));
        let reader = DdlOwnerPropReader;

        assert_before_add(
            &ctx,
            key,
            provider.as_ref(),
            reader.required_optional_eval_props(),
        );
        assert_reader_func_return_err(reader.is_ddl_owner(&ctx));

        ctx.props.add(provider);
        let val = assert_after_add(&ctx, key);
        let got = val
            .as_any()
            .downcast_ref::<DdlOwnerInfoProvider>()
            .expect("provider must keep its concrete type");

        is_owner.store(true, Ordering::SeqCst);
        assert!(got.call());
        is_owner.store(false, Ordering::SeqCst);
        assert!(!got.call());

        is_owner.store(true, Ordering::SeqCst);
        assert!(assert_reader_func_value(reader.is_ddl_owner(&ctx)));
        is_owner.store(false, Ordering::SeqCst);
        assert!(!assert_reader_func_value(reader.is_ddl_owner(&ctx)));
    }

    // OptPropPrivilegeChecker.
    {
        let key = OptionalEvalPropKey::PrivilegeChecker;
        let checker: Arc<dyn PrivilegeChecker> = Arc::new(MockPrivilegeChecker);
        let captured = Arc::clone(&checker);
        let provider = Arc::new(PrivilegeCheckerProvider::new(move || Arc::clone(&captured)));
        let reader = PrivilegeCheckerPropReader;

        assert_before_add(
            &ctx,
            key,
            provider.as_ref(),
            reader.required_optional_eval_props(),
        );
        assert_reader_func_return_err(reader.get_privilege_checker(&ctx));

        ctx.props.add(provider);
        let val = assert_after_add(&ctx, key);
        let got = val
            .as_any()
            .downcast_ref::<PrivilegeCheckerProvider>()
            .expect("provider must keep its concrete type");

        assert!(Arc::ptr_eq(
            &checker,
            &assert_reader_func_value(reader.get_privilege_checker(&ctx))
        ));
        assert!(Arc::ptr_eq(&checker, &got.call()));
    }

    // Every key is now provided.
    assert!(ctx.props.prop_key_set().is_full());
}
