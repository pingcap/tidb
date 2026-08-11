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

//! Public semantic contract for Go `pkg/util/context`.

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tidb_error::mysql::FormatArg;
use tidb_error::terror::ERR_RESULT_UNDETERMINED;
use tidb_util::context::{
    gen_context_id, FuncWarnAppender, IgnoreWarn, PlanCacheTracker, PlanCacheType,
    RangeFallbackHandler, SqlWarn, StaticWarnHandler, ValueStoreContext, WarnAppender, WarnErr,
    WarnHandler, WarnHandlerExt, MAX_WARNING_COUNT, WARN_LEVEL_ERROR, WARN_LEVEL_NOTE,
    WARN_LEVEL_WARNING,
};

fn warning(level: &str, message: &str) -> SqlWarn {
    SqlWarn {
        level: level.to_owned(),
        err: WarnErr::from(message),
    }
}

#[test]
fn warning_json_accepts_the_sources_empty_level() {
    for (input, message) in [
        (r#"{"level":"","msg":"empty"}"#, "empty"),
        (r#"{"msg":"missing"}"#, "missing"),
    ] {
        let warning: SqlWarn = serde_json::from_str(input).expect("Go accepts an empty level");
        assert!(warning.level.is_empty());
        assert_eq!(warning.err.to_string(), message);
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum ContextKey {
    Query,
    Initing,
}

#[derive(Default)]
struct Store {
    values: HashMap<ContextKey, Box<dyn Any>>,
    domain: Option<Box<dyn Any>>,
}

impl ValueStoreContext for Store {
    type Key = ContextKey;

    fn set_value(&mut self, key: &Self::Key, value: Box<dyn Any>) {
        self.values.insert(*key, value);
    }

    fn value(&self, key: &Self::Key) -> Option<&dyn Any> {
        self.values.get(key).map(Box::as_ref)
    }

    fn clear_value(&mut self, key: &Self::Key) {
        self.values.remove(key);
    }

    fn get_domain(&self) -> Option<&dyn Any> {
        self.domain.as_deref()
    }
}

#[test]
fn value_store_uses_a_typed_key_domain_and_exposes_its_domain() {
    let mut store = Store {
        domain: Some(Box::new(String::from("domain"))),
        ..Store::default()
    };
    store.set_value(&ContextKey::Query, Box::new(String::from("select 1")));
    store.set_value(&ContextKey::Initing, Box::new(true));

    assert_eq!(
        store
            .value(&ContextKey::Query)
            .and_then(|value| value.downcast_ref::<String>()),
        Some(&String::from("select 1"))
    );
    assert_eq!(
        store
            .value(&ContextKey::Initing)
            .and_then(|value| value.downcast_ref::<bool>()),
        Some(&true)
    );
    assert_eq!(
        store
            .get_domain()
            .and_then(|domain| domain.downcast_ref::<String>())
            .map(String::as_str),
        Some("domain")
    );

    store.clear_value(&ContextKey::Query);
    assert!(store.value(&ContextKey::Query).is_none());
    assert!(store.value(&ContextKey::Initing).is_some());
}

#[test]
fn context_ids_are_nonzero_and_unique_across_threads() {
    let first = gen_context_id();
    let second = gen_context_id();
    assert!(first > 0);
    assert!(second > first);

    let mut ids = (0..64)
        .map(|_| std::thread::spawn(gen_context_id))
        .map(|thread| thread.join().expect("ID worker"))
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), 64);
    assert!(ids[0] > second);
}

#[test]
fn warning_json_and_handlers_preserve_the_public_contract() {
    let terror = ERR_RESULT_UNDETERMINED.fast_generate(
        ERR_RESULT_UNDETERMINED.message(),
        &[FormatArg::from("unknown")],
    );
    let original = vec![
        warning(WARN_LEVEL_ERROR, "plain"),
        SqlWarn {
            level: WARN_LEVEL_WARNING.to_owned(),
            err: WarnErr::Terror(terror),
        },
        warning(WARN_LEVEL_NOTE, "EOF"),
    ];
    let encoded = serde_json::to_string(&original).expect("serialize warnings");
    let decoded: Vec<SqlWarn> = serde_json::from_str(&encoded).expect("deserialize warnings");
    assert_eq!(decoded.len(), original.len());
    for (before, after) in original.iter().zip(&decoded) {
        assert_eq!(after.level, before.level);
        assert_eq!(after.err.to_string(), before.err.to_string());
    }

    let handler = StaticWarnHandler::new(4);
    handler.append_warning(WarnErr::from("w0"));
    handler.append_note(WarnErr::from("n0"));
    handler.append_error(WarnErr::from("e0"));
    assert_eq!(handler.num_error_warnings(), (1, 3));
    let copied = StaticWarnHandler::with_handler(Some(&handler));
    assert_eq!(copied.warning_count(), 3);
    assert_eq!(copied.truncate_warnings(1).len(), 2);
    assert_eq!(copied.warning_count(), 1);
    copied.reset();
    assert_eq!(copied.warning_count(), 0);

    let calls = Arc::new(Mutex::new(Vec::new()));
    let captured = calls.clone();
    let appender = FuncWarnAppender::new(move |level: &str, err: WarnErr| {
        captured
            .lock()
            .unwrap()
            .push((level.to_owned(), err.to_string()));
    });
    appender.append_warning(WarnErr::from("function warning"));
    appender.append_note(WarnErr::from("function note"));
    assert_eq!(calls.lock().unwrap().len(), 2);
    assert_eq!(IgnoreWarn.warning_count(), 0);
}

#[test]
fn warning_retention_matches_single_and_batch_append_rules() {
    let handler = StaticWarnHandler::new(0);
    handler.set_warnings(vec![
        warning(WARN_LEVEL_WARNING, "old");
        MAX_WARNING_COUNT - 1
    ]);
    handler.append_warning(WarnErr::from("last"));
    handler.append_warning(WarnErr::from("dropped"));
    assert_eq!(handler.warning_count(), MAX_WARNING_COUNT);
    assert_eq!(
        handler.get_warnings().last().unwrap().err.to_string(),
        "last"
    );

    handler.set_warnings(vec![
        warning(WARN_LEVEL_ERROR, "old");
        MAX_WARNING_COUNT - 1
    ]);
    handler.append_warnings(vec![
        warning(WARN_LEVEL_ERROR, "batch 1"),
        warning(WARN_LEVEL_ERROR, "batch 2"),
    ]);
    assert_eq!(handler.warning_count(), MAX_WARNING_COUNT + 1);
    assert_eq!(handler.num_error_warnings(), (0, MAX_WARNING_COUNT + 1));
}

#[test]
fn plan_cache_and_range_fallback_publish_source_ordered_decisions() {
    let warnings = Arc::new(StaticWarnHandler::new(0));
    let tracker = Arc::new(PlanCacheTracker::new(warnings.clone()));

    tracker.set_skip_plan_cache("disabled");
    assert!(!tracker.use_cache());
    assert_eq!(warnings.warning_count(), 0);

    tracker.enable_plan_cache();
    tracker.set_cache_type(PlanCacheType::SessionPrepared);
    tracker.set_skip_plan_cache("has sub-queries");
    assert!(!tracker.use_cache());
    assert_eq!(tracker.plan_cache_unqualified(), "has sub-queries");
    assert_eq!(
        warnings.get_warnings().last().unwrap().err.to_string(),
        "skip prepared plan-cache: has sub-queries"
    );

    tracker.enable_plan_cache();
    tracker.set_force_plan_cache(true);
    tracker.set_skip_plan_cache("risky");
    assert!(tracker.use_cache());
    assert_eq!(
        warnings.get_warnings().last().unwrap().err.to_string(),
        "force plan-cache: may use risky cached plan: risky"
    );

    tracker.set_force_plan_cache(false);
    tracker.set_cache_type(PlanCacheType::SessionNonPrepared);
    tracker.set_skip_plan_cache("quiet");
    let quiet_count = warnings.warning_count();
    tracker.enable_plan_cache();
    tracker.set_always_warn_skip_cache(true);
    tracker.set_skip_plan_cache("loud");
    assert_eq!(warnings.warning_count(), quiet_count + 1);

    let saved = tracker.save();
    tracker.restore(
        true,
        PlanCacheType::SessionPrepared,
        "restored".into(),
        true,
        false,
    );
    assert!(tracker.use_cache());
    assert_eq!(tracker.plan_cache_unqualified(), "restored");
    tracker.restore(saved.0, saved.1, saved.2.clone(), saved.3, saved.4);
    assert_eq!(tracker.plan_cache_unqualified(), saved.2);

    tracker.enable_plan_cache();
    tracker.set_cache_type(PlanCacheType::SessionPrepared);
    let fallback = RangeFallbackHandler::new(tracker.clone(), warnings.clone());
    fallback.record_range_fallback(1024);
    fallback.record_range_fallback(1024);
    let fallback_warnings = warnings
        .get_warnings()
        .into_iter()
        .filter(|warning| warning.err.to_string().contains("tidb_opt_range_max_size"))
        .count();
    assert_eq!(fallback_warnings, 1);
    assert!(!tracker.use_cache());
    assert_eq!(tracker.plan_cache_unqualified(), "in-list is too long");
}
