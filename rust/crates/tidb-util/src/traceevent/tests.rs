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

//! The package's tests. Go's `TestSuite` exists because several tests share
//! process-wide state; Rust runs tests in parallel, so every test that touches
//! the mode flags, the global sinks, or the global recorder takes
//! [`GLOBAL_STATE`] first.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use super::*;
use crate::tracing::Phase;

static GLOBAL_STATE: Mutex<()> = Mutex::new(());

fn lock_global_state() -> MutexGuard<'static, ()> {
    GLOBAL_STATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn all_categories_config() -> FlightRecorderConfig {
    let mut config = FlightRecorderConfig::default();
    config.initialize();
    config.enabled_categories = vec!["*".to_owned()];
    config
}

/// Go `installRecorderSink`. The caller restores the previous sink.
fn install_recorder_sink(capacity: usize) -> (Arc<RingBufferSink>, Arc<dyn Sink>) {
    let recorder = Arc::new(RingBufferSink::new(capacity));
    let previous = current_sink();
    set_sink(Some(Arc::clone(&recorder) as Arc<dyn Sink>));
    (recorder, previous)
}

fn extract_names(events: &[Event]) -> Vec<String> {
    events.iter().map(|event| event.name.clone()).collect()
}

fn int_field(key: &str, value: i64) -> Field {
    Field::new(key, Value::I64(value))
}

// Go `TestSuite`. The six subtests run in order, under one lock, exactly as
// Go's `t.Run` sequence does.
#[test]
fn suite() {
    let _guard = lock_global_state();
    trace_event_categories();
    trace_event_category_filtering();
    trace_event_records_event();
    trace_event_carries_trace_id();
    trace_event_logging_switch();
    flight_recorder_cooling_off();
}

// Go `testTraceEventCategories`.
fn trace_event_categories() {
    start_log_flight_recorder(all_categories_config()).unwrap();
    let fr = get_flight_recorder().unwrap();

    assert!(is_enabled(TXN_LIFECYCLE));

    fr.disable(TXN_LIFECYCLE);
    assert!(!is_enabled(TXN_LIFECYCLE));

    fr.enable(TXN_LIFECYCLE);
    assert!(is_enabled(TXN_LIFECYCLE));

    fr.set_categories(TraceCategory(0));
    assert!(!is_enabled(TXN_LIFECYCLE));
    fr.set_categories(ALL_CATEGORIES);
    assert!(is_enabled(TXN_LIFECYCLE));

    fr.close();
}

// Go `testTraceEventCategoryFiltering`.
fn trace_event_category_filtering() {
    start_log_flight_recorder(all_categories_config()).unwrap();
    let fr = get_flight_recorder().unwrap();

    fr.set_categories(TraceCategory(0));
    set_mode(MODE_FULL).unwrap();
    flight_recorder().discard_or_flush();
    let (recorder, previous) = install_recorder_sink(8);

    let ctx = TraceContext::background();
    trace_event(
        &ctx,
        TXN_LIFECYCLE,
        "should-not-record",
        vec![int_field("value", 1)],
    );
    assert!(flight_recorder().snapshot().is_empty());
    assert!(recorder.snapshot().is_empty());

    set_sink(Some(previous));
    fr.close();
}

// Go `testTraceEventRecordsEvent`.
fn trace_event_records_event() {
    start_log_flight_recorder(all_categories_config()).unwrap();
    let fr = get_flight_recorder().unwrap();

    fr.set_categories(ALL_CATEGORIES);
    set_mode(MODE_FULL).unwrap();
    flight_recorder().discard_or_flush();
    let (recorder, previous) = install_recorder_sink(8);
    let ctx = TraceContext::background();

    trace_event(
        &ctx,
        TXN_LIFECYCLE,
        "test-event",
        vec![
            int_field("count", 42),
            Field::new("scope", Value::Str("unit-test".to_owned())),
        ],
    );

    let events = flight_recorder().snapshot();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].category, TXN_LIFECYCLE);
    assert_eq!(events[0].name, "test-event");
    assert!(events[0].timestamp > std::time::UNIX_EPOCH);
    assert_eq!(events[0].fields.len(), 2);

    let recorded = recorder.snapshot();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].name, "test-event");
    assert_eq!(recorded[0].fields.len(), 2);

    set_sink(Some(previous));
    fr.close();
}

// Go `testTraceEventCarriesTraceID`.
fn trace_event_carries_trace_id() {
    start_log_flight_recorder(all_categories_config()).unwrap();
    let fr = get_flight_recorder().unwrap();

    fr.set_categories(ALL_CATEGORIES);
    set_mode(MODE_FULL).unwrap();
    flight_recorder().discard_or_flush();

    let raw_trace = [0x01_u8, 0x10, 0xFE, 0xAA];
    let ctx = context_with_trace_id(&TraceContext::background(), &raw_trace);
    trace_event(&ctx, TXN_2PC, "trace-id-check", vec![int_field("value", 7)]);

    let events = flight_recorder().snapshot();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].trace_id, raw_trace);

    fr.close();
}

// Go `testTraceEventLoggingSwitch`.
fn trace_event_logging_switch() {
    start_log_flight_recorder(all_categories_config()).unwrap();
    let fr = get_flight_recorder().unwrap();

    fr.set_categories(ALL_CATEGORIES);
    set_mode(MODE_BASE).unwrap();
    flight_recorder().discard_or_flush();
    let (recorder, previous) = install_recorder_sink(8);
    set_mode(MODE_BASE).unwrap();
    let ctx = TraceContext::background();

    let flight_before = flight_recorder().snapshot().len();

    assert_eq!(current_mode(), MODE_BASE);
    trace_event(
        &ctx,
        TXN_LIFECYCLE,
        "disabled-log",
        vec![int_field("value", 1)],
    );
    assert_eq!(flight_recorder().snapshot().len(), flight_before + 1);
    let disabled_logged = recorder.snapshot().len();

    set_mode(MODE_FULL).unwrap();
    trace_event(
        &ctx,
        TXN_LIFECYCLE,
        "enabled-log",
        vec![int_field("value", 2)],
    );
    assert_eq!(flight_recorder().snapshot().len(), flight_before + 2);
    let recorded = recorder.snapshot();
    assert_eq!(recorded.len(), disabled_logged + 1);
    assert_eq!(recorded[recorded.len() - 1].name, "enabled-log");

    set_sink(Some(previous));
    fr.close();
}

// Go `testFlightRecorderCoolingOff`.
fn flight_recorder_cooling_off() {
    let previous_mode = current_mode();
    start_log_flight_recorder(all_categories_config()).unwrap();
    let fr = get_flight_recorder().unwrap();

    fr.set_categories(ALL_CATEGORIES);
    set_mode(MODE_FULL).unwrap();
    flight_recorder().discard_or_flush();
    LAST_DUMP_TIME.store(0, Ordering::SeqCst);

    let ctx = TraceContext::background();
    trace_event(
        &ctx,
        TXN_LIFECYCLE,
        "cooloff-test-event",
        vec![int_field("value", 1)],
    );

    // First dump should succeed.
    dump_flight_recorder_to_logger("test-reason-1");
    let first = LAST_DUMP_TIME.load(Ordering::SeqCst);
    assert!(first > 0);

    // Immediate second dump should be suppressed (in cooling-off period).
    dump_flight_recorder_to_logger("test-reason-2");
    assert_eq!(
        LAST_DUMP_TIME.load(Ordering::SeqCst),
        first,
        "timestamp should not update during cooling-off"
    );

    // Simulate passage of time by setting lastDumpTime to 11 seconds ago.
    let eleven_seconds_ago = unix_seconds(SystemTime::now()) - 11;
    LAST_DUMP_TIME.store(eleven_seconds_ago, Ordering::SeqCst);

    // Third dump should succeed (outside cooling-off period).
    dump_flight_recorder_to_logger("test-reason-3");
    assert!(
        LAST_DUMP_TIME.load(Ordering::SeqCst) > eleven_seconds_ago,
        "timestamp should update after cooling-off period"
    );

    fr.close();
    set_mode(previous_mode).unwrap();
    flight_recorder().discard_or_flush();
    LAST_DUMP_TIME.store(0, Ordering::SeqCst);
}

// Go `TestTraceEventModes`.
#[test]
fn trace_event_modes() {
    let _guard = lock_global_state();
    let previous = current_mode();

    assert_eq!(set_mode("base").unwrap(), MODE_BASE);
    assert_eq!(current_mode(), MODE_BASE);

    assert_eq!(set_mode("full").unwrap(), MODE_FULL);
    assert_eq!(current_mode(), MODE_FULL);

    assert_eq!(set_mode("off").unwrap(), MODE_OFF);
    assert_eq!(current_mode(), MODE_OFF);

    assert!(normalize_mode("invalid").is_err());

    set_mode(previous).unwrap();
}

// Go `TestRingBufferSnapshotOrder`.
#[test]
fn ring_buffer_snapshot_order() {
    let recorder = RingBufferSink::new(2);

    let event = |name: &str, nanos: u64| Event {
        category: TXN_LIFECYCLE,
        name: name.to_owned(),
        phase: Phase::Instant,
        timestamp: UNIX_EPOCH + Duration::from_nanos(nanos),
        trace_id: Vec::new(),
        fields: Vec::new(),
    };

    let context = TraceContext::background();
    recorder.record(&context, &event("first", 1));
    recorder.record(&context, &event("second", 2));
    assert_eq!(extract_names(&recorder.snapshot()), ["first", "second"]);

    recorder.record(&context, &event("third", 3));
    assert_eq!(extract_names(&recorder.snapshot()), ["second", "third"]);
}

// Go `TestRingBufferFlushTo`.
#[test]
fn ring_buffer_flush_to() {
    let recorder = RingBufferSink::new(4);
    let event = Event {
        category: TXN_LIFECYCLE,
        name: "flush".to_owned(),
        phase: Phase::Instant,
        // 123456 microseconds
        timestamp: UNIX_EPOCH + Duration::from_nanos(123_456_000),
        trace_id: Vec::new(),
        fields: vec![
            Field::new("status", Value::Str("ok".to_owned())),
            int_field("count", 2),
        ],
    };
    recorder.record(&TraceContext::background(), &event);

    let events = recorder.snapshot();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].name, "flush");
    assert_eq!(events[0].category, TXN_LIFECYCLE);
    assert_eq!(events[0].timestamp, event.timestamp);
    assert_eq!(events[0].fields.len(), 2);
}

// Go `TestCategoryNames`.
#[test]
fn category_names() {
    let cases = [
        (TXN_LIFECYCLE, "txn_lifecycle"),
        (TXN_2PC, "txn_2pc"),
        (TXN_LOCK_RESOLVE, "txn_lock_resolve"),
        (STMT_LIFECYCLE, "stmt_lifecycle"),
        (STMT_PLAN, "stmt_plan"),
        (KV_REQUEST, "kv_request"),
        (UNKNOWN_CLIENT, "unknown_client"),
        (TraceCategory(999), "unknown(999)"),
    ];
    for (category, name) in cases {
        assert_eq!(category.name(), name);
    }
}

// Go `TestParseTraceCategory`.
#[test]
fn parse_trace_category() {
    let strings = |names: &[&str]| names.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>();
    let cases = [
        (strings(&["*"]), ALL_CATEGORIES),
        (
            strings(&["-", "general"]),
            TraceCategory(ALL_CATEGORIES.0 & !GENERAL.0),
        ),
        (strings(&["txn_2pc"]), TXN_2PC),
        (
            strings(&["txn_2pc", "stmt_plan", "non_exist"]),
            TXN_2PC | STMT_PLAN,
        ),
        (strings(&["non_exist"]), TraceCategory(0)),
    ];
    for (idx, (input, expect)) in cases.into_iter().enumerate() {
        assert_eq!(parse_categories(&input), expect, "case {idx}");
    }
}

// Go `TestAndOrCombination`.
#[test]
fn and_or_combination() {
    let mut compiled = CompiledDumpTriggerConfig::default();
    let a = compiled.add_trigger("A".to_owned(), None).unwrap();
    let b = compiled.add_trigger("B".to_owned(), None).unwrap();
    let c = compiled.add_trigger("C".to_owned(), None).unwrap();
    let d = compiled.add_trigger("D".to_owned(), None).unwrap();

    let table = truth_table_for_and(vec![a], vec![b, c]);
    assert!(!check_truth_table(a, &table));
    assert!(!check_truth_table(d, &table));
    assert!(check_truth_table(a | b, &table));
    assert!(check_truth_table(a | c, &table));
    assert!(!check_truth_table(b | c, &table));

    let table = truth_table_for_and(vec![a | b], vec![c]);
    assert!(!check_truth_table(a, &table));
    assert!(!check_truth_table(d, &table));
    assert!(!check_truth_table(a | b, &table));
    assert!(!check_truth_table(a | c, &table));
    assert!(!check_truth_table(b | c, &table));
    assert!(check_truth_table(a | b | c, &table));
    assert!(check_truth_table(a | b | c | d, &table));

    let table = truth_table_for_and(vec![a, b], vec![c, d]);
    assert!(!check_truth_table(a, &table));
    assert!(!check_truth_table(b, &table));
    assert!(!check_truth_table(c, &table));
    assert!(!check_truth_table(d, &table));
    assert!(!check_truth_table(a | b, &table));
    assert!(!check_truth_table(c | d, &table));
    assert!(check_truth_table(a | c, &table));
    assert!(check_truth_table(b | c, &table));
    assert!(check_truth_table(b | d, &table));
    assert!(check_truth_table(b | c | d, &table));
    assert!(check_truth_table(a | b | c | d, &table));

    let table = truth_table_for_or(vec![a, b], vec![c | d]);
    assert!(check_truth_table(a, &table));
    assert!(check_truth_table(b, &table));
    assert!(!check_truth_table(c, &table));
    assert!(!check_truth_table(d, &table));
    assert!(check_truth_table(a | d, &table));
    assert!(check_truth_table(a | b, &table));

    let table = truth_table_for_or(vec![a | c], vec![b | d]);
    assert!(!check_truth_table(a, &table));
    assert!(!check_truth_table(b, &table));
    assert!(!check_truth_table(c, &table));
    assert!(!check_truth_table(d, &table));
    assert!(!check_truth_table(a | d, &table));
    assert!(check_truth_table(a | c, &table));
    assert!(!check_truth_table(b | c, &table));
    assert!(check_truth_table(b | d, &table));
    assert!(!check_truth_table(c | d, &table));
    assert!(check_truth_table(a | c | d, &table));
}

// Go `TestFlightRecorderConfig` = `testFlightRecorderConfigGoodCase` plus
// `testFlightRecorderConfigBadCase`.
#[test]
fn flight_recorder_config() {
    flight_recorder_config_good_case();
    flight_recorder_config_bad_case();
}

fn mapping(entries: &[(&str, usize)]) -> HashMap<String, usize> {
    entries
        .iter()
        .map(|(name, idx)| ((*name).to_owned(), *idx))
        .collect()
}

fn flight_recorder_config_good_case() {
    let conf1 = r#"{
  "enabled_categories": [
    "txn_2pc",
    "stmt_plan"
  ],
  "dump_trigger": {
    "type": "sampling",
    "sampling": 100
  }
}"#;
    let name1 = "dump_trigger.sampling";

    let conf2 = r#"{
  "enabled_categories": [
    "*"
  ],
  "dump_trigger": {
    "type": "sampling",
    "sampling": 1
  }
}"#;
    let name2 = "dump_trigger.sampling";

    let conf3 = r#"{
  "enabled_categories": [
    "general"
  ],
  "dump_trigger": {
    "type": "user_command",
    "user_command": {
      "type": "sql_regexp",
      "sql_regexp": "^select"
    }
  }
}"#;
    let name3 = "dump_trigger.user_command.sql_regexp";

    let conf4 = r#"{
  "enabled_categories": [
    "*"
  ],
  "dump_trigger": {
    "type": "user_command",
    "user_command": {
      "type": "stmt_label",
      "stmt_label": "CreateTable"
    }
  }
}"#;
    let name4 = "dump_trigger.user_command.stmt_label";

    let conf5 = conf3;
    let name5 = name3;

    let conf6 = r#"{
  "enabled_categories": [
    "*"
  ],
  "dump_trigger": {
    "type": "suspicious_event",
    "suspicious_event": {
      "type": "slow_query"
    }
  }
}"#;
    let name6 = "dump_trigger.suspicious_event";

    let conf7 = r#"{
  "enabled_categories": [
    "*"
  ],
  "dump_trigger": {
    "type": "suspicious_event",
    "suspicious_event": {
      "type": "region_error"
    }
  }
}"#;
    let name7 = "dump_trigger.suspicious_event";

    let conf8 = r#"{
  "enabled_categories": [
    "*"
  ],
  "dump_trigger": {
    "type": "and",
    "and": [
      {
        "type": "user_command",
        "user_command": {
          "type": "stmt_label",
          "stmt_label": "Select"
        }
      },
      {
        "type": "suspicious_event",
        "suspicious_event": {
          "type": "resolve_lock"
        }
      }
    ]
  }
}"#;

    let conf9 = r#"{
  "enabled_categories": [
    "*"
  ],
  "dump_trigger": {
    "type": "or",
    "or": [
      {
        "type": "and",
        "and": [
          {
            "type": "user_command",
            "user_command": {
              "type": "stmt_label",
              "stmt_label": "Insert"
            }
          },
          {
            "type": "suspicious_event",
            "suspicious_event": {
              "type": "query_fail"
            }
          }
        ]
      },
      {
        "type": "sampling",
        "sampling": 10
      }
    ]
  }
}"#;

    let conf10 = r#"{
  "enabled_categories": [
    "*"
  ],
  "dump_trigger": {
    "type": "suspicious_event",
    "suspicious_event": {
      "type": "is_internal",
      "is_internal": true
    }
  }
}"#;
    let name10 = "dump_trigger.suspicious_event.is_internal";

    let conf11 = r#"{
  "enabled_categories": [
    "*"
  ],
  "dump_trigger": {
    "type": "suspicious_event",
    "suspicious_event": {
      "type": "dev_debug",
      "dev_debug": {
        "type": "execute_internal_trace_missing"
      }
    }
  }
}"#;
    let name11 = "dump_trigger.suspicious_event.dev_debug";

    let testcases = [
        (conf1, mapping(&[(name1, 0)])),
        (conf2, mapping(&[(name2, 0)])),
        (conf3, mapping(&[(name3, 0)])),
        (conf4, mapping(&[(name4, 0)])),
        (conf5, mapping(&[(name5, 0)])),
        (conf6, mapping(&[(name6, 0)])),
        (conf7, mapping(&[(name7, 0)])),
        (
            conf8,
            mapping(&[
                ("dump_trigger.user_command.stmt_label", 0),
                ("dump_trigger.suspicious_event", 1),
            ]),
        ),
        (
            conf9,
            mapping(&[
                ("dump_trigger.user_command.stmt_label", 0),
                ("dump_trigger.suspicious_event", 1),
                ("dump_trigger.sampling", 2),
            ]),
        ),
        (conf10, mapping(&[(name10, 0)])),
        (conf11, mapping(&[(name11, 0)])),
    ];

    for (idx, (conf, expected)) in testcases.into_iter().enumerate() {
        // unmarshal success
        let value: FlightRecorderConfig = serde_json::from_str(conf).unwrap_or_else(|error| {
            panic!("case {idx}: {error}");
        });
        // compile success
        let res = value
            .compile()
            .unwrap_or_else(|error| panic!("case {idx}: {error}"));
        // result expected
        assert_eq!(res.name_mapping, expected, "case {idx}");
    }
}

fn flight_recorder_config_bad_case() {
    let badcase_json_decode = r#""enabled_categories": ["*"],
	"dump_trigger": {
	"type": "sampling",
	"sampling": 5,
	}"#;
    let badcase_validate = r#"{
  "enabled_categories": [
    "txn_2pc",
    "stmt_plan"
  ],
  "dump_trigger": {
    "type": "user_command",
    "sampling": 5
  }
}"#;
    let badcase_validate1 = r#"{
  "enabled_categories": [
    "sdaf"
  ],
  "dump_trigger": {
    "type": "user_command",
    "user_command": {
      "type": "non_exist",
      "sql_regexp": "^select"
    }
  }
}"#;
    let badcase_duplicated = r#"{
  "enabled_categories": [
    "*"
  ],
  "dump_trigger": {
    "type": "and",
    "and": [
      {
        "type": "suspicious_event",
        "suspicious_event": {
          "type": "slow_query"
        }
      },
      {
        "type": "suspicious_event",
        "suspicious_event": {
          "type": "query_fail"
        }
      }
    ]
  }
}"#;

    // errkind 1: the JSON itself is invalid; errkind 2: compilation fails.
    let badcases = [
        (badcase_json_decode, 1),
        (badcase_validate, 2),
        (badcase_validate1, 2),
        (badcase_duplicated, 2),
    ];

    for (idx, (conf, errkind)) in badcases.into_iter().enumerate() {
        let parsed = serde_json::from_str::<FlightRecorderConfig>(conf);
        if errkind == 1 {
            assert!(parsed.is_err(), "case {idx}");
            continue;
        }
        let value = parsed.unwrap_or_else(|error| panic!("case {idx}: {error}"));
        assert!(value.compile().is_err(), "case {idx}");
    }
}

// Go `TestCategoryParsing`.
#[test]
fn category_parsing() {
    for (name, category) in [
        ("tikv_request", TraceCategory::TIKV_REQUEST),
        ("tikv_write_details", TraceCategory::TIKV_WRITE_DETAILS),
        ("tikv_read_details", TraceCategory::TIKV_READ_DETAILS),
    ] {
        assert_eq!(TraceCategory::parse(name), category);
        assert_eq!(category.name(), name);
    }
}

// Go `TestDefaultConfiguration`.
#[test]
fn default_configuration() {
    let mut config = FlightRecorderConfig::default();
    config.initialize();

    let categories = parse_categories(&config.enabled_categories);

    assert!(
        categories.0 & TraceCategory::TIKV_REQUEST.0 != 0,
        "default should include tikv_request"
    );
    assert!(
        categories.0 & TraceCategory::TIKV_WRITE_DETAILS.0 == 0,
        "default should exclude tikv_write_details"
    );
    assert!(
        categories.0 & TraceCategory::TIKV_READ_DETAILS.0 == 0,
        "default should exclude tikv_read_details"
    );
    assert!(
        categories.0 & TraceCategory::TXN_LIFECYCLE.0 != 0,
        "default should include txn_lifecycle"
    );
    assert!(
        categories.0 & TraceCategory::GENERAL.0 != 0,
        "default should include general"
    );
}

// Go `TestTraceControlExtractor`, with all seven subtests.
#[test]
fn trace_control_extractor() {
    let _guard = lock_global_state();
    let old_categories = crate::tracing::enabled_categories();

    let mut config = FlightRecorderConfig::default();
    config.initialize();
    start_log_flight_recorder(config).unwrap();
    let fr = get_flight_recorder().unwrap();

    // NoSink
    crate::tracing::set_categories(TraceCategory::TIKV_REQUEST);
    let empty_context = tikv_client::trace::TraceContext::new();
    let flags = handle_trace_control_extractor(&empty_context);
    assert!(flags.has(tikv_client::trace::TraceControlFlags::TIKV_CATEGORY_REQUEST));
    assert!(!flags.has(tikv_client::trace::TraceControlFlags::IMMEDIATE_LOG));

    // KeepFalse
    let trace = Arc::new(Trace::new());
    let trace_context = empty_context.with_value::<Trace, _>(Arc::clone(&trace));
    crate::tracing::set_categories(TraceCategory::TIKV_REQUEST);
    let flags = handle_trace_control_extractor(&trace_context);
    assert!(
        !flags.has(tikv_client::trace::TraceControlFlags::IMMEDIATE_LOG),
        "immediate log should not be set when keep=false"
    );
    assert!(
        flags.has(tikv_client::trace::TraceControlFlags::TIKV_CATEGORY_REQUEST),
        "request category should be set"
    );

    // KeepTrue: this sets keep=true.
    let keeping = Arc::new(Trace::new());
    keeping.mark_bits(0);
    assert_eq!(keeping.bits(), fr.truth_table()[0]);
    let keeping_context = empty_context.with_value::<Trace, _>(Arc::clone(&keeping));
    crate::tracing::set_categories(TraceCategory::TIKV_REQUEST);
    let flags = handle_trace_control_extractor(&keeping_context);
    assert!(
        flags.has(tikv_client::trace::TraceControlFlags::IMMEDIATE_LOG),
        "immediate log should be set when keep=true"
    );
    assert!(
        flags.has(tikv_client::trace::TraceControlFlags::TIKV_CATEGORY_REQUEST),
        "request category should be set"
    );

    // CategoryTiKVRequest
    crate::tracing::set_categories(TraceCategory::TIKV_REQUEST);
    let flags = handle_trace_control_extractor(&trace_context);
    assert!(flags.has(tikv_client::trace::TraceControlFlags::TIKV_CATEGORY_REQUEST));
    assert!(!flags.has(tikv_client::trace::TraceControlFlags::TIKV_CATEGORY_WRITE_DETAILS));
    assert!(!flags.has(tikv_client::trace::TraceControlFlags::TIKV_CATEGORY_READ_DETAILS));

    // CategoryTiKVWriteDetails
    crate::tracing::set_categories(TraceCategory::TIKV_WRITE_DETAILS);
    let flags = handle_trace_control_extractor(&trace_context);
    assert!(!flags.has(tikv_client::trace::TraceControlFlags::TIKV_CATEGORY_REQUEST));
    assert!(flags.has(tikv_client::trace::TraceControlFlags::TIKV_CATEGORY_WRITE_DETAILS));
    assert!(!flags.has(tikv_client::trace::TraceControlFlags::TIKV_CATEGORY_READ_DETAILS));

    // CategoryTiKVReadDetails
    crate::tracing::set_categories(TraceCategory::TIKV_READ_DETAILS);
    let flags = handle_trace_control_extractor(&trace_context);
    assert!(!flags.has(tikv_client::trace::TraceControlFlags::TIKV_CATEGORY_REQUEST));
    assert!(!flags.has(tikv_client::trace::TraceControlFlags::TIKV_CATEGORY_WRITE_DETAILS));
    assert!(flags.has(tikv_client::trace::TraceControlFlags::TIKV_CATEGORY_READ_DETAILS));

    // MultipleCategoriesAndKeep
    crate::tracing::set_categories(
        TraceCategory::TIKV_REQUEST
            | TraceCategory::TIKV_WRITE_DETAILS
            | TraceCategory::TIKV_READ_DETAILS,
    );
    let flags = handle_trace_control_extractor(&keeping_context);
    assert!(flags.has(tikv_client::trace::TraceControlFlags::IMMEDIATE_LOG));
    assert!(flags.has(tikv_client::trace::TraceControlFlags::TIKV_CATEGORY_REQUEST));
    assert!(flags.has(tikv_client::trace::TraceControlFlags::TIKV_CATEGORY_WRITE_DETAILS));
    assert!(flags.has(tikv_client::trace::TraceControlFlags::TIKV_CATEGORY_READ_DETAILS));

    // ConcurrentAccess: 100 extractors racing 10 bit markers.
    crate::tracing::set_categories(TraceCategory::TIKV_REQUEST);
    let shared = Arc::new(Trace::new());
    let shared_context = empty_context.with_value::<Trace, _>(Arc::clone(&shared));
    std::thread::scope(|scope| {
        for _ in 0..100 {
            let shared_context = shared_context.clone();
            scope.spawn(move || {
                let _ = handle_trace_control_extractor(&shared_context);
            });
        }
        for _ in 0..10 {
            let shared = Arc::clone(&shared);
            scope.spawn(move || shared.mark_bits(1));
        }
    });

    fr.close();
    crate::tracing::set_categories(old_categories);
}

// `register_with_client_go` installs the real client hooks used by requests.
#[test]
fn register_installs_live_client_hooks() {
    let _guard = lock_global_state();
    start_log_flight_recorder(all_categories_config()).unwrap();
    flight_recorder().discard_or_flush();
    register_with_client_go();

    assert!(tikv_client::trace::is_category_enabled(
        tikv_client::trace::Category::KvRequest
    ));
    let context = tikv_client::trace::TraceContext::new().with_trace_id([1_u8, 2, 3]);
    tikv_client::trace::trace_event(
        &context,
        tikv_client::trace::Category::KvRequest,
        "live.client.event",
        &[
            tikv_client::trace::TraceField::new("key", "value"),
            tikv_client::trace::TraceField::object(
                "route",
                vec![tikv_client::trace::TraceField::binary("start", [4_u8, 5])],
            ),
        ],
    );
    let events = flight_recorder().snapshot();
    assert_eq!(events.last().unwrap().name, "live.client.event");
    assert_eq!(events.last().unwrap().trace_id, [1, 2, 3]);
    assert!(matches!(
        &events.last().unwrap().fields[1].value,
        Value::Object(fields)
            if matches!(fields[0].value, Value::Binary(ref value) if value == &[4, 5])
    ));

    get_flight_recorder().unwrap().close();
}
