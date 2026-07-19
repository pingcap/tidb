// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(missing_docs)]

use std::sync::{Arc, Mutex};

use tidb_exec::real_tikv_read::{
    ReadProcessShutdownError, ReadProcessShutdownFailure, ReadProcessShutdownStage,
};
use tidb_server::{
    run_with_process_shutdown, ProcessReadAuthority, RunConfiguredNodeError, SqlNodeError,
};

struct FactoryLease {
    events: Arc<Mutex<Vec<&'static str>>>,
}

impl Drop for FactoryLease {
    fn drop(&mut self) {
        self.events.lock().unwrap().push("factory_drop");
    }
}

struct ScriptedAuthority {
    events: Arc<Mutex<Vec<&'static str>>>,
    result: Option<Result<(), ReadProcessShutdownError>>,
}

impl ProcessReadAuthority for ScriptedAuthority {
    fn shutdown_process(&mut self) -> Result<(), ReadProcessShutdownError> {
        self.events.lock().unwrap().push("authority_shutdown");
        self.result.take().expect("one shutdown result")
    }
}

fn shutdown_failure() -> ReadProcessShutdownError {
    ReadProcessShutdownError::StageFailures(vec![
        ReadProcessShutdownFailure {
            stage: ReadProcessShutdownStage::RegionCache,
            message: "maintenance worker panicked".to_owned(),
        },
        ReadProcessShutdownFailure {
            stage: ReadProcessShutdownStage::Pd,
            message: "PD worker panicked".to_owned(),
        },
    ])
}

#[test]
fn node_and_authority_failures_are_combined_after_factory_drop() {
    // Source obligations:
    // - cmd/tidb-server/main_test.go:56 TestExitCodeForSignal
    // - pkg/server/tests/commontest/tidb_test.go:1098 TestGracefulShutdown
    let events = Arc::new(Mutex::new(Vec::new()));
    let factory = FactoryLease {
        events: Arc::clone(&events),
    };
    let authority = ScriptedAuthority {
        events: Arc::clone(&events),
        result: Some(Err(shutdown_failure())),
    };

    let result = run_with_process_shutdown(factory, authority, |factory| {
        events.lock().unwrap().push("node_run");
        drop(factory);
        Err(RunConfiguredNodeError::Node(
            SqlNodeError::WorkerQueueClosed,
        ))
    });

    assert_eq!(
        *events.lock().unwrap(),
        ["node_run", "factory_drop", "authority_shutdown"]
    );
    let Err(RunConfiguredNodeError::Combined { run, authority }) = result else {
        panic!("both independent failures must survive composition");
    };
    assert!(matches!(*run, RunConfiguredNodeError::Node(_)));
    assert!(matches!(
        authority,
        ReadProcessShutdownError::StageFailures(failures) if failures.len() == 2
    ));
}

#[test]
fn authority_failure_turns_an_otherwise_successful_run_into_failure() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let result = run_with_process_shutdown(
        FactoryLease {
            events: Arc::clone(&events),
        },
        ScriptedAuthority {
            events: Arc::clone(&events),
            result: Some(Err(shutdown_failure())),
        },
        |factory| {
            drop(factory);
            Ok(())
        },
    );

    assert!(matches!(result, Err(RunConfiguredNodeError::Authority(_))));
    assert_eq!(
        *events.lock().unwrap(),
        ["factory_drop", "authority_shutdown"]
    );
}

#[test]
fn post_connect_early_node_failure_still_shuts_down_authority() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let result = run_with_process_shutdown(
        FactoryLease {
            events: Arc::clone(&events),
        },
        ScriptedAuthority {
            events: Arc::clone(&events),
            result: Some(Ok(())),
        },
        |factory| {
            drop(factory);
            Err(RunConfiguredNodeError::Node(
                SqlNodeError::WorkerQueueClosed,
            ))
        },
    );

    assert!(matches!(result, Err(RunConfiguredNodeError::Node(_))));
    assert_eq!(
        *events.lock().unwrap(),
        ["factory_drop", "authority_shutdown"]
    );
}
