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

//! Deterministic proof for dependency-ordered process shutdown.

use tidb_exec::real_tikv_read::{
    shutdown_read_process, ReadProcessShutdownError, ReadProcessShutdownFailure,
    ReadProcessShutdownStage, ReadProcessShutdownStages,
};

#[derive(Default)]
struct ScriptedStages {
    events: Vec<ReadProcessShutdownStage>,
    fail_region: bool,
    fail_transport: bool,
    fail_pd: bool,
}

impl ReadProcessShutdownStages for ScriptedStages {
    fn shutdown_region_cache(&mut self) -> Result<(), String> {
        self.events.push(ReadProcessShutdownStage::RegionCache);
        if self.fail_region {
            Err("region worker panicked".to_owned())
        } else {
            Ok(())
        }
    }

    fn shutdown_tikv_transport(&mut self) -> Result<(), String> {
        self.events.push(ReadProcessShutdownStage::TikvTransport);
        if self.fail_transport {
            Err("transport acknowledgement lost".to_owned())
        } else {
            Ok(())
        }
    }

    fn shutdown_pd(&mut self) -> Result<(), String> {
        self.events.push(ReadProcessShutdownStage::Pd);
        if self.fail_pd {
            Err("PD worker panicked".to_owned())
        } else {
            Ok(())
        }
    }
}

#[test]
fn shutdown_attempts_every_stage_in_dependency_order() {
    // Source obligation: client-go::tikv/kv_test.go:285
    // (TestKVStoreCloseCheckRegionCacheClosedBeforePDClose).
    let mut stages = ScriptedStages {
        fail_region: true,
        fail_transport: true,
        fail_pd: true,
        ..ScriptedStages::default()
    };

    let result = shutdown_read_process(0, &mut stages);

    assert_eq!(
        stages.events,
        [
            ReadProcessShutdownStage::RegionCache,
            ReadProcessShutdownStage::TikvTransport,
            ReadProcessShutdownStage::Pd,
        ]
    );
    assert_eq!(
        result,
        Err(ReadProcessShutdownError::StageFailures(vec![
            ReadProcessShutdownFailure {
                stage: ReadProcessShutdownStage::RegionCache,
                message: "region worker panicked".to_owned(),
            },
            ReadProcessShutdownFailure {
                stage: ReadProcessShutdownStage::TikvTransport,
                message: "transport acknowledgement lost".to_owned(),
            },
            ReadProcessShutdownFailure {
                stage: ReadProcessShutdownStage::Pd,
                message: "PD worker panicked".to_owned(),
            },
        ]))
    );
}

#[test]
fn active_session_rejection_does_not_touch_any_process_stage() {
    let mut stages = ScriptedStages::default();

    assert_eq!(
        shutdown_read_process(2, &mut stages),
        Err(ReadProcessShutdownError::ActiveSessions { active: 2 })
    );
    assert!(stages.events.is_empty());
}
