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

//! Go `pkg/executor/analyze_worker.go`: the workers that save ANALYZE
//! results.

use crossbeam_channel::{Receiver, Sender};
use tidb_sqlkiller::SqlKiller;

/// Go `analyzeSaveStatsWorker`.
pub(crate) struct AnalyzeSaveStatsWorker<'a, R> {
    results: Receiver<R>,
    err: Sender<String>,
    killer: &'a SqlKiller,
}

impl<'a, R> AnalyzeSaveStatsWorker<'a, R> {
    /// Go `newAnalyzeSaveStatsWorker`.
    pub(crate) fn new(results: Receiver<R>, err: Sender<String>, killer: &'a SqlKiller) -> Self {
        Self { results, err, killer }
    }

    /// Go `run`: consumes results and saves them. After a kill signal it
    /// drains: no more stats are saved, the remaining jobs finish with the
    /// same error so the analyze workers are not blocked on sending. A save
    /// failure does not drain, so later partitions are still saved. Each
    /// worker reports at most one error.
    ///
    /// `save` is Go `statsHandle.SaveAnalyzeResultToStorage`, `finish` Go
    /// `finishJobWithLog`.
    pub(crate) fn run(
        self,
        save: impl Fn(&R) -> Result<(), String>,
        finish: impl Fn(&R, Option<&str>),
    ) {
        let mut err_reported = false;
        let outcome = crate::analyze::panic_recovery::recover_analyze_panic(|| {
            let mut drain_err: Option<String> = None;
            for results in self.results.iter() {
                if let Some(error) = &drain_err {
                    // Drain mode: consume the remaining results only to
                    // unblock producers.
                    finish(&results, Some(error));
                    continue;
                }
                tidb_failpoint::inject_call(
                    "github.com/pingcap/tidb/pkg/executor/analyzeSaveWorkerBeforeHandleSignal",
                    &mut (),
                );
                if let Some(error) = self.killer.handle_signal() {
                    let error = error.to_sql_error().message;
                    finish(&results, Some(&error));
                    if !err_reported {
                        let _ = self.err.send(error.clone());
                        err_reported = true;
                    }
                    drain_err = Some(error);
                    continue;
                }
                match save(&results) {
                    Err(error) => {
                        tidb_logutil::bg_logger().warn(
                            "save table stats to storage failed",
                            &[tidb_log::Field::new("error", tidb_log::Value::Str(error.clone()))],
                        );
                        finish(&results, Some(&error));
                        if !err_reported {
                            let _ = self.err.send(error);
                            err_reported = true;
                        }
                        // Keep draining results to avoid blocking analyze
                        // workers after a save failure.
                    }
                    Ok(()) => finish(&results, None),
                }
            }
        });
        if let Err(panic) = outcome {
            tidb_logutil::bg_logger().error("analyze save stats worker panicked", &[]);
            if !err_reported {
                let _ = self.err.send(panic.rendered_message().to_owned());
            }
        }
    }
}
