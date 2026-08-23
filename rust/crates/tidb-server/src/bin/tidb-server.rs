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

//! Executable entry point for the bounded read-only Rust SQL node.

use std::process::ExitCode;

// Go's runtime owns its own allocator; the Rust node's statement path is
// allocation-heavy (parse/plan/stage churn dominates its CPU profile), so the
// binary runs on jemalloc like production TiKV instead of glibc malloc.
#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL_ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use tidb_server::{run_configured_node, NodeConfig, NodeConfigError};

fn runtime_exit_code<E: std::fmt::Display>(result: Result<(), E>) -> ExitCode {
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("tidb-server startup/runtime failure: {error}");
            ExitCode::FAILURE
        }
    }
}

fn main() -> ExitCode {
    let arguments = std::env::args().collect::<Vec<_>>();
    if arguments
        .iter()
        .skip(1)
        .any(|argument| argument == "-h" || argument == "--help")
    {
        println!("{}", NodeConfig::help_text());
        return ExitCode::SUCCESS;
    }
    if arguments.iter().skip(1).any(|argument| argument == "-V") {
        return match NodeConfig::version_info_for_display(arguments) {
            Ok(info) => {
                println!("{}", tidb_util::printer::get_tidb_info(&info));
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("tidb-server configuration failure: {error}");
                ExitCode::from(2)
            }
        };
    }
    match NodeConfig::parse(arguments) {
        Ok(config) => {
            if !config.affinity_cpus.is_empty() {
                if let Err(error) = tidb_util::sys::linux::set_affinity(&config.affinity_cpus) {
                    eprintln!("set cpu affinity failure: {error}");
                    return ExitCode::FAILURE;
                }
            }
            runtime_exit_code(run_configured_node(config))
        }
        Err(NodeConfigError::HelpRequested) => {
            println!("{}", NodeConfig::help_text());
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("tidb-server configuration failure: {error}");
            ExitCode::from(2)
        }
    }
}

#[cfg(test)]
mod tests {
    // `cmd/tidb-server/main_test.go` (343 lines), dispositioned test by
    // test:
    // * `TestExitCodeForSignal` — TRANSCREATED in
    //   `crate::shutdown_signal`, whose handler is also the WIRING: a
    //   SIGINT-started shutdown exits 130 live.
    // * `TestSetVersionByConfig{InNextGen,InvalidNextGenReleaseVersion,
    //   NormalizeLegacyPlaceholderForNextGen}` — Go's own suite SKIPS all
    //   three on the classic kernel, which is the kernel this port builds
    //   ("Kernel Type"=Classic); skipped here for the same reason.
    // * `TestOverrideConfigKeyspaceActivateMode`, `TestInitDeployMode`,
    //   `TestCreateMgrClientRequiresPodIdentityInStarter`,
    //   `TestSetupKeyspaceObservabilityForStarter{,SkipsNonTiKV}` —
    //   nextgen keyspace/starter infrastructure, out of the ported scope
    //   by name.
    // * `TestRunMain` — the coverage-server harness, not a behavior test.
    // * `TestSetGlobalVars` — TRANSCREATED in part in
    //   `crate::set_global_vars` (port/socket/isolation-engines legs plus
    //   the registry defaults); the instance-scope promotion leg stays with
    //   `setInstanceVar`, named there.
    use super::*;

    #[test]
    fn runtime_failure_is_a_nonzero_process_result() {
        assert_eq!(
            runtime_exit_code(Err("injected shutdown failure")),
            ExitCode::FAILURE
        );
    }
}
