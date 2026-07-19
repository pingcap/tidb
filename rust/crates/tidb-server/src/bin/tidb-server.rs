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
    match NodeConfig::parse(std::env::args()) {
        Ok(config) => runtime_exit_code(run_configured_node(config)),
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
    use super::*;

    #[test]
    fn runtime_failure_is_a_nonzero_process_result() {
        assert_eq!(
            runtime_exit_code(Err("injected shutdown failure")),
            ExitCode::FAILURE
        );
    }
}
