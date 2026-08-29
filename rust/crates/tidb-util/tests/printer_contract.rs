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

//! Go `pkg/util/printer/printer_test.go::TestPrintTiDBInfo`.

use tidb_util::logutil::{init_logger, new_log_config, FileLogConfig, DEFAULT_LOG_FORMAT};
use tidb_util::printer::print_tidb_info;
use tidb_util::versioninfo::VersionInfo;

struct RestoreStdoutLogger;

impl Drop for RestoreStdoutLogger {
    fn drop(&mut self) {
        let config = new_log_config(
            "info",
            DEFAULT_LOG_FORMAT,
            "",
            "",
            FileLogConfig::default(),
            false,
        );
        let _ = init_logger(&config);
    }
}

#[test]
fn print_tidb_info_source() {
    let directory = tempfile::tempdir().expect("log directory");
    let filename = directory.path().join("classic.log");
    let config = new_log_config(
        "info",
        DEFAULT_LOG_FORMAT,
        "",
        "",
        FileLogConfig {
            filename: filename.to_string_lossy().into_owned(),
            max_size: 4096,
            ..FileLogConfig::default()
        },
        true,
    );
    init_logger(&config).expect("install the logger owned by pkg/util/logutil");
    let _restore = RestoreStdoutLogger;

    print_tidb_info(&VersionInfo::build_default(), br#"{"store":"tikv"}"#);

    let output = std::fs::read_to_string(filename).expect("read startup logs");
    assert!(output.contains("Welcome to TiDB."), "{output}");
    assert!(output.contains("loaded config"), "{output}");
    assert!(!output.contains("TiDB Component Version"), "{output}");
    assert!(!output.contains("Deploy Mode"), "{output}");

    let filename = directory.path().join("next-gen.log");
    let config = new_log_config(
        "info",
        DEFAULT_LOG_FORMAT,
        "",
        "",
        FileLogConfig {
            filename: filename.to_string_lossy().into_owned(),
            max_size: 4096,
            ..FileLogConfig::default()
        },
        true,
    );
    init_logger(&config).expect("install next-generation logger");
    let next_gen = VersionInfo {
        release_version: "v26.3.0".to_owned(),
        ..VersionInfo::build_default().with_runtime_environment(
            false,
            "tikv",
            "Next Generation",
            None,
        )
    };
    print_tidb_info(&next_gen, br#"{"store":"tikv"}"#);

    let output = std::fs::read_to_string(filename).expect("read next-generation startup logs");
    assert!(output.contains("TiDB Component Version"), "{output}");
    assert!(output.contains("v26.3.0"), "{output}");
    assert!(output.contains("Deploy Mode"), "{output}");
    assert!(output.contains("premium"), "{output}");
}
