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

//! Captures the compiler identity printed through TiDB's version surfaces.

use std::process::Command;

use chrono::Utc;

fn command_output(program: &str, args: &[&str]) -> Option<String> {
    Command::new(program)
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|output| output.trim().to_owned())
        .filter(|output| !output.is_empty())
}

fn build_value(name: &str, fallback: impl FnOnce() -> String) -> String {
    println!("cargo:rerun-if-env-changed={name}");
    std::env::var(name).unwrap_or_else(|_| fallback())
}

fn emit_build_value(name: &str, value: &str) {
    println!("cargo:rustc-env={name}={value}");
}

fn register_git_inputs() {
    for name in ["HEAD", "packed-refs"] {
        if let Some(path) = command_output("git", &["rev-parse", "--git-path", name]) {
            println!("cargo:rerun-if-changed={path}");
        }
    }
    if let Some(reference) = command_output("git", &["symbolic-ref", "-q", "HEAD"]) {
        if let Some(path) = command_output("git", &["rev-parse", "--git-path", &reference]) {
            println!("cargo:rerun-if-changed={path}");
        }
    }
}

fn main() {
    register_git_inputs();
    println!("cargo:rerun-if-env-changed=RUSTC");
    let rustc = std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());
    let version = Command::new(rustc)
        .arg("--version")
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|version| version.trim().to_owned())
        .filter(|version| !version.is_empty())
        .unwrap_or_else(|| "rustc unknown".to_owned());
    println!("cargo:rustc-env=TIDB_RUST_VERSION={version}");

    emit_build_value(
        "TIDB_BUILD_TS",
        &build_value("TIDB_BUILD_TS", || {
            Utc::now().format("%Y-%m-%d %H:%M:%S").to_string()
        }),
    );
    emit_build_value(
        "TIDB_GIT_HASH",
        &build_value("TIDB_GIT_HASH", || {
            command_output("git", &["rev-parse", "HEAD"]).unwrap_or_else(|| "None".to_owned())
        }),
    );
    emit_build_value(
        "TIDB_GIT_BRANCH",
        &build_value("TIDB_GIT_BRANCH", || {
            command_output("git", &["rev-parse", "--abbrev-ref", "HEAD"])
                .unwrap_or_else(|| "None".to_owned())
        }),
    );
    emit_build_value(
        "TIDB_EDITION",
        &build_value("TIDB_EDITION", || "Community".to_owned()),
    );
    emit_build_value(
        "TIDB_ENTERPRISE_EXTENSION_GIT_HASH",
        &build_value("TIDB_ENTERPRISE_EXTENSION_GIT_HASH", String::new),
    );
}
