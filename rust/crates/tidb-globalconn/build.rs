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

//! The global-kill build switches Go sets through ldflags.

fn build_value(name: &str, fallback: impl FnOnce() -> String) -> String {
    println!("cargo:rerun-if-env-changed={name}");
    std::env::var(name).unwrap_or_else(|_| fallback())
}

fn emit_build_value(name: &str, value: &str) {
    println!("cargo:rustc-env={name}={value}");
}

fn main() {
    emit_build_value(
        "TIDB_GLOBAL_KILL_TEST",
        &build_value("TIDB_GLOBAL_KILL_TEST", || "0".to_owned()),
    );
    emit_build_value(
        "TIDB_GLOBAL_KILL_SERVER_ID_BITS32",
        &build_value("TIDB_GLOBAL_KILL_SERVER_ID_BITS32", || "11".to_owned()),
    );
    emit_build_value(
        "TIDB_GLOBAL_KILL_LOCAL_CONN_ID_BITS32",
        &build_value("TIDB_GLOBAL_KILL_LOCAL_CONN_ID_BITS32", || "20".to_owned()),
    );
}
