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

//! Source tests for the remaining uncovered parts of Go
//! `pkg/config/config_test.go::TestConfig` and `TestDeployModeConfig`
//! (telemetry overrides, gRPC keepalive bounds, hosted embedding).

use tidb_config::config_tree::new_config;

// Go `TestConfig`: telemetry defaults to false and is only enabled by an
// explicit `enable-telemetry = true`; unrelated option loads keep it false.
#[test]
fn telemetry_default_and_overrides_match_source() {
    let mut conf = new_config();
    conf.load_str("config.toml", "\n").unwrap();
    assert!(!conf.enable_telemetry);

    conf.load_str("config.toml", "enable-table-lock = true\n")
        .unwrap();
    assert!(!conf.enable_telemetry);

    conf.load_str("config.toml", "enable-telemetry = true\n")
        .unwrap();
    assert!(conf.enable_telemetry);

    // Same block also flips the spilled-file encryption method.
    conf.load_str(
        "config.toml",
        "[security]\nspilled-file-encryption-method = \"aes128-ctr\"\n",
    )
    .unwrap();
    assert_eq!(
        conf.security.spilled_file_encryption_method,
        tidb_config::config_tree::big_sections::SPILLED_FILE_ENCRYPTION_METHOD_AES128_CTR
    );
}

// Go `TestConfig`: a sub-0.05s grpc-keepalive-timeout fails Valid with an
// exact message.
#[test]
fn grpc_keepalive_timeout_minimum_matches_source() {
    let mut conf = new_config();
    conf.load_str("config.toml", "[tikv-client]\ngrpc-keepalive-timeout = 3\n")
        .unwrap();
    assert_eq!(conf.tikv_client.grpc_keep_alive_timeout_nanos(), 3_000_000_000);

    let mut conf = new_config();
    conf.load_str(
        "config.toml",
        "[tikv-client]\ngrpc-keepalive-timeout = 0.01\n",
    )
    .unwrap();
    assert_eq!(
        conf.valid().unwrap_err(),
        "grpc-keepalive-timeout should be at least 0.05, but got 0.010000"
    );
}

// go-parity-gap: the `HostedEmbedding` config section (Go config.go
// `HostedEmbedding`, its `configured()` helper and the
// "hosted-embedding can only be configured for starter deploy mode" checks in
// `Load`/`Valid`) has not been transcreated yet.
#[test]
#[ignore]
fn hosted_embedding_starter_only_checks_match_source() {
    // Go `TestConfig` opening block:
    //   load("[hosted-embedding]") -> error containing
    //   "hosted-embedding can only be configured for starter deploy mode";
    //   Enabled=true / APIEndpoint set -> same error from Valid().
}
