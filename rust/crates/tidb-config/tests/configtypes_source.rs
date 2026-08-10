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

//! Source tests for Go `pkg/config/configtypes/types_test.go`.

use serde::{Deserialize, Serialize};
use tidb_config::configtypes::{ByteSize, Duration};

#[derive(Serialize, Deserialize)]
struct ByteSizeConfig {
    size: ByteSize,
}

#[derive(Serialize, Deserialize)]
struct DurationConfig {
    duration: Duration,
}

#[test]
fn byte_size_matches_source() {
    let cfg: ByteSizeConfig = serde_json::from_str(r#"{"size":"1MiB"}"#).unwrap();
    assert_eq!(cfg.size, ByteSize(1024 * 1024));

    let data = serde_json::to_string(&cfg).unwrap();
    assert_eq!(data, r#"{"size":"1MiB"}"#);

    let cfg: ByteSizeConfig = toml::from_str(r#"size = "512KiB""#).unwrap();
    assert_eq!(cfg.size, ByteSize(512 * 1024));

    let out = toml::to_string(&cfg).unwrap();
    assert_eq!(out, "size = \"512KiB\"\n");
}

#[test]
fn duration_matches_source() {
    let duration = Duration(3_600_000_000_000 + 2 * 60_000_000_000 + 3 * 1_000_000_000);
    let cfg = DurationConfig { duration };
    let data = serde_json::to_string(&cfg).unwrap();
    assert_eq!(data, r#"{"duration":"1h2m3s"}"#);

    let decoded: DurationConfig = serde_json::from_str(&data).unwrap();
    assert_eq!(decoded.duration, duration);

    let cfg: DurationConfig = toml::from_str(r#"duration = "2m3s""#).unwrap();
    assert_eq!(
        cfg.duration,
        Duration(2 * 60_000_000_000 + 3 * 1_000_000_000)
    );

    let out = toml::to_string(&cfg).unwrap();
    assert_eq!(out, "duration = \"2m3s\"\n");
}
