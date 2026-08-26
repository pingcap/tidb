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

//! Direct ports of the Go leaf-package tests under `pkg/config`
//! (origin/master): `configtypes/types_test.go`, `deploymode/mode_test.go`,
//! and `kerneltype/type_test.go`.

use crate::configtypes::{ByteSize, Duration};
use crate::deploymode;
use crate::kerneltype;

#[derive(serde::Serialize, serde::Deserialize)]
struct ByteSizeConfig {
    size: ByteSize,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct DurationConfig {
    duration: Duration,
}

// Go TestByteSize (configtypes/types_test.go).
#[test]
fn byte_size() {
    // json subtest.
    let mut cfg: ByteSizeConfig = serde_json::from_str(r#"{"size":"1MiB"}"#).unwrap();
    assert_eq!(cfg.size.0, 1024 * 1024);
    assert_eq!(serde_json::to_string(&cfg).unwrap(), r#"{"size":"1MiB"}"#);

    // toml subtest.
    cfg = toml::from_str("size = \"512KiB\"").unwrap();
    assert_eq!(cfg.size.0, 512 * 1024);
    assert_eq!(toml::to_string(&cfg).unwrap(), "size = \"512KiB\"\n");
}

// Go TestDuration (configtypes/types_test.go).
#[test]
fn duration() {
    // json subtest: 1h2m3s round trip.
    let d = Duration::from_std(
        std::time::Duration::from_secs(3600) + std::time::Duration::from_secs(123),
    );
    let cfg = DurationConfig { duration: d };
    let data = serde_json::to_string(&cfg).unwrap();
    assert_eq!(data, r#"{"duration":"1h2m3s"}"#);
    let decoded: DurationConfig = serde_json::from_str(&data).unwrap();
    assert_eq!(decoded.duration.0, d.0);

    // toml subtest: 2m3s.
    let cfg: DurationConfig = toml::from_str("duration = \"2m3s\"").unwrap();
    assert_eq!(
        cfg.duration.as_std(),
        std::time::Duration::from_secs(123)
    );
    assert_eq!(toml::to_string(&cfg).unwrap(), "duration = \"2m3s\"\n");
}

// Go TestModeJSON (deploymode/mode_test.go).
#[test]
fn mode_json() {
    assert_eq!(
        serde_json::to_string(&deploymode::Mode::PremiumReserved).unwrap(),
        "\"premium_reserved\""
    );
    assert_eq!(
        serde_json::to_string(&deploymode::Mode::Starter).unwrap(),
        "\"starter\""
    );

    let mode: deploymode::Mode = serde_json::from_str("\"premium\"").unwrap();
    assert_eq!(mode, deploymode::Mode::Premium);

    let mode: deploymode::Mode = serde_json::from_str("\"premium_reserved\"").unwrap();
    assert_eq!(mode, deploymode::Mode::PremiumReserved);

    let mode: deploymode::Mode = serde_json::from_str("\"Premium_Reserved\"").unwrap();
    assert_eq!(mode, deploymode::Mode::PremiumReserved);

    let mode: deploymode::Mode = serde_json::from_str("\"Starter\"").unwrap();
    assert_eq!(mode, deploymode::Mode::Starter);

    assert!(serde_json::from_str::<deploymode::Mode>("\"unknown\"").is_err());
    assert!(serde_json::from_str::<deploymode::Mode>("1").is_err());
}

// Go TestModeTOML (deploymode/mode_test.go).
#[test]
fn mode_toml() {
    #[derive(serde::Deserialize)]
    struct Cfg {
        #[serde(rename = "deploy-mode")]
        mode: deploymode::Mode,
    }

    let cfg: Cfg = toml::from_str("deploy-mode = \"premium_reserved\"").unwrap();
    assert_eq!(cfg.mode, deploymode::Mode::PremiumReserved);

    let cfg: Cfg = toml::from_str("deploy-mode = \"Premium\"").unwrap();
    assert_eq!(cfg.mode, deploymode::Mode::Premium);

    let cfg: Cfg = toml::from_str("deploy-mode = \"Starter\"").unwrap();
    assert_eq!(cfg.mode, deploymode::Mode::Starter);
}

// Go TestCurrentMode (deploymode/mode_test.go): Get/Is* follow the current
// atomic mode; Set validates against the kernel type.
#[test]
fn current_mode() {
    let original = deploymode::get();

    if !kerneltype::is_next_gen() {
        assert_eq!(deploymode::get(), deploymode::Mode::Premium);
        deploymode::store_for_test(deploymode::Mode::PremiumReserved);
        assert!(!deploymode::is_premium_reserved());
        deploymode::store_for_test(deploymode::Mode::Starter);
        assert!(!deploymode::is_starter());
        assert!(deploymode::set(deploymode::Mode::PremiumReserved)
            .unwrap_err()
            .contains("deploy mode can only be set for nextgen TiDB"));
        deploymode::store_for_test(original);
        return;
    }

    assert_eq!(deploymode::get(), deploymode::Mode::Premium);
    assert!(!deploymode::is_premium_reserved());
    assert!(!deploymode::is_starter());
    deploymode::set(deploymode::Mode::PremiumReserved).unwrap();
    assert_eq!(deploymode::get(), deploymode::Mode::PremiumReserved);
    assert!(deploymode::is_premium_reserved());
    assert!(!deploymode::is_starter());
    deploymode::set(deploymode::Mode::Starter).unwrap();
    assert_eq!(deploymode::get(), deploymode::Mode::Starter);
    assert!(!deploymode::is_premium_reserved());
    assert!(deploymode::is_starter());
    assert!(deploymode::set(deploymode::Mode::Unknown(100))
        .unwrap_err()
        .contains("invalid deploy mode"));
    deploymode::store_for_test(original);
}

// Go TestKernelType (kerneltype/type_test.go).
#[test]
fn kernel_type() {
    assert_eq!(!kerneltype::is_classic(), kerneltype::is_next_gen());
    assert_eq!(kerneltype::is_classic(), !kerneltype::is_next_gen());
}

// Go TestIsMatch (kerneltype/type_test.go).
#[test]
fn is_match() {
    if kerneltype::is_classic() {
        assert!(kerneltype::is_match(""));
        assert!(kerneltype::is_match("Classic"));
    } else if kerneltype::is_next_gen() {
        assert!(kerneltype::is_match("Next Generation"));
    }
    assert!(!kerneltype::is_match("Unknown"));
}
