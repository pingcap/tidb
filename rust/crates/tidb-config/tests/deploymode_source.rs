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

//! Source tests for Go `pkg/config/deploymode/mode_test.go`.

use tidb_config::deploymode::{
    get, is_premium_reserved, is_starter, set, Mode, Mode::Premium, Mode::PremiumReserved,
    Mode::Starter,
};

#[test]
fn mode_json_matches_source() {
    assert_eq!(
        serde_json::to_string(&PremiumReserved).unwrap(),
        r#""premium_reserved""#
    );
    assert_eq!(serde_json::to_string(&Starter).unwrap(), r#""starter""#);

    let mode: Mode = serde_json::from_str(r#""premium""#).unwrap();
    assert_eq!(mode, Premium);
    let mode: Mode = serde_json::from_str(r#""premium_reserved""#).unwrap();
    assert_eq!(mode, PremiumReserved);
    let mode: Mode = serde_json::from_str(r#""Premium_Reserved""#).unwrap();
    assert_eq!(mode, PremiumReserved);
    let mode: Mode = serde_json::from_str(r#""Starter""#).unwrap();
    assert_eq!(mode, Starter);

    let unknown = serde_json::from_str::<Mode>(r#""unknown""#).unwrap_err();
    assert!(unknown
        .to_string()
        .contains(r#"invalid deploy mode "unknown""#));
    assert!(serde_json::from_str::<Mode>("1").is_err());
}

#[test]
fn mode_toml_matches_source() {
    #[derive(serde::Deserialize)]
    struct Cfg {
        #[serde(rename = "deploy-mode")]
        mode: Mode,
    }

    let cfg: Cfg = toml::from_str(r#"deploy-mode = "premium_reserved""#).unwrap();
    assert_eq!(cfg.mode, PremiumReserved);
    let cfg: Cfg = toml::from_str(r#"deploy-mode = "Premium""#).unwrap();
    assert_eq!(cfg.mode, Premium);
    let cfg: Cfg = toml::from_str(r#"deploy-mode = "Starter""#).unwrap();
    assert_eq!(cfg.mode, Starter);
}

#[test]
fn current_mode_matches_source() {
    if !tidb_config::kerneltype::is_next_gen() {
        assert_eq!(get(), Premium);
        assert!(!is_premium_reserved());
        assert!(!is_starter());
        assert!(set(PremiumReserved)
            .unwrap_err()
            .contains("deploy mode can only be set for nextgen TiDB"));
        return;
    }

    let original = get();
    assert_eq!(get(), Premium);
    assert!(!is_premium_reserved());
    assert!(!is_starter());
    set(PremiumReserved).unwrap();
    assert_eq!(get(), PremiumReserved);
    assert!(is_premium_reserved());
    assert!(!is_starter());
    set(Starter).unwrap();
    assert_eq!(get(), Starter);
    assert!(!is_premium_reserved());
    assert!(is_starter());
    assert!(set(Mode::Unknown(100))
        .unwrap_err()
        .contains("invalid deploy mode 100"));
    set(original).unwrap();
}
