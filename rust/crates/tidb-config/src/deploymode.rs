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

//! Transcreation of Go `pkg/config/deploymode`: the process-wide deployment
//! mode for TiDB X (NextGen) deployments.
//!
//! Premium Reserved keeps the Premium capability set on a fixed-resource
//! deployment shape; Starter supports a large number of small tenants. The
//! mode is initialized during startup, stored process-wide, and only valid
//! on the NextGen kernel.

use std::fmt;
use std::sync::atomic::{AtomicI32, Ordering::SeqCst};

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::kerneltype;

const PREMIUM_NAME: &str = "premium";
const PREMIUM_RESERVED_NAME: &str = "premium_reserved";
const STARTER_NAME: &str = "starter";

/// Deployment mode of the TiDB instance (Go `Mode`). Only allowed when the
/// kernel type is NextGen.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
#[repr(i32)]
pub enum Mode {
    /// The default deployment mode.
    #[default]
    Premium = 0,
    /// Fixed-resource premium: workers are not scaled on demand.
    PremiumReserved = 1,
    /// Deployment supporting a large number of small tenants.
    Starter = 2,
}

static CURRENT_MODE: AtomicI32 = AtomicI32::new(Mode::Premium as i32);

/// The current deployment mode (Go `Get`).
pub fn get() -> Mode {
    match CURRENT_MODE.load(SeqCst) {
        1 => Mode::PremiumReserved,
        2 => Mode::Starter,
        _ => Mode::Premium,
    }
}

/// Whether the current mode is PremiumReserved (Go `IsPremiumReserved`).
pub fn is_premium_reserved() -> bool {
    kerneltype::is_next_gen() && get() == Mode::PremiumReserved
}

/// Whether the current mode is Starter (Go `IsStarter`).
pub fn is_starter() -> bool {
    kerneltype::is_next_gen() && get() == Mode::Starter
}

/// Sets the deployment mode during startup (Go `Set`); it cannot be changed
/// after it is set.
pub fn set(mode: Mode) -> Result<(), String> {
    if !kerneltype::is_next_gen() {
        return Err("deploy mode can only be set for nextgen TiDB".to_string());
    }
    if !mode.valid() {
        return Err(format!("invalid deploy mode {}", mode as i32));
    }
    CURRENT_MODE.store(mode as i32, SeqCst);
    Ok(())
}

/// Restores the mode directly (test surface mirroring the Go tests' direct
/// atomic store).
#[cfg(test)]
pub(crate) fn store_for_test(mode: Mode) {
    CURRENT_MODE.store(mode as i32, SeqCst);
}

/// Parses a deployment mode string, case-insensitively (Go `Parse`).
pub fn parse(s: &str) -> Result<Mode, String> {
    match s.to_lowercase().as_str() {
        PREMIUM_NAME => Ok(Mode::Premium),
        PREMIUM_RESERVED_NAME => Ok(Mode::PremiumReserved),
        STARTER_NAME => Ok(Mode::Starter),
        _ => Err(format!("invalid deploy mode {s:?}")),
    }
}

impl Mode {
    /// Whether the mode is valid (Go `Valid`).
    pub fn valid(&self) -> bool {
        matches!(self, Mode::Premium | Mode::PremiumReserved | Mode::Starter)
    }

    /// The string representation (Go `String`).
    pub fn as_str(&self) -> &'static str {
        match self {
            Mode::Premium => PREMIUM_NAME,
            Mode::PremiumReserved => PREMIUM_RESERVED_NAME,
            Mode::Starter => STARTER_NAME,
        }
    }
}

impl fmt::Display for Mode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// All valid deployment modes (Go `ModeList`).
pub fn mode_list() -> [Mode; 3] {
    [Mode::Premium, Mode::PremiumReserved, Mode::Starter]
}

impl Serialize for Mode {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for Mode {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Mode, D::Error> {
        let s = String::deserialize(deserializer)?;
        parse(&s).map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mode_json() {
        assert_eq!(
            serde_json::to_string(&Mode::PremiumReserved).unwrap(),
            r#""premium_reserved""#
        );
        assert_eq!(
            serde_json::to_string(&Mode::Starter).unwrap(),
            r#""starter""#
        );

        let mode: Mode = serde_json::from_str(r#""premium""#).unwrap();
        assert_eq!(mode, Mode::Premium);
        let mode: Mode = serde_json::from_str(r#""premium_reserved""#).unwrap();
        assert_eq!(mode, Mode::PremiumReserved);
        let mode: Mode = serde_json::from_str(r#""Premium_Reserved""#).unwrap();
        assert_eq!(mode, Mode::PremiumReserved);
        let mode: Mode = serde_json::from_str(r#""Starter""#).unwrap();
        assert_eq!(mode, Mode::Starter);

        let err = serde_json::from_str::<Mode>(r#""unknown""#).unwrap_err();
        assert!(err.to_string().contains(r#"invalid deploy mode "unknown""#));
        assert!(serde_json::from_str::<Mode>("1").is_err());
    }

    #[test]
    fn mode_toml() {
        #[derive(serde::Deserialize)]
        struct Cfg {
            #[serde(rename = "deploy-mode")]
            mode: Mode,
        }
        let cfg: Cfg = toml::from_str(r#"deploy-mode = "premium_reserved""#).unwrap();
        assert_eq!(cfg.mode, Mode::PremiumReserved);
        let cfg: Cfg = toml::from_str(r#"deploy-mode = "Premium""#).unwrap();
        assert_eq!(cfg.mode, Mode::Premium);
        let cfg: Cfg = toml::from_str(r#"deploy-mode = "Starter""#).unwrap();
        assert_eq!(cfg.mode, Mode::Starter);
    }

    #[test]
    fn current_mode() {
        let original = get();

        if !crate::kerneltype::is_next_gen() {
            assert_eq!(get(), Mode::Premium);
            store_for_test(Mode::PremiumReserved);
            assert!(!is_premium_reserved());
            store_for_test(Mode::Starter);
            assert!(!is_starter());
            assert!(set(Mode::PremiumReserved)
                .unwrap_err()
                .contains("deploy mode can only be set for nextgen TiDB"));
            store_for_test(original);
            return;
        }

        assert_eq!(get(), Mode::Premium);
        assert!(!is_premium_reserved());
        assert!(!is_starter());
        set(Mode::PremiumReserved).unwrap();
        assert_eq!(get(), Mode::PremiumReserved);
        assert!(is_premium_reserved());
        assert!(!is_starter());
        set(Mode::Starter).unwrap();
        assert_eq!(get(), Mode::Starter);
        assert!(!is_premium_reserved());
        assert!(is_starter());
        store_for_test(original);
    }
}
