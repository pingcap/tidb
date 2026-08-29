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
//!
//! Premium Reserved runs user traffic and distributed tasks directly on TiDB
//! nodes in the SYSTEM keyspace instead of assuming TiDB-worker,
//! TiKV-worker, or coprocessor-worker resources can scale on demand. Like Go,
//! the process-wide value comes from each TiDB instance's component config;
//! cloud deployment orchestration is responsible for keeping instances in a
//! group consistent.

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
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
#[repr(transparent)]
pub struct Mode(i32);

#[allow(non_upper_case_globals)]
impl Mode {
    /// The default deployment mode.
    pub const Premium: Self = Self(0);
    /// Fixed-resource premium: workers are not scaled on demand.
    pub const PremiumReserved: Self = Self(1);
    /// Deployment supporting a large number of small tenants.
    pub const Starter: Self = Self(2);
}

static CURRENT_MODE: AtomicI32 = AtomicI32::new(Mode::Premium.to_i32());

/// The current deployment mode (Go `Get`).
pub fn get() -> Mode {
    Mode::from_i32(CURRENT_MODE.load(SeqCst))
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
        return Err(format!("invalid deploy mode {}", mode.to_i32()));
    }
    CURRENT_MODE.store(mode.to_i32(), SeqCst);
    Ok(())
}

/// Restores the mode directly (test surface mirroring the Go tests' direct
/// atomic store).
#[cfg(test)]
pub(crate) fn store_for_test(mode: Mode) {
    CURRENT_MODE.store(mode.to_i32(), SeqCst);
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
    /// Converts a source integer to a mode value, preserving invalid values.
    pub const fn from_i32(value: i32) -> Self {
        Self(value)
    }

    /// Returns the source integer value.
    pub const fn to_i32(&self) -> i32 {
        self.0
    }

    /// Whether the mode is valid (Go `Valid`).
    pub fn valid(&self) -> bool {
        matches!(*self, Mode::Premium | Mode::PremiumReserved | Mode::Starter)
    }

    /// The valid string representation.
    fn as_str(&self) -> Option<&'static str> {
        match *self {
            Mode::Premium => Some(PREMIUM_NAME),
            Mode::PremiumReserved => Some(PREMIUM_RESERVED_NAME),
            Mode::Starter => Some(STARTER_NAME),
            _ => None,
        }
    }
}

impl fmt::Display for Mode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(name) = self.as_str() {
            f.write_str(name)
        } else {
            write!(f, "unknown({})", self.to_i32())
        }
    }
}

/// All valid deployment modes (Go `ModeList`).
pub fn mode_list() -> Vec<Mode> {
    vec![Mode::Premium, Mode::PremiumReserved, Mode::Starter]
}

impl Serialize for Mode {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let Some(name) = self.as_str() else {
            return Err(serde::ser::Error::custom(format!(
                "invalid deploy mode {}",
                self.to_i32()
            )));
        };
        serializer.serialize_str(name)
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
        assert!(serde_json::to_string(&Mode::from_i32(100))
            .unwrap_err()
            .to_string()
            .contains("invalid deploy mode 100"));
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
        assert!(set(Mode::from_i32(100))
            .unwrap_err()
            .contains("invalid deploy mode 100"));
        assert!(Mode::from_i32(-1) < Mode::Premium);
        assert_eq!(Mode::from_i32(100).to_string(), "unknown(100)");
        store_for_test(original);
    }
}
