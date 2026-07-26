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

//! The `nullableBool` and `AtomicBool` config marshaling helpers from Go
//! `pkg/config/config.go`.
//!
//! Both carry custom TOML/JSON codecs in Go. The exact BurntSushi error
//! *text* on a bad value is library-specific and not part of the config
//! contract; what matters — and what the Rust port reproduces — is the
//! value mapping: `AtomicBool` de/serializes as the strings `"true"`/
//! `"false"` (empty/null → false), and `nullableBool` distinguishes unset
//! (JSON null / empty text) from true/false.

use std::fmt;

use serde::de::Unexpected;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Defaults an unset bool to "unset" instead of false, so conflicting
/// options can be detected (Go `nullableBool`).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct NullableBool {
    /// Whether a value was set.
    pub is_valid: bool,
    /// The value, when valid.
    pub is_true: bool,
}

/// Go `nbUnset`.
pub const NB_UNSET: NullableBool = NullableBool {
    is_valid: false,
    is_true: false,
};
/// Go `nbFalse`.
pub const NB_FALSE: NullableBool = NullableBool {
    is_valid: true,
    is_true: false,
};
/// Go `nbTrue`.
pub const NB_TRUE: NullableBool = NullableBool {
    is_valid: true,
    is_true: true,
};

impl Default for NullableBool {
    fn default() -> Self {
        NB_UNSET
    }
}

impl NullableBool {
    /// Go `toBool`.
    pub fn to_bool(&self) -> bool {
        self.is_valid && self.is_true
    }

    // Go `UnmarshalText` (also the TOML path).
    fn from_text(s: &str) -> Result<NullableBool, String> {
        match s {
            "" | "null" => Ok(NB_UNSET),
            "true" => Ok(NB_TRUE),
            "false" => Ok(NB_FALSE),
            other => Err(format!("Invalid value for bool type: {other}")),
        }
    }
}

impl Serialize for NullableBool {
    // Go `MarshalJSON`: true / false / null.
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match *self {
            NB_TRUE => serializer.serialize_bool(true),
            NB_FALSE => serializer.serialize_bool(false),
            _ => serializer.serialize_none(),
        }
    }
}

impl<'de> Deserialize<'de> for NullableBool {
    // Go `UnmarshalJSON`: a JSON bool → valid; anything else → unset. TOML
    // feeds strings here, handled via `from_text`.
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<NullableBool, D::Error> {
        struct V;
        impl serde::de::Visitor<'_> for V {
            type Value = NullableBool;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a bool, string, or null")
            }
            fn visit_bool<E>(self, v: bool) -> Result<NullableBool, E> {
                Ok(NullableBool {
                    is_valid: true,
                    is_true: v,
                })
            }
            fn visit_none<E>(self) -> Result<NullableBool, E> {
                Ok(NB_UNSET)
            }
            fn visit_unit<E>(self) -> Result<NullableBool, E> {
                Ok(NB_UNSET)
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<NullableBool, E> {
                NullableBool::from_text(v)
                    .map_err(|_| E::invalid_value(Unexpected::Str(v), &"true, false, or empty"))
            }
            // A non-bool scalar (e.g. TOML `enable-error-stack = 1`) is
            // rejected, matching Go's UnmarshalText contract exercised by
            // the config tests. Go's UnmarshalJSON maps a JSON number to
            // unset, but the marshaled round-trip only ever produces
            // null/true/false, so that branch is unreachable in practice.
            fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<NullableBool, E> {
                Err(E::custom(format!("Invalid value for bool type: {v}")))
            }
            fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<NullableBool, E> {
                Err(E::custom(format!("Invalid value for bool type: {v}")))
            }
        }
        deserializer.deserialize_any(V)
    }
}

/// A bool that de/serializes as the text `"true"`/`"false"` (Go
/// `AtomicBool`). The Go type is atomic for concurrent config reads; the
/// value semantics are what the config contract needs.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct AtomicBool(pub bool);

impl AtomicBool {
    /// Go `NewAtomicBool`.
    pub fn new(v: bool) -> AtomicBool {
        AtomicBool(v)
    }
    /// Go `Load`.
    pub fn load(&self) -> bool {
        self.0
    }
}

impl Serialize for AtomicBool {
    // Go `MarshalText`: "true" / "false".
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(if self.0 { "true" } else { "false" })
    }
}

impl<'de> Deserialize<'de> for AtomicBool {
    // Go `UnmarshalText`: "true" → true; "false"/""/"null" → false; else
    // error. Go's BurntSushi feeds a bare TOML bool as the text "true";
    // the Rust toml crate hands over a typed bool, so accept both forms.
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<AtomicBool, D::Error> {
        struct V;
        impl serde::de::Visitor<'_> for V {
            type Value = AtomicBool;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a bool or true/false string")
            }
            fn visit_bool<E>(self, v: bool) -> Result<AtomicBool, E> {
                Ok(AtomicBool(v))
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<AtomicBool, E> {
                match v {
                    "" | "null" | "false" => Ok(AtomicBool(false)),
                    "true" => Ok(AtomicBool(true)),
                    other => Err(E::custom(format!("Invalid value for bool type: {other}"))),
                }
            }
            fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<AtomicBool, E> {
                Err(E::custom(format!("Invalid value for bool type: {v}")))
            }
            fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<AtomicBool, E> {
                Err(E::custom(format!("Invalid value for bool type: {v}")))
            }
        }
        deserializer.deserialize_any(V)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Serialize, Deserialize)]
    struct AbData {
        ab: AtomicBool,
    }

    // Go TestAtomicBoolUnmarshal (value contract, not BurntSushi error text).
    #[test]
    fn atomic_bool() {
        let d: AbData = toml::from_str("ab=true").unwrap();
        assert!(d.ab.load());
        assert_eq!(toml::to_string(&d).unwrap(), "ab = \"true\"\n");

        let d: AbData = toml::from_str("ab=false").unwrap();
        assert!(!d.ab.load());
        assert_eq!(toml::to_string(&d).unwrap(), "ab = \"false\"\n");

        // A non-string TOML value is rejected.
        assert!(toml::from_str::<AbData>("ab = 1").is_err());
    }

    // Go TestNullableBoolUnmarshal (JSON round-trips).
    #[test]
    fn nullable_bool_json() {
        for (nb, expect) in [(NB_UNSET, "null"), (NB_FALSE, "false"), (NB_TRUE, "true")] {
            let data = serde_json::to_string(&nb).unwrap();
            assert_eq!(data, expect);
            let back: NullableBool = serde_json::from_str(&data).unwrap();
            assert_eq!(back, nb);
        }

        // JSON object field round-trips (as in the Log-struct part of the
        // Go test).
        let nb: NullableBool = serde_json::from_str("false").unwrap();
        assert_eq!(nb, NB_FALSE);
        let nb: NullableBool = serde_json::from_str("null").unwrap();
        assert_eq!(nb, NB_UNSET);
        // A bare JSON number is rejected (the marshaled round-trip only
        // produces null/true/false, so this path is never hit in practice).
        assert!(serde_json::from_str::<NullableBool>("1").is_err());
    }

    #[test]
    fn nullable_bool_text() {
        assert_eq!(NullableBool::from_text("true").unwrap(), NB_TRUE);
        assert_eq!(NullableBool::from_text("false").unwrap(), NB_FALSE);
        assert_eq!(NullableBool::from_text("").unwrap(), NB_UNSET);
        assert_eq!(NullableBool::from_text("null").unwrap(), NB_UNSET);
        assert!(NullableBool::from_text("1").is_err());
    }
}
