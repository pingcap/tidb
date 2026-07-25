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

//! Complete transcreation of Go `pkg/util/dbterror` (`terror.go` +
//! `ddl_terror.go`): the typed error classes shared by TiDB subsystems and
//! the full DDL error catalog.
//!
//! `terror.go` wraps `parser/terror.ErrClass` with the subsystem class
//! constants and `NewStd`, which resolves the standard message for an error
//! code from `pkg/errno`'s catalog — in this workspace that superset catalog
//! (standard MySQL codes plus TiDB's 8xxx range) is `tidb_error::tidb`, not
//! the `parser/mysql`-only `tidb_error::mysql` one.
//!
//! `ddl_terror.go` is a 228-entry table of DDL error prototypes. The Rust
//! table in [`ddl_errors`] was generated mechanically from the Go source and
//! is verified entry-by-entry — code, RFC identity, and message template —
//! against `dbterror_go_fixture.txt`, a dump produced by executing the REAL
//! Go package (each variable's `Code()`/`RFCCode()`/`GetMsg()`), so every
//! `fmt.Sprintf`-composed template and cross-code definition is checked
//! byte-for-byte rather than re-derived by hand.

mod ddl_errors;

pub use ddl_errors::*;

use tidb_error::terror::{TerrorClass, TerrorCode, TerrorError};
use tidb_error::ErrMessage;

/// A class of errors (Go `dbterror.ErrClass`, wrapping `terror.ErrClass`).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ErrClass(pub TerrorClass);

/// `ClassAutoid`.
pub const CLASS_AUTOID: ErrClass = ErrClass(TerrorClass::Autoid);
/// `ClassDDL`.
pub const CLASS_DDL: ErrClass = ErrClass(TerrorClass::Ddl);
/// `ClassDomain`.
pub const CLASS_DOMAIN: ErrClass = ErrClass(TerrorClass::Domain);
/// `ClassExecutor`.
pub const CLASS_EXECUTOR: ErrClass = ErrClass(TerrorClass::Executor);
/// `ClassExpression`.
pub const CLASS_EXPRESSION: ErrClass = ErrClass(TerrorClass::Expression);
/// `ClassAdmin`.
pub const CLASS_ADMIN: ErrClass = ErrClass(TerrorClass::Admin);
/// `ClassKV`.
pub const CLASS_KV: ErrClass = ErrClass(TerrorClass::Kv);
/// `ClassMeta`.
pub const CLASS_META: ErrClass = ErrClass(TerrorClass::Meta);
/// `ClassOptimizer`.
pub const CLASS_OPTIMIZER: ErrClass = ErrClass(TerrorClass::Optimizer);
/// `ClassPrivilege`.
pub const CLASS_PRIVILEGE: ErrClass = ErrClass(TerrorClass::Privilege);
/// `ClassSchema`.
pub const CLASS_SCHEMA: ErrClass = ErrClass(TerrorClass::Schema);
/// `ClassServer`.
pub const CLASS_SERVER: ErrClass = ErrClass(TerrorClass::Server);
/// `ClassStructure`.
pub const CLASS_STRUCTURE: ErrClass = ErrClass(TerrorClass::Structure);
/// `ClassVariable`.
pub const CLASS_VARIABLE: ErrClass = ErrClass(TerrorClass::Variable);
/// `ClassXEval`.
pub const CLASS_XEVAL: ErrClass = ErrClass(TerrorClass::XEval);
/// `ClassTable`.
pub const CLASS_TABLE: ErrClass = ErrClass(TerrorClass::Table);
/// `ClassTypes`.
pub const CLASS_TYPES: ErrClass = ErrClass(TerrorClass::Types);
/// `ClassJSON`.
pub const CLASS_JSON: ErrClass = ErrClass(TerrorClass::Json);
/// `ClassTiKV`.
pub const CLASS_TIKV: ErrClass = ErrClass(TerrorClass::TiKv);
/// `ClassSession`.
pub const CLASS_SESSION: ErrClass = ErrClass(TerrorClass::Session);
/// `ClassPlugin`.
pub const CLASS_PLUGIN: ErrClass = ErrClass(TerrorClass::Plugin);
/// `ClassUtil`.
pub const CLASS_UTIL: ErrClass = ErrClass(TerrorClass::Util);

/// Resolves the standard `pkg/errno` catalog message (raw text + redaction
/// positions) for an error code. This is Go's `errno.MySQLErrName[code]`.
fn catalog_message(code: u16) -> ErrMessage {
    *tidb_error::tidb::message_by_code(code)
        .expect("dbterror code exists in the errno message catalog")
}

impl ErrClass {
    /// `NewStd`: creates the error with the standard catalog message for the
    /// code. Like the source, intended for global initializers.
    #[must_use]
    pub fn new_std(&self, code: u16) -> TerrorError {
        self.new_std_err(code, catalog_message(code))
    }

    /// `NewStdErr`: creates the error with an explicit catalog message
    /// (retaining its redaction metadata).
    #[must_use]
    pub fn new_std_err(&self, code: u16, message: ErrMessage) -> TerrorError {
        TerrorError::registered_standard(self.0, TerrorCode::new(code as isize), message)
    }

    /// `NewStdErr` with a plain composed message (Go's
    /// `parser_mysql.Message(text, nil)`, carrying no redaction positions).
    #[must_use]
    pub fn new_plain_err(&self, code: u16, message: &'static str) -> TerrorError {
        TerrorError::registered(self.0, TerrorCode::new(code as isize), message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_error::mysql::{set_redaction_mode, FormatArg, RedactionMode};
    use tidb_error::tidb::errcode;

    /// Every generated DDL error must match the identity and message dumped
    /// from the REAL Go package variable (`Code()`/`RFCCode()`/`GetMsg()`).
    #[test]
    fn ddl_errors_match_go_fixture() {
        let fixture = include_str!("dbterror_go_fixture.txt");
        let mut expected = std::collections::HashMap::new();
        for line in fixture.lines() {
            let mut parts = line.splitn(4, '\u{1f}');
            let name = parts.next().unwrap();
            let code: isize = parts.next().unwrap().parse().unwrap();
            let rfc = parts.next().unwrap();
            let msg = parts.next().unwrap();
            expected.insert(name, (code, rfc, msg));
        }

        let entries = ddl_errors::fixture_entries();
        assert_eq!(
            entries.len(),
            expected.len(),
            "every Go dbterror variable must have a Rust counterpart"
        );
        for (go_name, err) in entries {
            let (code, rfc, msg) = expected
                .remove(go_name)
                .unwrap_or_else(|| panic!("{go_name} missing from fixture"));
            assert_eq!(err.code().value(), code, "{go_name} code");
            assert_eq!(err.rfc_code(), rfc, "{go_name} rfc");
            assert_eq!(err.message(), msg, "{go_name} message");
        }
        assert!(expected.is_empty(), "unported entries: {expected:?}");
    }

    /// Go `TestErrorRedact`: NewStd errors formatted by args redact exactly
    /// the catalog's sensitive positions in both Enabled (`?`) and Marker
    /// modes. Go's test uses the zero `ErrClass{}`; class identity is
    /// irrelevant to redaction, and this workspace's terror registry requires
    /// a registered class, so `CLASS_DDL` stands in.
    #[test]
    fn error_redact() {
        struct Case {
            code: u16,
            args: &'static [&'static str],
            // Expected argument renderings under RedactionMode::Enabled.
            enabled: &'static [&'static str],
        }
        const SENSITIVE: &str = "sensitive_data";
        const PLAIN: &str = "no_sensitive";
        let cases = [
            Case {
                code: errcode::ErrDupEntry,
                args: &[SENSITIVE, PLAIN],
                enabled: &["?", PLAIN],
            },
            Case {
                code: errcode::ErrCutValueGroupConcat,
                args: &[SENSITIVE],
                enabled: &["?"],
            },
            Case {
                code: errcode::ErrDuplicatedValueInType,
                args: &[PLAIN, SENSITIVE],
                enabled: &[PLAIN, "?"],
            },
            Case {
                code: errcode::ErrTruncatedWrongValue,
                args: &[PLAIN, SENSITIVE],
                enabled: &[PLAIN, "?"],
            },
            Case {
                code: errcode::ErrInvalidCharacterString,
                args: &[PLAIN, SENSITIVE],
                enabled: &[PLAIN, "?"],
            },
            Case {
                code: errcode::ErrTruncatedWrongValueForField,
                args: &[SENSITIVE, SENSITIVE],
                enabled: &["?", "?"],
            },
            Case {
                code: errcode::ErrIllegalValueForType,
                args: &[PLAIN, SENSITIVE],
                enabled: &[PLAIN, "?"],
            },
            Case {
                code: errcode::ErrPartitionWrongValues,
                args: &[PLAIN, SENSITIVE],
                enabled: &[PLAIN, "?"],
            },
            Case {
                code: errcode::ErrNoParts,
                args: &[SENSITIVE],
                enabled: &["?"],
            },
            Case {
                code: errcode::ErrWrongValue,
                args: &[PLAIN, SENSITIVE],
                enabled: &[PLAIN, "?"],
            },
            Case {
                code: errcode::ErrNoPartitionForGivenValue,
                args: &[SENSITIVE],
                enabled: &["?"],
            },
            Case {
                code: errcode::ErrDataOutOfRange,
                args: &[PLAIN, SENSITIVE],
                enabled: &[PLAIN, "?"],
            },
            Case {
                code: errcode::ErrRowInWrongPartition,
                args: &[SENSITIVE],
                enabled: &["?"],
            },
            Case {
                code: errcode::ErrInvalidJSONText,
                args: &[SENSITIVE],
                enabled: &["?"],
            },
            Case {
                code: errcode::ErrTxnRetryable,
                args: &[SENSITIVE],
                enabled: &["?"],
            },
            Case {
                code: errcode::ErrIncorrectDatetimeValue,
                args: &[SENSITIVE],
                enabled: &["?"],
            },
            Case {
                code: errcode::ErrInvalidTimeFormat,
                args: &[SENSITIVE],
                enabled: &["?"],
            },
            Case {
                code: errcode::ErrRowNotFound,
                args: &[SENSITIVE],
                enabled: &["?"],
            },
            Case {
                code: errcode::ErrWriteConflict,
                args: &[PLAIN, PLAIN, PLAIN, SENSITIVE],
                enabled: &[PLAIN, PLAIN, PLAIN, "?"],
            },
        ];

        for case in &cases {
            let prototype = CLASS_DDL.new_std(case.code);
            let template = prototype.message().to_string();
            let args: Vec<FormatArg> = case.args.iter().map(|a| FormatArg::from(*a)).collect();

            set_redaction_mode(RedactionMode::Enabled);
            let msg = prototype
                .fast_generate(&template, &args)
                .message()
                .to_string();
            for want in case.enabled {
                assert!(
                    msg.contains(want),
                    "code {}: {msg:?} lacks {want:?}",
                    case.code
                );
            }
            assert!(
                !msg.contains("sensitive_data"),
                "code {}: {msg:?} leaks the sensitive value",
                case.code
            );

            set_redaction_mode(RedactionMode::Marker);
            let msg = prototype
                .fast_generate(&template, &args)
                .message()
                .to_string();
            assert!(
                msg.contains("\u{2039}sensitive_data\u{203a}"),
                "code {}: {msg:?} lacks the redaction marker",
                case.code
            );
        }
        set_redaction_mode(RedactionMode::Disabled);
    }
}
