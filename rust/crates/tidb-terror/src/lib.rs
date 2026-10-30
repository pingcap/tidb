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

//! Complete Rust-native transcreation of `pkg/parser/terror/terror.go`.
//!
//! The package owns machine-width class/code domains, initialization-time
//! registration and freeze behavior, RFC identities, MySQL conversion,
//! compatible JSON, equality, logging/termination helpers, and stack capture.

pub mod catalog_support;
mod mysql_error;

pub use mysql_error::MysqlError;

use std::backtrace::Backtrace;
use std::borrow::Cow;
use std::error::Error;
use std::fmt;
use std::process;
use std::sync::{Arc, LazyLock};

use tidb_mysql::errors::{errcode, mysql_state, FormatArg, SqlError};
use tidb_mysql::errors::ErrMessage;
pub use tidb_terror_identity::{
    class_for_rfc_code, register_error_class, registration_frozen, TerrorClass, TerrorCode,
    TerrorIdentity,
};
use tidb_terror_identity::{is_registered_code, register_error_code};
use serde::de::{Error as SerdeError, Unexpected};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Source `RegisterFinish`: forces the package-level errors, then freezes
/// registration.
pub fn register_finish() {
    LazyLock::force(&ERR_CRITICAL);
    LazyLock::force(&ERR_RESULT_UNDETERMINED);
    tidb_terror_identity::freeze_registration();
}

/// Source `CodeUnknown`.
pub const CODE_UNKNOWN: TerrorCode = TerrorCode::new(-1);
/// Source `CodeExecResultIsEmpty`.
pub const CODE_EXEC_RESULT_IS_EMPTY: TerrorCode = TerrorCode::new(3);
/// Source `CodeMissConnectionID`.
pub const CODE_MISS_CONNECTION_ID: TerrorCode = TerrorCode::new(1);
/// Source `CodeResultUndetermined`.
pub const CODE_RESULT_UNDETERMINED: TerrorCode = TerrorCode::new(2);

/// A generated TiDB terror error.
#[derive(Clone, Debug)]
pub struct TerrorError {
    identity: TerrorIdentity,
    rfc_code: Cow<'static, str>,
    message: Cow<'static, str>,
    redact_arg_pos: &'static [usize],
    stack: Option<Arc<Backtrace>>,
}

impl TerrorError {
    /// Constructs the compatibility form used by `pingcap/errors` without an RFC code.
    pub fn compatible(code: TerrorCode, message: impl Into<String>) -> Self {
        Self {
            identity: TerrorIdentity::new(TerrorClass::from_value(0), code),
            rfc_code: Cow::Borrowed(""),
            message: Cow::Owned(message.into()),
            redact_arg_pos: &[],
            stack: None,
        }
    }

    /// Defines a registered source error prototype, corresponding to `New` or
    /// `NewStdErr` during package initialization.
    pub fn registered(class: TerrorClass, code: TerrorCode, message: impl Into<String>) -> Self {
        register_error_code(class, code);
        Self {
            identity: TerrorIdentity::new(class, code),
            rfc_code: Cow::Owned(TerrorIdentity::new(class, code).rfc_code()),
            message: Cow::Owned(message.into()),
            redact_arg_pos: &[],
            stack: None,
        }
    }

    /// Source `NewStdErr`, retaining the shared catalog's redaction metadata
    /// for every generated message.
    pub fn registered_standard(class: TerrorClass, code: TerrorCode, message: ErrMessage) -> Self {
        register_error_code(class, code);
        Self {
            identity: TerrorIdentity::new(class, code),
            rfc_code: Cow::Owned(TerrorIdentity::new(class, code).rfc_code()),
            message: Cow::Borrowed(message.raw),
            redact_arg_pos: message.redact_arg_pos,
            stack: None,
        }
    }

    /// Source `NewStd`, resolving the complete checked MySQL message catalog.
    pub fn registered_from_catalog(class: TerrorClass, code: TerrorCode) -> Self {
        let protocol_code = u16::try_from(code.value())
            .expect("NewStd error code must fit the MySQL uint16 catalog domain");
        let message = tidb_mysql::errors::message_by_code(protocol_code)
            .copied()
            .expect("NewStd error code must exist in the MySQL message catalog");
        Self::registered_standard(class, code, message)
    }


    /// Source `Synthesize`: creates an identity without registering its code
    /// for protocol conversion.
    pub fn synthesize(class: TerrorClass, code: TerrorCode, message: impl Into<String>) -> Self {
        Self {
            identity: TerrorIdentity::new(class, code),
            rfc_code: Cow::Owned(TerrorIdentity::new(class, code).rfc_code()),
            message: Cow::Owned(message.into()),
            redact_arg_pos: &[],
            stack: None,
        }
    }

    /// Generates a contextual message while retaining the prototype identity.
    pub fn generate(&self, message: impl Into<String>) -> Self {
        Self {
            identity: self.identity,
            rfc_code: self.rfc_code.clone(),
            message: Cow::Owned(message.into()),
            redact_arg_pos: self.redact_arg_pos,
            stack: None,
        }
    }

    /// Source `GenWithStack`, using Rust's native captured backtrace.
    pub fn generate_with_stack(&self, message: impl Into<String>) -> Self {
        let mut generated = self.generate(message);
        generated.stack = Some(Arc::new(Backtrace::force_capture()));
        generated
    }

    /// Source `GenWithStackByArgs`: formats the prototype's catalog message,
    /// applying its precision and redaction rules before capturing a stack.
    pub fn generate_with_stack_by_args(&self, arguments: &[FormatArg]) -> Self {
        if arguments.is_empty() {
            // Go GetMsg returns the template verbatim when there are no args.
            return self.generate_with_stack(self.message());
        }
        let formatted = SqlError::new_f(
            self.protocol_code(),
            self.message(),
            self.redact_arg_pos,
            arguments,
        );
        self.generate_with_stack(formatted.message)
    }

    /// Source `FastGenByArgs`: formats the prototype's catalog message like
    /// [`Self::generate_with_stack_by_args`], without capturing a stack.
    pub fn fast_generate_by_args(&self, arguments: &[FormatArg]) -> Self {
        if arguments.is_empty() {
            return self.generate(self.message());
        }
        let formatted = SqlError::new_f(
            self.protocol_code(),
            self.message(),
            self.redact_arg_pos,
            arguments,
        );
        self.generate(formatted.message)
    }

    /// Source `FastGen` formatting over the shared Go-format authority.
    pub fn fast_generate(&self, format: &str, arguments: &[FormatArg]) -> Self {
        let formatted =
            SqlError::new_f(self.protocol_code(), format, self.redact_arg_pos, arguments);
        self.generate(formatted.message)
    }

    /// Returns the stable class/code identity.
    pub const fn identity(&self) -> TerrorIdentity {
        self.identity
    }

    /// Returns the source error class.
    pub const fn class(&self) -> TerrorClass {
        self.identity.class()
    }

    /// Returns the source class-local error code.
    pub const fn code(&self) -> TerrorCode {
        self.identity.code()
    }

    /// Returns the generated message without the RFC prefix.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns the source RFC identity text.
    pub fn rfc_code(&self) -> &str {
        &self.rfc_code
    }

    /// Returns the captured Rust backtrace for a stack-generating operation.
    pub fn stack(&self) -> Option<&Backtrace> {
        self.stack.as_deref()
    }

    /// Identity equality used by Go's generated terror errors.
    pub fn equal(&self, other: Option<&(dyn Error + 'static)>) -> bool {
        other
            .map(root_cause)
            .and_then(|other| other.downcast_ref::<Self>())
            .is_some_and(|other| self.error_id() == other.error_id())
    }

    /// Source `ToSQLError`. Synthesized/unregistered identities deliberately
    /// use `ErrUnknown`, matching `getMySQLErrorCode` fallback behavior.
    pub fn to_sql_error(&self) -> SqlError {
        let code = self.protocol_code();
        SqlError {
            code,
            message: self.message.to_string(),
            state: mysql_state(code),
            identity: Some(self.identity),
        }
    }

    fn protocol_code(&self) -> u16 {
        let Some(class) = class_for_rfc_code(self.rfc_code()) else {
            return errcode::ErrUnknown;
        };
        let registered = is_registered_code(class, self.code());
        if !registered {
            return errcode::ErrUnknown;
        }
        u16::try_from(self.code().value()).unwrap_or(errcode::ErrUnknown)
    }

    fn error_id(&self) -> Cow<'_, str> {
        if self.rfc_code.is_empty() {
            Cow::Owned(self.code().value().to_string())
        } else {
            Cow::Borrowed(&self.rfc_code)
        }
    }
}

impl PartialEq for TerrorError {
    fn eq(&self, other: &Self) -> bool {
        self.error_id() == other.error_id()
    }
}

impl Eq for TerrorError {}

impl fmt::Display for TerrorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let identity = if self.rfc_code.is_empty() {
            self.code().value().to_string()
        } else {
            self.rfc_code.to_string()
        };
        write!(formatter, "[{identity}]{}", self.message)
    }
}

impl Error for TerrorError {}

#[derive(Serialize)]
struct CompatibleJsonRef<'a> {
    class: isize,
    code: isize,
    message: &'a str,
    #[serde(rename = "rfccode")]
    rfc_code: &'a str,
}

#[derive(Deserialize)]
struct CompatibleJsonOwned {
    class: Option<isize>,
    code: Option<isize>,
    message: Option<String>,
    #[serde(rename = "rfccode")]
    rfc_code: Option<String>,
}

impl Serialize for TerrorError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        CompatibleJsonRef {
            class: legacy_class_for_rfc(self.rfc_code()),
            code: self.code().value(),
            message: self.message(),
            rfc_code: self.rfc_code(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for TerrorError {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        if !value.is_object() {
            return Err(D::Error::invalid_type(
                Unexpected::Other("non-object JSON value"),
                &"a TiDB error JSON object",
            ));
        }
        let compatible =
            serde_json::from_value::<CompatibleJsonOwned>(value).map_err(D::Error::custom)?;
        let class = compatible.class.unwrap_or_default();
        let code = compatible.code.unwrap_or_default();
        let rfc_code = compatible.rfc_code.unwrap_or_default();
        let rfc_code = if rfc_code.is_empty() && class > 0 {
            format!("{}:{}", legacy_rfc_class(class).unwrap_or_default(), code)
        } else {
            rfc_code
        };
        let class = class_for_rfc_code(&rfc_code).unwrap_or_else(|| TerrorClass::from_value(class));
        Ok(Self {
            identity: TerrorIdentity::new(class, TerrorCode::new(code)),
            rfc_code: Cow::Owned(rfc_code),
            message: Cow::Owned(compatible.message.unwrap_or_default()),
            redact_arg_pos: &[],
            stack: None,
        })
    }
}

fn legacy_rfc_class(class: isize) -> Option<&'static str> {
    const LEGACY_CLASSES: [&str; 27] = [
        "autoid",
        "ddl",
        "domain",
        "evaluator",
        "executor",
        "expression",
        "admin",
        "kv",
        "meta",
        "planner",
        "parser",
        "perfschema",
        "privilege",
        "schema",
        "server",
        "struct",
        "variable",
        "xeval",
        "table",
        "types",
        "global",
        "mocktikv",
        "json",
        "tikv",
        "session",
        "plugin",
        "util",
    ];
    usize::try_from(class - 1)
        .ok()
        .and_then(|index| LEGACY_CLASSES.get(index).copied())
}

fn legacy_class_for_rfc(rfc_code: &str) -> isize {
    let Some((class, _)) = rfc_code.split_once(':') else {
        return 0;
    };
    (1_isize..=27)
        .find(|candidate| legacy_rfc_class(*candidate) == Some(class))
        .unwrap_or(0)
}

/// Source `ErrCritical` prototype.
pub static ERR_CRITICAL: LazyLock<TerrorError> = LazyLock::new(|| {
    TerrorError::registered(
        TerrorClass::Global,
        CODE_EXEC_RESULT_IS_EMPTY,
        "critical error %v",
    )
});

/// Source `ErrResultUndetermined` prototype.
pub static ERR_RESULT_UNDETERMINED: LazyLock<TerrorError> = LazyLock::new(|| {
    TerrorError::registered(
        TerrorClass::Global,
        CODE_RESULT_UNDETERMINED,
        "execution result undetermined",
    )
});

/// Source `ErrorEqual`, including root-cause traversal and RFC identity rules.
pub fn terror_error_equal(
    left: Option<&(dyn Error + 'static)>,
    right: Option<&(dyn Error + 'static)>,
) -> bool {
    let (Some(left), Some(right)) = (left, right) else {
        return left.is_none() && right.is_none();
    };
    let left = root_cause(left);
    let right = root_cause(right);
    if std::ptr::eq(left, right) {
        return true;
    }
    match (
        left.downcast_ref::<TerrorError>(),
        right.downcast_ref::<TerrorError>(),
    ) {
        (Some(left), Some(right)) => left.error_id() == right.error_id(),
        _ => left.to_string() == right.to_string(),
    }
}

/// Source `GetErrClass`, resolved through the registered RFC class prefix.
pub fn get_error_class(error: &TerrorError) -> Option<TerrorClass> {
    class_for_rfc_code(error.rfc_code())
}

/// Source `Call`: execute a function and log its error without propagating it.
pub fn call<E>(function: impl FnOnce() -> Result<(), E>)
where
    E: fmt::Display,
{
    if let Err(error) = function() {
        tracing::error!(error = %error, stack = %Backtrace::force_capture(), "function call errored");
    }
}

/// Source `Log`: log a present error with a stack and ignore `None`.
pub fn log(error: Option<&(dyn Error + 'static)>) {
    if let Some(error) = error {
        tracing::error!(error = %error, stack = %Backtrace::force_capture(), "encountered error");
    }
}

/// Source `MustNil`: run cleanup in order, log, and terminate on an error.
pub fn must_nil(
    error: Option<&(dyn Error + 'static)>,
    cleanup: impl IntoIterator<Item = Box<dyn FnOnce()>>,
) {
    let Some(error) = error else {
        return;
    };
    for close in cleanup {
        close();
    }
    tracing::error!(error = %error, stack = %Backtrace::force_capture(), "unexpected error");
    process::exit(1);
}

/// `github.com/pingcap/errors.Cause`: the innermost error a chain wraps.
pub fn root_cause<'a>(mut error: &'a (dyn Error + 'static)) -> &'a (dyn Error + 'static) {
    while let Some(source) = error.source() {
        error = source;
    }
    error
}

#[cfg(test)]
mod registered_std_tests {
    use super::TerrorClass;
    use super::TerrorError;


    #[test]
    fn compatible_json_accepts_defaults_and_unknown_fields() {
        let decoded: TerrorError = serde_json::from_str(
            r#"{"class":21,"code":2,"message":"kept","rfccode":"","unknown":{"ignored":true}}"#,
        )
        .unwrap();
        assert_eq!(decoded.class(), TerrorClass::Global);
        assert_eq!(decoded.code().value(), 2);
        assert_eq!(decoded.message(), "kept");
        assert_eq!(decoded.rfc_code(), "global:2");

        let unknown_legacy: TerrorError = serde_json::from_str(r#"{"class":99,"code":7}"#).unwrap();
        assert_eq!(unknown_legacy.rfc_code(), ":7");

        for non_object in ["[]", r#""text""#, "1", "true"] {
            assert!(serde_json::from_str::<TerrorError>(non_object).is_err());
        }

        let nulls: TerrorError =
            serde_json::from_str(r#"{"class":null,"code":null,"message":null,"rfccode":null}"#)
                .unwrap();
        assert_eq!(nulls.class().code(), 0);
        assert_eq!(nulls.code().value(), 0);
        assert_eq!(nulls.message(), "");
        assert_eq!(nulls.rfc_code(), "");
    }
}

/// Source `ErrClass.EqualClass`, following context wrappers to their root cause.
pub fn equal_class(class: TerrorClass, error: Option<&(dyn Error + 'static)>) -> bool {
    error
        .map(root_cause)
        .and_then(|error| error.downcast_ref::<TerrorError>())
        .and_then(get_error_class)
        .is_some_and(|found| found == class)
}

/// Source `ErrClass.NotEqualClass`, including the nil-error case.
pub fn not_equal_class(class: TerrorClass, error: Option<&(dyn Error + 'static)>) -> bool {
    !equal_class(class, error)
}
