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

//! Stable error class, code, RFC identity, and equality authority translated
//! from the dependency-ready portion of `pkg/parser/terror/terror.go`.
//!
//! TiDB's Go implementation builds these identities through mutable package
//! registration during initialization. Rust consumers use the closed source
//! class set directly, making duplicate registration and initialization order
//! impossible while preserving the externally observed `class:code` contract.

use std::borrow::Cow;
use std::error::Error;
use std::fmt;

use crate::mysql::{errcode, mysql_state, FormatArg, SqlError};
use crate::ErrMessage;

/// Source `CodeUnknown`.
pub const CODE_UNKNOWN: TerrorCode = TerrorCode::new(-1);
/// Source `CodeExecResultIsEmpty`.
pub const CODE_EXEC_RESULT_IS_EMPTY: TerrorCode = TerrorCode::new(3);
/// Source `CodeMissConnectionID`.
pub const CODE_MISS_CONNECTION_ID: TerrorCode = TerrorCode::new(1);
/// Source `CodeResultUndetermined`.
pub const CODE_RESULT_UNDETERMINED: TerrorCode = TerrorCode::new(2);

/// A code within one [`TerrorClass`].
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TerrorCode(i32);

impl TerrorCode {
    /// Creates a source error code.
    #[must_use]
    pub const fn new(value: i32) -> Self {
        Self(value)
    }

    /// Returns the source integer value.
    #[must_use]
    pub const fn value(self) -> i32 {
        self.0
    }
}

/// The complete fixed class set registered by `pkg/parser/terror/terror.go`.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(i16)]
pub enum TerrorClass {
    /// `ClassAutoid`.
    Autoid = 1,
    /// `ClassDDL`.
    Ddl = 2,
    /// `ClassDomain`.
    Domain = 3,
    /// `ClassEvaluator`.
    Evaluator = 4,
    /// `ClassExecutor`.
    Executor = 5,
    /// `ClassExpression`.
    Expression = 6,
    /// `ClassAdmin`.
    Admin = 7,
    /// `ClassKV`.
    Kv = 8,
    /// `ClassMeta`.
    Meta = 9,
    /// `ClassOptimizer` (RFC description `planner`).
    Optimizer = 10,
    /// `ClassParser`.
    Parser = 11,
    /// `ClassPerfSchema`.
    PerfSchema = 12,
    /// `ClassPrivilege`.
    Privilege = 13,
    /// `ClassSchema`.
    Schema = 14,
    /// `ClassServer`.
    Server = 15,
    /// `ClassStructure`.
    Structure = 16,
    /// `ClassVariable`.
    Variable = 17,
    /// `ClassXEval`.
    XEval = 18,
    /// `ClassTable`.
    Table = 19,
    /// `ClassTypes`.
    Types = 20,
    /// `ClassGlobal`.
    Global = 21,
    /// `ClassMockTikv`.
    MockTiKv = 22,
    /// `ClassJSON`.
    Json = 23,
    /// `ClassTiKV`.
    TiKv = 24,
    /// `ClassSession`.
    Session = 25,
    /// `ClassPlugin`.
    Plugin = 26,
    /// `ClassUtil`.
    Util = 27,
}

impl TerrorClass {
    /// All source classes in registration order.
    pub const ALL: [Self; 27] = [
        Self::Autoid,
        Self::Ddl,
        Self::Domain,
        Self::Evaluator,
        Self::Executor,
        Self::Expression,
        Self::Admin,
        Self::Kv,
        Self::Meta,
        Self::Optimizer,
        Self::Parser,
        Self::PerfSchema,
        Self::Privilege,
        Self::Schema,
        Self::Server,
        Self::Structure,
        Self::Variable,
        Self::XEval,
        Self::Table,
        Self::Types,
        Self::Global,
        Self::MockTiKv,
        Self::Json,
        Self::TiKv,
        Self::Session,
        Self::Plugin,
        Self::Util,
    ];

    /// Source class number.
    #[must_use]
    pub const fn code(self) -> i16 {
        self as i16
    }

    /// Source RFC class description.
    #[must_use]
    pub const fn description(self) -> &'static str {
        match self {
            Self::Autoid => "autoid",
            Self::Ddl => "ddl",
            Self::Domain => "domain",
            Self::Evaluator => "evaluator",
            Self::Executor => "executor",
            Self::Expression => "expression",
            Self::Admin => "admin",
            Self::Kv => "kv",
            Self::Meta => "meta",
            Self::Optimizer => "planner",
            Self::Parser => "parser",
            Self::PerfSchema => "perfschema",
            Self::Privilege => "privilege",
            Self::Schema => "schema",
            Self::Server => "server",
            Self::Structure => "structure",
            Self::Variable => "variable",
            Self::XEval => "xeval",
            Self::Table => "table",
            Self::Types => "types",
            Self::Global => "global",
            Self::MockTiKv => "mocktikv",
            Self::Json => "json",
            Self::TiKv => "tikv",
            Self::Session => "session",
            Self::Plugin => "plugin",
            Self::Util => "util",
        }
    }

    /// Source `EqualClass`, following context wrappers to their root cause.
    #[must_use]
    pub fn equal_class(self, error: Option<&(dyn Error + 'static)>) -> bool {
        error
            .map(root_cause)
            .and_then(|error| error.downcast_ref::<TerrorError>())
            .is_some_and(|error| error.class() == self)
    }

    /// Source `NotEqualClass`, including the nil-error case.
    #[must_use]
    pub fn not_equal_class(self, error: Option<&(dyn Error + 'static)>) -> bool {
        !self.equal_class(error)
    }
}

impl fmt::Display for TerrorClass {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.description())
    }
}

/// Stable identity used by `errors.RFCCodeText("class:code")` in Go.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TerrorIdentity {
    class: TerrorClass,
    code: TerrorCode,
}

impl TerrorIdentity {
    /// Creates a class/code identity.
    #[must_use]
    pub const fn new(class: TerrorClass, code: TerrorCode) -> Self {
        Self { class, code }
    }

    /// Returns the error class.
    #[must_use]
    pub const fn class(self) -> TerrorClass {
        self.class
    }

    /// Returns the class-local error code.
    #[must_use]
    pub const fn code(self) -> TerrorCode {
        self.code
    }

    /// Returns the source RFC identity text.
    #[must_use]
    pub fn rfc_code(self) -> String {
        format!("{}:{}", self.class, self.code.value())
    }
}

impl fmt::Display for TerrorIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}:{}", self.class, self.code.value())
    }
}

/// A generated TiDB terror error.
#[derive(Clone, Debug)]
pub struct TerrorError {
    identity: TerrorIdentity,
    message: Cow<'static, str>,
    redact_arg_pos: &'static [usize],
    registered: bool,
}

impl TerrorError {
    /// Defines a registered source error prototype, corresponding to `New` or
    /// `NewStdErr` during package initialization.
    #[must_use]
    pub const fn registered(class: TerrorClass, code: TerrorCode, message: &'static str) -> Self {
        Self {
            identity: TerrorIdentity::new(class, code),
            message: Cow::Borrowed(message),
            redact_arg_pos: &[],
            registered: true,
        }
    }

    /// Source `NewStdErr`, retaining the shared catalog's redaction metadata
    /// for every generated message.
    #[must_use]
    pub const fn registered_standard(
        class: TerrorClass,
        code: TerrorCode,
        message: ErrMessage,
    ) -> Self {
        Self {
            identity: TerrorIdentity::new(class, code),
            message: Cow::Borrowed(message.raw),
            redact_arg_pos: message.redact_arg_pos,
            registered: true,
        }
    }

    /// Source `Synthesize`: creates an identity without registering its code
    /// for protocol conversion.
    #[must_use]
    pub fn synthesize(class: TerrorClass, code: TerrorCode, message: impl Into<String>) -> Self {
        Self {
            identity: TerrorIdentity::new(class, code),
            message: Cow::Owned(message.into()),
            redact_arg_pos: &[],
            registered: false,
        }
    }

    /// Generates a contextual message while retaining the prototype identity.
    #[must_use]
    pub fn generate(&self, message: impl Into<String>) -> Self {
        Self {
            identity: self.identity,
            message: Cow::Owned(message.into()),
            redact_arg_pos: self.redact_arg_pos,
            registered: self.registered,
        }
    }

    /// Source `FastGen` formatting over the shared Go-format authority.
    #[must_use]
    pub fn fast_generate(&self, format: &str, arguments: &[FormatArg]) -> Self {
        let formatted =
            SqlError::new_f(self.protocol_code(), format, self.redact_arg_pos, arguments);
        self.generate(formatted.message)
    }

    /// Returns the stable class/code identity.
    #[must_use]
    pub const fn identity(&self) -> TerrorIdentity {
        self.identity
    }

    /// Returns the source error class.
    #[must_use]
    pub const fn class(&self) -> TerrorClass {
        self.identity.class()
    }

    /// Returns the source class-local error code.
    #[must_use]
    pub const fn code(&self) -> TerrorCode {
        self.identity.code()
    }

    /// Returns the generated message without the RFC prefix.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns the source RFC identity text.
    #[must_use]
    pub fn rfc_code(&self) -> String {
        self.identity.rfc_code()
    }

    /// Identity equality used by Go's generated terror errors.
    #[must_use]
    pub fn equal(&self, other: Option<&(dyn Error + 'static)>) -> bool {
        other
            .map(root_cause)
            .and_then(|other| other.downcast_ref::<Self>())
            .is_some_and(|other| self.identity == other.identity)
    }

    /// Source `ToSQLError`. Synthesized/unregistered identities deliberately
    /// use `ErrUnknown`, matching `getMySQLErrorCode` fallback behavior.
    #[must_use]
    pub fn to_sql_error(&self) -> SqlError {
        let code = self.protocol_code();
        SqlError {
            code,
            message: self.message.to_string(),
            state: mysql_state(code),
        }
    }

    fn protocol_code(&self) -> u16 {
        if !self.registered && !is_fixed_registered_identity(self.identity) {
            return errcode::ErrUnknown;
        }
        u16::try_from(self.code().value()).unwrap_or(errcode::ErrUnknown)
    }
}

impl PartialEq for TerrorError {
    fn eq(&self, other: &Self) -> bool {
        self.identity == other.identity
    }
}

impl Eq for TerrorError {}

impl fmt::Display for TerrorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "[{}]{}", self.identity, self.message)
    }
}

impl Error for TerrorError {}

/// Source `ErrCritical` prototype.
pub const ERR_CRITICAL: TerrorError = TerrorError::registered(
    TerrorClass::Global,
    CODE_EXEC_RESULT_IS_EMPTY,
    "critical error %v",
);

/// Source `ErrResultUndetermined` prototype.
pub const ERR_RESULT_UNDETERMINED: TerrorError = TerrorError::registered(
    TerrorClass::Global,
    CODE_RESULT_UNDETERMINED,
    "execution result undetermined",
);

/// Source `ErrorEqual`, including root-cause traversal and RFC identity rules.
#[must_use]
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
        (Some(left), Some(right)) => left.identity == right.identity,
        _ => left.to_string() == right.to_string(),
    }
}

fn root_cause<'a>(mut error: &'a (dyn Error + 'static)) -> &'a (dyn Error + 'static) {
    while let Some(source) = error.source() {
        error = source;
    }
    error
}

fn is_fixed_registered_identity(identity: TerrorIdentity) -> bool {
    matches!(
        (identity.class(), identity.code()),
        (TerrorClass::Global, CODE_EXEC_RESULT_IS_EMPTY)
            | (TerrorClass::Global, CODE_RESULT_UNDETERMINED)
    )
}
