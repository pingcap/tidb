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

use std::backtrace::Backtrace;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;
use std::process;
use std::sync::{Arc, LazyLock, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::mysql::{errcode, mysql_state, FormatArg, SqlError};
use crate::ErrMessage;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

const FIXED_CLASSES: [(isize, &str); 27] = [
    (1, "autoid"),
    (2, "ddl"),
    (3, "domain"),
    (4, "evaluator"),
    (5, "executor"),
    (6, "expression"),
    (7, "admin"),
    (8, "kv"),
    (9, "meta"),
    (10, "planner"),
    (11, "parser"),
    (12, "perfschema"),
    (13, "privilege"),
    (14, "schema"),
    (15, "server"),
    (16, "structure"),
    (17, "variable"),
    (18, "xeval"),
    (19, "table"),
    (20, "types"),
    (21, "global"),
    (22, "mocktikv"),
    (23, "json"),
    (24, "tikv"),
    (25, "session"),
    (26, "plugin"),
    (27, "util"),
];

#[derive(Debug)]
struct Registry {
    descriptions: HashMap<TerrorClass, String>,
    classes_by_description: HashMap<String, TerrorClass>,
    codes: HashMap<TerrorClass, HashSet<TerrorCode>>,
    frozen: bool,
}

impl Registry {
    fn source_defaults() -> Self {
        let descriptions = FIXED_CLASSES
            .into_iter()
            .map(|(code, description)| (TerrorClass::from_value(code), description.to_owned()))
            .collect::<HashMap<_, _>>();
        Self {
            descriptions,
            classes_by_description: HashMap::new(),
            codes: HashMap::new(),
            frozen: false,
        }
    }
}

static REGISTRY: LazyLock<RwLock<Registry>> =
    LazyLock::new(|| RwLock::new(Registry::source_defaults()));

fn registry_read() -> RwLockReadGuard<'static, Registry> {
    REGISTRY
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn registry_write() -> RwLockWriteGuard<'static, Registry> {
    REGISTRY
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Registers a new error class and panics on a duplicate class code.
pub fn register_error_class(class_code: isize, description: impl Into<String>) -> TerrorClass {
    let description = description.into();
    let class = TerrorClass::from_value(class_code);
    let mut registry = registry_write();
    if registry.descriptions.contains_key(&class) {
        drop(registry);
        panic!("duplicate register ClassCode {class_code} - {description}");
    }
    registry.descriptions.insert(class, description);
    class
}

/// Prevents registration of any new error code, matching `RegisterFinish`.
pub fn register_finish() {
    LazyLock::force(&ERR_CRITICAL);
    LazyLock::force(&ERR_RESULT_UNDETERMINED);
    registry_write().frozen = true;
}

/// Reports whether error-code registration is frozen.
#[must_use]
pub fn registration_frozen() -> bool {
    registry_read().frozen
}

fn register_error_code(class: TerrorClass, code: TerrorCode) {
    let mut registry = registry_write();
    if registry.frozen {
        drop(registry);
        eprintln!("{}", Backtrace::force_capture());
        panic!("register error after initialized is prohibited");
    }
    let description = registry
        .descriptions
        .get(&class)
        .cloned()
        .unwrap_or_default();
    registry.classes_by_description.insert(description, class);
    registry.codes.entry(class).or_default().insert(code);
}

fn class_for_rfc_code(rfc_code: &str) -> Option<TerrorClass> {
    let (description, _) = rfc_code.split_once(':')?;
    if description.is_empty() {
        return None;
    }
    registry_read()
        .classes_by_description
        .get(description)
        .copied()
}

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
pub struct TerrorCode(isize);

impl TerrorCode {
    /// Creates a source error code across Go's complete machine-width `int` domain.
    #[must_use]
    pub const fn new(value: isize) -> Self {
        Self(value)
    }

    /// Returns the complete source machine-width integer value.
    #[must_use]
    pub const fn value(self) -> isize {
        self.0
    }
}

/// An error class across Go's complete machine-width `int` domain.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TerrorClass(isize);

#[allow(non_upper_case_globals)]
impl TerrorClass {
    /// `ClassAutoid`.
    pub const Autoid: Self = Self(1);
    /// `ClassDDL`.
    pub const Ddl: Self = Self(2);
    /// `ClassDomain`.
    pub const Domain: Self = Self(3);
    /// `ClassEvaluator`.
    pub const Evaluator: Self = Self(4);
    /// `ClassExecutor`.
    pub const Executor: Self = Self(5);
    /// `ClassExpression`.
    pub const Expression: Self = Self(6);
    /// `ClassAdmin`.
    pub const Admin: Self = Self(7);
    /// `ClassKV`.
    pub const Kv: Self = Self(8);
    /// `ClassMeta`.
    pub const Meta: Self = Self(9);
    /// `ClassOptimizer` (RFC description `planner`).
    pub const Optimizer: Self = Self(10);
    /// `ClassParser`.
    pub const Parser: Self = Self(11);
    /// `ClassPerfSchema`.
    pub const PerfSchema: Self = Self(12);
    /// `ClassPrivilege`.
    pub const Privilege: Self = Self(13);
    /// `ClassSchema`.
    pub const Schema: Self = Self(14);
    /// `ClassServer`.
    pub const Server: Self = Self(15);
    /// `ClassStructure`.
    pub const Structure: Self = Self(16);
    /// `ClassVariable`.
    pub const Variable: Self = Self(17);
    /// `ClassXEval`.
    pub const XEval: Self = Self(18);
    /// `ClassTable`.
    pub const Table: Self = Self(19);
    /// `ClassTypes`.
    pub const Types: Self = Self(20);
    /// `ClassGlobal`.
    pub const Global: Self = Self(21);
    /// `ClassMockTikv`.
    pub const MockTiKv: Self = Self(22);
    /// `ClassJSON`.
    pub const Json: Self = Self(23);
    /// `ClassTiKV`.
    pub const TiKv: Self = Self(24);
    /// `ClassSession`.
    pub const Session: Self = Self(25);
    /// `ClassPlugin`.
    pub const Plugin: Self = Self(26);
    /// `ClassUtil`.
    pub const Util: Self = Self(27);

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
    pub const fn code(self) -> isize {
        self.0
    }

    /// Creates an arbitrary source class without registering a description.
    #[must_use]
    pub const fn from_value(value: isize) -> Self {
        Self(value)
    }

    /// Source RFC class description, or the decimal class code if unregistered.
    #[must_use]
    pub fn description(self) -> String {
        registry_read()
            .descriptions
            .get(&self)
            .cloned()
            .unwrap_or_else(|| self.code().to_string())
    }

    /// Source `EqualClass`, following context wrappers to their root cause.
    #[must_use]
    pub fn equal_class(self, error: Option<&(dyn Error + 'static)>) -> bool {
        error
            .map(root_cause)
            .and_then(|error| error.downcast_ref::<TerrorError>())
            .and_then(get_error_class)
            .is_some_and(|class| class == self)
    }

    /// Source `NotEqualClass`, including the nil-error case.
    #[must_use]
    pub fn not_equal_class(self, error: Option<&(dyn Error + 'static)>) -> bool {
        !self.equal_class(error)
    }
}

impl fmt::Display for TerrorClass {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.description())
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
        let description = registry_read()
            .descriptions
            .get(&self.class)
            .cloned()
            .unwrap_or_default();
        format!("{description}:{}", self.code.value())
    }
}

impl fmt::Display for TerrorIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.rfc_code())
    }
}

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
    #[must_use]
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
    #[must_use]
    pub fn registered(class: TerrorClass, code: TerrorCode, message: &'static str) -> Self {
        register_error_code(class, code);
        Self {
            identity: TerrorIdentity::new(class, code),
            rfc_code: Cow::Owned(TerrorIdentity::new(class, code).rfc_code()),
            message: Cow::Borrowed(message),
            redact_arg_pos: &[],
            stack: None,
        }
    }

    /// Source `NewStdErr`, retaining the shared catalog's redaction metadata
    /// for every generated message.
    #[must_use]
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
    #[must_use]
    pub fn registered_from_catalog(class: TerrorClass, code: TerrorCode) -> Self {
        let protocol_code = u16::try_from(code.value())
            .expect("NewStd error code must fit the MySQL uint16 catalog domain");
        let message = crate::mysql::message_by_code(protocol_code)
            .copied()
            .expect("NewStd error code must exist in the MySQL message catalog");
        Self::registered_standard(class, code, message)
    }

    /// Source `NewStd` resolving from the full error registry: the MySQL
    /// catalog first, then the TiDB catalog.
    ///
    /// Go's global `MySQLErrName` map is populated from both catalogs, so
    /// `Class.NewStd` works for MySQL and TiDB-specific codes alike;
    /// [`registered_from_catalog`](Self::registered_from_catalog) covers only
    /// the MySQL half and panics on a TiDB code. This is the faithful `NewStd`
    /// for the many error-catalog packages (plannererrors, ...) whose codes
    /// span both halves.
    #[must_use]
    pub fn registered_std(class: TerrorClass, code: TerrorCode) -> Self {
        let protocol_code = u16::try_from(code.value())
            .expect("NewStd error code must fit the uint16 catalog domain");
        let message = crate::mysql::message_by_code(protocol_code)
            .or_else(|| crate::tidb::message_by_code(protocol_code))
            .copied()
            .expect("NewStd error code must exist in the MySQL or TiDB catalog");
        Self::registered_standard(class, code, message)
    }

    /// Source `Synthesize`: creates an identity without registering its code
    /// for protocol conversion.
    #[must_use]
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
    #[must_use]
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
    #[must_use]
    pub fn generate_with_stack(&self, message: impl Into<String>) -> Self {
        let mut generated = self.generate(message);
        generated.stack = Some(Arc::new(Backtrace::force_capture()));
        generated
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
    pub fn rfc_code(&self) -> &str {
        &self.rfc_code
    }

    /// Returns the captured Rust backtrace for a stack-generating operation.
    #[must_use]
    pub fn stack(&self) -> Option<&Backtrace> {
        self.stack.as_deref()
    }

    /// Identity equality used by Go's generated terror errors.
    #[must_use]
    pub fn equal(&self, other: Option<&(dyn Error + 'static)>) -> bool {
        other
            .map(root_cause)
            .and_then(|other| other.downcast_ref::<Self>())
            .is_some_and(|other| self.error_id() == other.error_id())
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
        let Some(class) = class_for_rfc_code(self.rfc_code()) else {
            return errcode::ErrUnknown;
        };
        let registered = registry_read()
            .codes
            .get(&class)
            .is_some_and(|codes| codes.contains(&self.code()));
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
    #[serde(default)]
    class: isize,
    #[serde(default)]
    code: isize,
    #[serde(default)]
    message: String,
    #[serde(default, rename = "rfccode")]
    rfc_code: String,
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
        let compatible = CompatibleJsonOwned::deserialize(deserializer)?;
        let rfc_code = if compatible.rfc_code.is_empty() && compatible.class > 0 {
            legacy_rfc_class(compatible.class)
                .map(|class| format!("{class}:{}", compatible.code))
                .unwrap_or_default()
        } else {
            compatible.rfc_code
        };
        let class = class_for_rfc_code(&rfc_code)
            .unwrap_or_else(|| TerrorClass::from_value(compatible.class));
        Ok(Self {
            identity: TerrorIdentity::new(class, TerrorCode::new(compatible.code)),
            rfc_code: Cow::Owned(rfc_code),
            message: Cow::Owned(compatible.message),
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
        (Some(left), Some(right)) => left.error_id() == right.error_id(),
        _ => left.to_string() == right.to_string(),
    }
}

/// Source `GetErrClass`, resolved through the registered RFC class prefix.
#[must_use]
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

fn root_cause<'a>(mut error: &'a (dyn Error + 'static)) -> &'a (dyn Error + 'static) {
    while let Some(source) = error.source() {
        error = source;
    }
    error
}

#[cfg(test)]
mod registered_std_tests {
    use super::{TerrorClass, TerrorCode, TerrorError};

    // registered_std resolves codes from both the MySQL and TiDB catalogs,
    // unlike registered_from_catalog (MySQL only) -- Go's NewStd behavior.
    #[test]
    fn resolves_both_catalogs() {
        // A MySQL-catalog code (ErrUnknown = 1105).
        let e = TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(1105));
        assert_eq!(e.code().value(), 1105);
        // A TiDB-only optimizer code (ErrCartesianProductUnsupported = 8110);
        // registered_from_catalog would panic on this, registered_std resolves it.
        let e = TerrorError::registered_std(TerrorClass::Optimizer, TerrorCode::new(8110));
        assert_eq!(e.code().value(), 8110);
    }
}
