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

//! The class/code identity of a TiDB `*terror.Error` and Go's class
//! registry (`ErrClass`, `ErrCode`, `RegisterErrorClass`, `RFCCodeText`).
//!
//! Not a Go package: Go keeps these in `pkg/parser/terror`. They are their own
//! crate because the Rust `parser/mysql` error view (`SqlError`) carries the
//! identity of the `*terror.Error` it came from -- Rust passes that view
//! where Go passes the `error` -- so the identity sits beneath both
//! `parser/mysql`'s errors and `parser/terror`.

mod go_error_codes;

use std::backtrace::Backtrace;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::{LazyLock, RwLock, RwLockReadGuard, RwLockWriteGuard};

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
        // Go registers every error at package `init`, so `ToSQLError` and
        // the RFC-code lookups never depend on which errors were used first.
        let mut classes_by_description = HashMap::new();
        let mut codes: HashMap<TerrorClass, HashSet<TerrorCode>> = HashMap::new();
        for &(class, code) in crate::go_error_codes::GO_ERROR_CODES {
            let class = TerrorClass::from_value(class);
            if let Some(description) = descriptions.get(&class) {
                classes_by_description.insert(description.clone(), class);
            }
            codes.entry(class).or_default().insert(TerrorCode::new(code));
        }
        Self {
            descriptions,
            classes_by_description,
            codes,
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


/// Reports whether error-code registration is frozen.
pub fn registration_frozen() -> bool {
    registry_read().frozen
}

/// Records `code` under `class` (Go `ErrClass.NewStdErr`'s registration).
pub fn register_error_code(class: TerrorClass, code: TerrorCode) {
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

/// The class an RFC code text (`description:code`) names.
pub fn class_for_rfc_code(rfc_code: &str) -> Option<TerrorClass> {
    let (description, _) = rfc_code.split_once(':')?;
    if description.is_empty() {
        return None;
    }
    registry_read()
        .classes_by_description
        .get(description)
        .copied()
}


/// Freezes registration (the tail of Go's `RegisterFinish`).
pub fn freeze_registration() {
    registry_write().frozen = true;
}

/// Whether `code` was registered under `class`.
pub fn is_registered_code(class: TerrorClass, code: TerrorCode) -> bool {
    registry_read()
        .codes
        .get(&class)
        .is_some_and(|codes| codes.contains(&code))
}

/// A code within one [`TerrorClass`].
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TerrorCode(isize);

impl TerrorCode {
    /// Creates a source error code across Go's complete machine-width `int` domain.
    pub const fn new(value: isize) -> Self {
        Self(value)
    }

    /// Returns the complete source machine-width integer value.
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
    pub const fn code(self) -> isize {
        self.0
    }

    /// Creates an arbitrary source class without registering a description.
    pub const fn from_value(value: isize) -> Self {
        Self(value)
    }

    /// Source RFC class description, or the decimal class code if unregistered.
    pub fn description(self) -> String {
        registry_read()
            .descriptions
            .get(&self)
            .cloned()
            .unwrap_or_else(|| self.code().to_string())
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
    pub const fn new(class: TerrorClass, code: TerrorCode) -> Self {
        Self { class, code }
    }

    /// Returns the error class.
    pub const fn class(self) -> TerrorClass {
        self.class
    }

    /// Returns the class-local error code.
    pub const fn code(self) -> TerrorCode {
        self.code
    }

    /// Returns the source RFC identity text.
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

