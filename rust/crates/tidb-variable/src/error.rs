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

/// Why a variable statement failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VarError {
    /// Go `ErrUnknownSystemVar` (1193).
    UnknownSystemVariable(String),
    /// Go `ErrVariableNoLongerSupported` (8136), used by the expression
    /// read path for a removed system variable.
    RemovedSystemVariable { name: String, reason: String },
    /// Go `ErrIncorrectGlobalLocalVar` (1238): the variable is read-only.
    ReadOnlyVariable(String),
    /// Go `ErrWrongTypeForVar` (1232).
    WrongTypeForVar(String),
    /// Go `ErrWrongValueForVar` (1231).
    WrongValueForVar(String, String),
    /// A catalogued MySQL error returned unchanged by validation.
    SqlError(tidb_error::mysql::SqlError),
    /// Go `ErrLocalVariable` (1228): `SET GLOBAL` named a SESSION-only
    /// variable.
    SessionOnlyVariable(String),
    /// Go `ErrGlobalVariable` (1229): `SET SESSION` named a GLOBAL-only
    /// variable.
    GlobalOnlyVariable(String),
    /// Go `ErrIncorrectGlobalLocalVar` (1238), the read-side form: `SELECT
    /// @@global.x` named a SESSION-only variable (there is no GLOBAL copy to
    /// read).
    NoGlobalCopy(String),
    /// Go `ErrIncorrectScope` (1238), the expression rewriter's explicit read
    /// scope validation. The second field is Go's allowed-scope text, such as
    /// `GLOBAL` or `SESSION or GLOBAL`.
    IncorrectScope(String, &'static str),
    /// A `SysVar.Validation` closure's own `errors.Errorf`, whose wording is
    /// the whole error (Go gives it no code, so it reports as 1105).
    ValidationRefused(String),
}
