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

/// Why a session-variable statement failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VarErrorKind {
    /// Go `ErrUnknownSystemVar` (1193).
    UnknownSystemVariable(String),
    /// Go `ErrIncorrectGlobalLocalVar` (1238): the variable is read-only.
    ReadOnlyVariable(String),
    /// Go `ErrWrongTypeForVar` (1232).
    WrongTypeForVar(String),
    /// Go `ErrWrongValueForVar` (1231).
    WrongValueForVar(String, String),
    /// Go `ErrLocalVariable` (1228): `SET GLOBAL` named a SESSION-only
    /// variable.
    SessionOnlyVariable(String),
    /// Go `ErrGlobalVariable` (1229): `SET SESSION` (or plain `SET`) named a
    /// GLOBAL-only variable.
    GlobalOnlyVariable(String),
    /// Go `ErrIncorrectGlobalLocalVar` (1238), read side: `SELECT
    /// @@global.x` named a SESSION-only variable.
    NoGlobalCopy(String),
    /// Go `ErrSpecificAccessDenied.GenWithStackByArgs("SUPER or
    /// SYSTEM_VARIABLES_ADMIN")` (1227): `SET GLOBAL` without SUPER or the
    /// dynamic `SYSTEM_VARIABLES_ADMIN` privilege.
    SetGlobalAccessDenied,
    /// A `SysVar.Validation` closure that refuses the value with a bare
    /// `errors.Errorf`, which carries no MySQL code of its own and so reports
    /// as `ER_UNKNOWN_ERROR` (1105) with the closure's own wording --
    /// `tidb_enable_list_partition` set to anything but ON is the case that
    /// exists.
    ValidationRefused(String),
    /// Go `ErrUnsupportedIsolationLevel` (8048), from `checkIsolationLevel`
    /// (`pkg/sessionctx/variable/varsutil.go:116`): `SERIALIZABLE` and
    /// `READ-UNCOMMITTED` are refused unless
    /// `tidb_skip_isolation_level_check` is ON, which downgrades the same
    /// error to a warning.
    UnsupportedIsolationLevel(String),
    /// Go `ErrReadOnly` (1621), from `max_allowed_packet`'s `Validation`
    /// (`pkg/sessionctx/variable/sysvar.go:2193`): the variable is settable
    /// only at GLOBAL scope, even though it HAS session scope for reading.
    ///
    /// The three arguments are Go's: the scope that was refused, the
    /// variable, and the scope to use instead.
    SessionScopeIsReadOnly(String),
}
