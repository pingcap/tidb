// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package variable

import (
	"github.com/pingcap/parser/terror"
	mysql "github.com/pingcap/tidb/errno"
)

// Error instances.
var (
	errCantGetValidID              = terror.ClassVariable.New(mysql.ErrCantGetValidID, mysql.MySQLErrName[mysql.ErrCantGetValidID])
	errWarnDeprecatedSyntax        = terror.ClassVariable.New(mysql.ErrWarnDeprecatedSyntax, mysql.MySQLErrName[mysql.ErrWarnDeprecatedSyntax])
	ErrCantSetToNull               = terror.ClassVariable.New(mysql.ErrCantSetToNull, mysql.MySQLErrName[mysql.ErrCantSetToNull])
	ErrSnapshotTooOld              = terror.ClassVariable.New(mysql.ErrSnapshotTooOld, mysql.MySQLErrName[mysql.ErrSnapshotTooOld])
	ErrUnsupportedValueForVar      = terror.ClassVariable.New(mysql.ErrUnsupportedValueForVar, mysql.MySQLErrName[mysql.ErrUnsupportedValueForVar])
	ErrUnknownSystemVar            = terror.ClassVariable.New(mysql.ErrUnknownSystemVariable, mysql.MySQLErrName[mysql.ErrUnknownSystemVariable])
	ErrIncorrectScope              = terror.ClassVariable.New(mysql.ErrIncorrectGlobalLocalVar, mysql.MySQLErrName[mysql.ErrIncorrectGlobalLocalVar])
	ErrUnknownTimeZone             = terror.ClassVariable.New(mysql.ErrUnknownTimeZone, mysql.MySQLErrName[mysql.ErrUnknownTimeZone])
	ErrReadOnly                    = terror.ClassVariable.New(mysql.ErrVariableIsReadonly, mysql.MySQLErrName[mysql.ErrVariableIsReadonly])
	ErrWrongValueForVar            = terror.ClassVariable.New(mysql.ErrWrongValueForVar, mysql.MySQLErrName[mysql.ErrWrongValueForVar])
	ErrWrongTypeForVar             = terror.ClassVariable.New(mysql.ErrWrongTypeForVar, mysql.MySQLErrName[mysql.ErrWrongTypeForVar])
	ErrTruncatedWrongValue         = terror.ClassVariable.New(mysql.ErrTruncatedWrongValue, mysql.MySQLErrName[mysql.ErrTruncatedWrongValue])
	ErrMaxPreparedStmtCountReached = terror.ClassVariable.New(mysql.ErrMaxPreparedStmtCountReached, mysql.MySQLErrName[mysql.ErrMaxPreparedStmtCountReached])
	ErrUnsupportedIsolationLevel   = terror.ClassVariable.New(mysql.ErrUnsupportedIsolationLevel, mysql.MySQLErrName[mysql.ErrUnsupportedIsolationLevel])
)
