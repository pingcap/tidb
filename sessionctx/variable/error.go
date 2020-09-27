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
	errCantGetValidID              = terror.ClassVariable.NewStd(mysql.ErrCantGetValidID)
	errWarnDeprecatedSyntax        = terror.ClassVariable.NewStd(mysql.ErrWarnDeprecatedSyntax)
	ErrCantSetToNull               = terror.ClassVariable.NewStd(mysql.ErrCantSetToNull)
	ErrSnapshotTooOld              = terror.ClassVariable.NewStd(mysql.ErrSnapshotTooOld)
	ErrUnsupportedValueForVar      = terror.ClassVariable.NewStd(mysql.ErrUnsupportedValueForVar)
	ErrUnknownSystemVar            = terror.ClassVariable.NewStd(mysql.ErrUnknownSystemVariable)
	ErrIncorrectScope              = terror.ClassVariable.NewStd(mysql.ErrIncorrectGlobalLocalVar)
	ErrUnknownTimeZone             = terror.ClassVariable.NewStd(mysql.ErrUnknownTimeZone)
	ErrReadOnly                    = terror.ClassVariable.NewStd(mysql.ErrVariableIsReadonly)
	ErrWrongValueForVar            = terror.ClassVariable.NewStd(mysql.ErrWrongValueForVar)
	ErrWrongTypeForVar             = terror.ClassVariable.NewStd(mysql.ErrWrongTypeForVar)
	ErrTruncatedWrongValue         = terror.ClassVariable.NewStd(mysql.ErrTruncatedWrongValue)
	ErrMaxPreparedStmtCountReached = terror.ClassVariable.NewStd(mysql.ErrMaxPreparedStmtCountReached)
	ErrUnsupportedIsolationLevel   = terror.ClassVariable.NewStd(mysql.ErrUnsupportedIsolationLevel)
)
