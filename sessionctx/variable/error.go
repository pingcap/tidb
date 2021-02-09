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
	mysql "github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/util/dbterror"
)

// Error instances.
var (
	errWarnDeprecatedSyntax        = dbterror.ClassVariable.NewStd(mysql.ErrWarnDeprecatedSyntax)
	ErrSnapshotTooOld              = dbterror.ClassVariable.NewStd(mysql.ErrSnapshotTooOld)
	ErrUnsupportedValueForVar      = dbterror.ClassVariable.NewStd(mysql.ErrUnsupportedValueForVar)
	ErrUnknownSystemVar            = dbterror.ClassVariable.NewStd(mysql.ErrUnknownSystemVariable)
	ErrIncorrectScope              = dbterror.ClassVariable.NewStd(mysql.ErrIncorrectGlobalLocalVar)
	ErrUnknownTimeZone             = dbterror.ClassVariable.NewStd(mysql.ErrUnknownTimeZone)
	ErrReadOnly                    = dbterror.ClassVariable.NewStd(mysql.ErrVariableIsReadonly)
	ErrWrongValueForVar            = dbterror.ClassVariable.NewStd(mysql.ErrWrongValueForVar)
	ErrWrongTypeForVar             = dbterror.ClassVariable.NewStd(mysql.ErrWrongTypeForVar)
	ErrTruncatedWrongValue         = dbterror.ClassVariable.NewStd(mysql.ErrTruncatedWrongValue)
	ErrMaxPreparedStmtCountReached = dbterror.ClassVariable.NewStd(mysql.ErrMaxPreparedStmtCountReached)
	ErrUnsupportedIsolationLevel   = dbterror.ClassVariable.NewStd(mysql.ErrUnsupportedIsolationLevel)
)
