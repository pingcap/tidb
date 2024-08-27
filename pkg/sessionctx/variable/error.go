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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package variable

import (
	mysql "github.com/pingcap/tidb/pkg/errno"
	pmysql "github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/dbterror"
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
	errUnknownSystemVariable       = dbterror.ClassVariable.NewStd(mysql.ErrUnknownSystemVariable)
	errGlobalVariable              = dbterror.ClassVariable.NewStd(mysql.ErrGlobalVariable)
	errLocalVariable               = dbterror.ClassVariable.NewStd(mysql.ErrLocalVariable)
	errValueNotSupportedWhen       = dbterror.ClassVariable.NewStdErr(mysql.ErrNotSupportedYet, pmysql.Message("%s = OFF is not supported when %s = ON", nil))
	ErrNotValidPassword            = dbterror.ClassExecutor.NewStd(mysql.ErrNotValidPassword)
	// ErrFunctionsNoopImpl is an error to say the behavior is protected by the tidb_enable_noop_functions sysvar.
	// This is copied from expression.ErrFunctionsNoopImpl to prevent circular dependencies.
	// It needs to be public for tests.
	ErrFunctionsNoopImpl                 = dbterror.ClassVariable.NewStdErr(mysql.ErrNotSupportedYet, pmysql.Message("function %s has only noop implementation in tidb now, use tidb_enable_noop_functions to enable these functions", nil))
	ErrVariableNoLongerSupported         = dbterror.ClassVariable.NewStd(mysql.ErrVariableNoLongerSupported)
	ErrInvalidDefaultUTF8MB4Collation    = dbterror.ClassVariable.NewStd(mysql.ErrInvalidDefaultUTF8MB4Collation)
	ErrWarnDeprecatedSyntaxNoReplacement = dbterror.ClassVariable.NewStdErr(mysql.ErrWarnDeprecatedSyntaxNoReplacement, pmysql.Message("Updating '%s' is deprecated. It will be made read-only in a future release.", nil))
	ErrWarnDeprecatedSyntaxSimpleMsg     = dbterror.ClassVariable.NewStdErr(mysql.ErrWarnDeprecatedSyntaxNoReplacement, pmysql.Message("%s is deprecated and will be removed in a future release.", nil))
)
