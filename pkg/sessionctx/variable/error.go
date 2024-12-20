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
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

// Error instances.
var (
	errWarnDeprecatedSyntax        = dbterror.ClassVariable.NewStd(errno.ErrWarnDeprecatedSyntax)
	ErrSnapshotTooOld              = dbterror.ClassVariable.NewStd(errno.ErrSnapshotTooOld)
	ErrUnsupportedValueForVar      = dbterror.ClassVariable.NewStd(errno.ErrUnsupportedValueForVar)
	ErrUnknownSystemVar            = dbterror.ClassVariable.NewStd(errno.ErrUnknownSystemVariable)
	ErrIncorrectScope              = dbterror.ClassVariable.NewStd(errno.ErrIncorrectGlobalLocalVar)
	ErrUnknownTimeZone             = dbterror.ClassVariable.NewStd(errno.ErrUnknownTimeZone)
	ErrReadOnly                    = dbterror.ClassVariable.NewStd(errno.ErrVariableIsReadonly)
	ErrWrongValueForVar            = dbterror.ClassVariable.NewStd(errno.ErrWrongValueForVar)
	ErrWrongTypeForVar             = dbterror.ClassVariable.NewStd(errno.ErrWrongTypeForVar)
	ErrTruncatedWrongValue         = dbterror.ClassVariable.NewStd(errno.ErrTruncatedWrongValue)
	ErrMaxPreparedStmtCountReached = dbterror.ClassVariable.NewStd(errno.ErrMaxPreparedStmtCountReached)
	ErrUnsupportedIsolationLevel   = dbterror.ClassVariable.NewStd(errno.ErrUnsupportedIsolationLevel)
	errUnknownSystemVariable       = dbterror.ClassVariable.NewStd(errno.ErrUnknownSystemVariable)
	errGlobalVariable              = dbterror.ClassVariable.NewStd(errno.ErrGlobalVariable)
	errLocalVariable               = dbterror.ClassVariable.NewStd(errno.ErrLocalVariable)
	errValueNotSupportedWhen       = dbterror.ClassVariable.NewStdErr(errno.ErrNotSupportedYet, errno.Message("%s = OFF is not supported when %s = ON", nil))
	ErrNotValidPassword            = dbterror.ClassExecutor.NewStd(errno.ErrNotValidPassword)
	// ErrFunctionsNoopImpl is an error to say the behavior is protected by the tidb_enable_noop_functions sysvar.
	// This is copied from expression.ErrFunctionsNoopImpl to prevent circular dependencies.
	// It needs to be public for tests.
	ErrFunctionsNoopImpl                 = dbterror.ClassVariable.NewStdErr(errno.ErrNotSupportedYet, errno.Message("function %s has only noop implementation in tidb now, use tidb_enable_noop_functions to enable these functions", nil))
	ErrVariableNoLongerSupported         = dbterror.ClassVariable.NewStd(errno.ErrVariableNoLongerSupported)
	ErrInvalidDefaultUTF8MB4Collation    = dbterror.ClassVariable.NewStd(errno.ErrInvalidDefaultUTF8MB4Collation)
	ErrWarnDeprecatedSyntaxNoReplacement = dbterror.ClassVariable.NewStdErr(errno.ErrWarnDeprecatedSyntaxNoReplacement, errno.Message("Updating '%s' is deprecated. It will be made read-only in a future release.", nil))
	ErrWarnDeprecatedSyntaxSimpleMsg     = dbterror.ClassVariable.NewStdErr(errno.ErrWarnDeprecatedSyntaxNoReplacement, errno.Message("%s is deprecated and will be removed in a future release.", nil))
)
