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

package dbterror

import (
	"fmt"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/errno"
	"github.com/stretchr/testify/require"
)

func genErrMsg(pattern string, a ...interface{}) string {
	return fmt.Sprintf(pattern, a...)
}

func TestErrorRedact(t *testing.T) {
	original := errors.RedactLogEnabled.Load()
	errors.RedactLogEnabled.Store(true)
	defer func() { errors.RedactLogEnabled.Store(original) }()

	class := ErrClass{}

	noSensitiveValue := "no_sensitive"
	sensitiveData := "sensitive_data"
	questionMark := "?"

	err := class.NewStd(errno.ErrDupEntry).GenWithStackByArgs(sensitiveData, noSensitiveValue)
	require.Contains(t, err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrDupEntry].Raw, questionMark, noSensitiveValue))
	err = class.NewStd(errno.ErrCutValueGroupConcat).GenWithStackByArgs(sensitiveData)
	require.Contains(t, err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrCutValueGroupConcat].Raw, questionMark))
	err = class.NewStd(errno.ErrDuplicatedValueInType).GenWithStackByArgs(noSensitiveValue, sensitiveData)
	require.Contains(t, err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrDuplicatedValueInType].Raw, noSensitiveValue, questionMark))
	err = class.NewStd(errno.ErrTruncatedWrongValue).GenWithStackByArgs(noSensitiveValue, sensitiveData)
	require.Contains(t, err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrTruncatedWrongValue].Raw, noSensitiveValue, questionMark))
	err = class.NewStd(errno.ErrInvalidCharacterString).FastGenByArgs(noSensitiveValue, sensitiveData)
	require.Contains(t, err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrInvalidCharacterString].Raw, noSensitiveValue, questionMark))
	err = class.NewStd(errno.ErrTruncatedWrongValueForField).FastGenByArgs(sensitiveData, sensitiveData)
	require.Contains(t, err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrTruncatedWrongValueForField].Raw, questionMark, questionMark))
	err = class.NewStd(errno.ErrIllegalValueForType).FastGenByArgs(noSensitiveValue, sensitiveData)
	require.Contains(t, err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrIllegalValueForType].Raw, noSensitiveValue, questionMark))

	err = class.NewStd(errno.ErrPartitionWrongValues).GenWithStackByArgs(noSensitiveValue, sensitiveData)
	require.Contains(t, err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrPartitionWrongValues].Raw, noSensitiveValue, questionMark))
	err = class.NewStd(errno.ErrNoParts).GenWithStackByArgs(sensitiveData)
	require.Contains(t, err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrNoParts].Raw, questionMark))
	err = class.NewStd(errno.ErrWrongValue).GenWithStackByArgs(noSensitiveValue, sensitiveData)
	require.Contains(t, err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrWrongValue].Raw, noSensitiveValue, questionMark))
	err = class.NewStd(errno.ErrNoPartitionForGivenValue).GenWithStackByArgs(sensitiveData)
	require.Contains(t, err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrNoPartitionForGivenValue].Raw, questionMark))
	err = class.NewStd(errno.ErrDataOutOfRange).GenWithStackByArgs(noSensitiveValue, sensitiveData)
	require.Contains(t, err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrDataOutOfRange].Raw, noSensitiveValue, questionMark))

	err = class.NewStd(errno.ErrRowInWrongPartition).GenWithStackByArgs(sensitiveData)
	require.Contains(t, err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrRowInWrongPartition].Raw, questionMark))
	err = class.NewStd(errno.ErrInvalidJSONText).GenWithStackByArgs(sensitiveData)
	require.Contains(t, err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrInvalidJSONText].Raw, questionMark))
	err = class.NewStd(errno.ErrTxnRetryable).GenWithStackByArgs(sensitiveData)
	require.Contains(t, err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrTxnRetryable].Raw, questionMark))
	require.Contains(t, err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrTxnRetryable].Raw, questionMark))
	err = class.NewStd(errno.ErrIncorrectDatetimeValue).GenWithStackByArgs(sensitiveData)
	require.Contains(t, err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrIncorrectDatetimeValue].Raw, questionMark))
	err = class.NewStd(errno.ErrInvalidTimeFormat).GenWithStackByArgs(sensitiveData)
	require.Contains(t, err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrInvalidTimeFormat].Raw, questionMark))
	err = class.NewStd(errno.ErrRowNotFound).GenWithStackByArgs(sensitiveData)
	require.Contains(t, err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrRowNotFound].Raw, questionMark))
	err = class.NewStd(errno.ErrWriteConflict).GenWithStackByArgs(noSensitiveValue, noSensitiveValue, noSensitiveValue, sensitiveData)
	require.Contains(t, err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrWriteConflict].Raw, noSensitiveValue, noSensitiveValue, noSensitiveValue, questionMark))
}
