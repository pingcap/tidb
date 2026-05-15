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
	"regexp"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/stretchr/testify/require"
)

var (
	markerRegexp = regexp.MustCompile("‹sensitive_data›")
)

func checkErrMsg(t *testing.T, isMarker bool, err error, pattern string, a ...any) {
	if !isMarker {
		require.Contains(t, err.Error(), fmt.Sprintf(pattern, a...))
	} else {
		markerRegexp.MatchString(err.Error())
	}
}

func TestErrorRedact(t *testing.T) {
	original := errors.RedactLogEnabled.Load()
	defer func() { errors.RedactLogEnabled.Store(original) }()

	class := ErrClass{}

	noSensitiveValue := "no_sensitive"
	sensitiveData := "sensitive_data"
	questionMark := "?"

	for _, c := range []string{errors.RedactLogEnable, errors.RedactLogMarker} {
		isMarker := c == errors.RedactLogMarker
		errors.RedactLogEnabled.Store(c)

		err := class.NewStd(errno.ErrDupEntry).GenWithStackByArgs(sensitiveData, noSensitiveValue)
		checkErrMsg(t, isMarker, err, errno.MySQLErrName[errno.ErrDupEntry].Raw, questionMark, noSensitiveValue)
		err = class.NewStd(errno.ErrCutValueGroupConcat).GenWithStackByArgs(sensitiveData)
		checkErrMsg(t, isMarker, err, errno.MySQLErrName[errno.ErrCutValueGroupConcat].Raw, questionMark)
		err = class.NewStd(errno.ErrDuplicatedValueInType).GenWithStackByArgs(noSensitiveValue, sensitiveData)
		checkErrMsg(t, isMarker, err, errno.MySQLErrName[errno.ErrDuplicatedValueInType].Raw, noSensitiveValue, questionMark)
		err = class.NewStd(errno.ErrTruncatedWrongValue).GenWithStackByArgs(noSensitiveValue, sensitiveData)
		checkErrMsg(t, isMarker, err, errno.MySQLErrName[errno.ErrTruncatedWrongValue].Raw, noSensitiveValue, questionMark)
		err = class.NewStd(errno.ErrInvalidCharacterString).FastGenByArgs(noSensitiveValue, sensitiveData)
		checkErrMsg(t, isMarker, err, errno.MySQLErrName[errno.ErrInvalidCharacterString].Raw, noSensitiveValue, questionMark)
		err = class.NewStd(errno.ErrTruncatedWrongValueForField).FastGenByArgs(sensitiveData, sensitiveData)
		checkErrMsg(t, isMarker, err, errno.MySQLErrName[errno.ErrTruncatedWrongValueForField].Raw, questionMark, questionMark)
		err = class.NewStd(errno.ErrIllegalValueForType).FastGenByArgs(noSensitiveValue, sensitiveData)
		checkErrMsg(t, isMarker, err, errno.MySQLErrName[errno.ErrIllegalValueForType].Raw, noSensitiveValue, questionMark)

		err = class.NewStd(errno.ErrPartitionWrongValues).GenWithStackByArgs(noSensitiveValue, sensitiveData)
		checkErrMsg(t, isMarker, err, errno.MySQLErrName[errno.ErrPartitionWrongValues].Raw, noSensitiveValue, questionMark)
		err = class.NewStd(errno.ErrNoParts).GenWithStackByArgs(sensitiveData)
		checkErrMsg(t, isMarker, err, errno.MySQLErrName[errno.ErrNoParts].Raw, questionMark)
		err = class.NewStd(errno.ErrWrongValue).GenWithStackByArgs(noSensitiveValue, sensitiveData)
		checkErrMsg(t, isMarker, err, errno.MySQLErrName[errno.ErrWrongValue].Raw, noSensitiveValue, questionMark)
		err = class.NewStd(errno.ErrNoPartitionForGivenValue).GenWithStackByArgs(sensitiveData)
		checkErrMsg(t, isMarker, err, errno.MySQLErrName[errno.ErrNoPartitionForGivenValue].Raw, questionMark)
		err = class.NewStd(errno.ErrDataOutOfRange).GenWithStackByArgs(noSensitiveValue, sensitiveData)
		checkErrMsg(t, isMarker, err, errno.MySQLErrName[errno.ErrDataOutOfRange].Raw, noSensitiveValue, questionMark)

		err = class.NewStd(errno.ErrRowInWrongPartition).GenWithStackByArgs(sensitiveData)
		checkErrMsg(t, isMarker, err, errno.MySQLErrName[errno.ErrRowInWrongPartition].Raw, questionMark)
		err = class.NewStd(errno.ErrInvalidJSONText).GenWithStackByArgs(sensitiveData)
		checkErrMsg(t, isMarker, err, errno.MySQLErrName[errno.ErrInvalidJSONText].Raw, questionMark)
		err = class.NewStd(errno.ErrTxnRetryable).GenWithStackByArgs(sensitiveData)
		checkErrMsg(t, isMarker, err, errno.MySQLErrName[errno.ErrTxnRetryable].Raw, questionMark)
		checkErrMsg(t, isMarker, err, errno.MySQLErrName[errno.ErrTxnRetryable].Raw, questionMark)
		err = class.NewStd(errno.ErrIncorrectDatetimeValue).GenWithStackByArgs(sensitiveData)
		checkErrMsg(t, isMarker, err, errno.MySQLErrName[errno.ErrIncorrectDatetimeValue].Raw, questionMark)
		err = class.NewStd(errno.ErrInvalidTimeFormat).GenWithStackByArgs(sensitiveData)
		checkErrMsg(t, isMarker, err, errno.MySQLErrName[errno.ErrInvalidTimeFormat].Raw, questionMark)
		err = class.NewStd(errno.ErrRowNotFound).GenWithStackByArgs(sensitiveData)
		checkErrMsg(t, isMarker, err, errno.MySQLErrName[errno.ErrRowNotFound].Raw, questionMark)
		err = class.NewStd(errno.ErrWriteConflict).GenWithStackByArgs(noSensitiveValue, noSensitiveValue, noSensitiveValue, sensitiveData)
		checkErrMsg(t, isMarker, err, errno.MySQLErrName[errno.ErrWriteConflict].Raw, noSensitiveValue, noSensitiveValue, noSensitiveValue, questionMark)
	}
}
