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

package dbterror

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/errno"
	"github.com/stretchr/testify/assert"
)

func genErrMsg(pattern string, a ...interface{}) string {
	return fmt.Sprintf(pattern, a...)
}

func TestErrorRedact(t *testing.T) {
	t.Parallel()

	original := errors.RedactLogEnabled.Load()
	errors.RedactLogEnabled.Store(true)
	defer func() { errors.RedactLogEnabled.Store(original) }()

	class := ErrClass{}

	NoSensitiveValue := "no_sensitive"
	SensitiveData := "sensitive_data"
	QuestionMark := "?"

	err := class.NewStd(errno.ErrDupEntry).GenWithStackByArgs(SensitiveData, NoSensitiveValue)
	assert.True(t, strings.Contains(err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrDupEntry].Raw, QuestionMark, NoSensitiveValue)))
	err = class.NewStd(errno.ErrCutValueGroupConcat).GenWithStackByArgs(SensitiveData)
	assert.True(t, strings.Contains(err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrCutValueGroupConcat].Raw, QuestionMark)))
	err = class.NewStd(errno.ErrDuplicatedValueInType).GenWithStackByArgs(NoSensitiveValue, SensitiveData)
	assert.True(t, strings.Contains(err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrDuplicatedValueInType].Raw, NoSensitiveValue, QuestionMark)))
	err = class.NewStd(errno.ErrTruncatedWrongValue).GenWithStackByArgs(NoSensitiveValue, SensitiveData)
	assert.True(t, strings.Contains(err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrTruncatedWrongValue].Raw, NoSensitiveValue, QuestionMark)))
	err = class.NewStd(errno.ErrInvalidCharacterString).FastGenByArgs(NoSensitiveValue, SensitiveData)
	assert.True(t, strings.Contains(err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrInvalidCharacterString].Raw, NoSensitiveValue, QuestionMark)))
	err = class.NewStd(errno.ErrTruncatedWrongValueForField).FastGenByArgs(SensitiveData, SensitiveData)
	assert.True(t, strings.Contains(err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrTruncatedWrongValueForField].Raw, QuestionMark, QuestionMark)))
	err = class.NewStd(errno.ErrIllegalValueForType).FastGenByArgs(NoSensitiveValue, SensitiveData)
	assert.True(t, strings.Contains(err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrIllegalValueForType].Raw, NoSensitiveValue, QuestionMark)))

	err = class.NewStd(errno.ErrPartitionWrongValues).GenWithStackByArgs(NoSensitiveValue, SensitiveData)
	assert.True(t, strings.Contains(err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrPartitionWrongValues].Raw, NoSensitiveValue, QuestionMark)))
	err = class.NewStd(errno.ErrNoParts).GenWithStackByArgs(SensitiveData)
	assert.True(t, strings.Contains(err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrNoParts].Raw, QuestionMark)))
	err = class.NewStd(errno.ErrWrongValue).GenWithStackByArgs(NoSensitiveValue, SensitiveData)
	assert.True(t, strings.Contains(err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrWrongValue].Raw, NoSensitiveValue, QuestionMark)))
	err = class.NewStd(errno.ErrNoPartitionForGivenValue).GenWithStackByArgs(SensitiveData)
	assert.True(t, strings.Contains(err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrNoPartitionForGivenValue].Raw, QuestionMark)))
	err = class.NewStd(errno.ErrDataOutOfRange).GenWithStackByArgs(NoSensitiveValue, SensitiveData)
	assert.True(t, strings.Contains(err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrDataOutOfRange].Raw, NoSensitiveValue, QuestionMark)))

	err = class.NewStd(errno.ErrRowInWrongPartition).GenWithStackByArgs(SensitiveData)
	assert.True(t, strings.Contains(err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrRowInWrongPartition].Raw, QuestionMark)))
	err = class.NewStd(errno.ErrInvalidJSONText).GenWithStackByArgs(SensitiveData)
	assert.True(t, strings.Contains(err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrInvalidJSONText].Raw, QuestionMark)))
	err = class.NewStd(errno.ErrTxnRetryable).GenWithStackByArgs(SensitiveData)
	assert.True(t, strings.Contains(err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrTxnRetryable].Raw, QuestionMark)))
	assert.True(t, strings.Contains(err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrTxnRetryable].Raw, QuestionMark)))
	err = class.NewStd(errno.ErrIncorrectDatetimeValue).GenWithStackByArgs(SensitiveData)
	assert.True(t, strings.Contains(err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrIncorrectDatetimeValue].Raw, QuestionMark)))
	err = class.NewStd(errno.ErrInvalidTimeFormat).GenWithStackByArgs(SensitiveData)
	assert.True(t, strings.Contains(err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrInvalidTimeFormat].Raw, QuestionMark)))
	err = class.NewStd(errno.ErrRowNotFound).GenWithStackByArgs(SensitiveData)
	assert.True(t, strings.Contains(err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrRowNotFound].Raw, QuestionMark)))
	err = class.NewStd(errno.ErrWriteConflict).GenWithStackByArgs(NoSensitiveValue, NoSensitiveValue, NoSensitiveValue, SensitiveData)
	assert.True(t, strings.Contains(err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrWriteConflict].Raw, NoSensitiveValue, NoSensitiveValue, NoSensitiveValue, QuestionMark)))
}
