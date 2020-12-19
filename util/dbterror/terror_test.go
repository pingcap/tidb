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
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/errno"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testkSuite{})

type testkSuite struct{}

func (s *testkSuite) TestErrorRedact(c *C) {
	original := errors.RedactLogEnabled.Load()
	errors.RedactLogEnabled.Store(true)
	defer func() { errors.RedactLogEnabled.Store(original) }()

	class := ErrClass{}
<<<<<<< HEAD
	err := class.NewStd(errno.ErrDupEntry).GenWithStackByArgs("sensitive_data", "no_sensitive")
	c.Assert(strings.Contains(err.Error(), "?"), IsTrue)
	c.Assert(strings.Contains(err.Error(), "no_sensitive"), IsTrue)
	c.Assert(strings.Contains(err.Error(), "sensitive_data"), IsFalse)
=======

	NoSensitiveValue := "no_sensitive"
	SensitiveData := "sensitive_data"
	QuestionMark := "?"

	err := class.NewStd(errno.ErrDupEntry).GenWithStackByArgs(SensitiveData, NoSensitiveValue)
	c.Assert(strings.Contains(err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrDupEntry].Raw, QuestionMark, NoSensitiveValue)), IsTrue)
	err = class.NewStd(errno.ErrCutValueGroupConcat).GenWithStackByArgs(SensitiveData)
	c.Assert(strings.Contains(err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrCutValueGroupConcat].Raw, QuestionMark)), IsTrue)
	err = class.NewStd(errno.ErrDuplicatedValueInType).GenWithStackByArgs(NoSensitiveValue, SensitiveData)
	c.Assert(strings.Contains(err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrDuplicatedValueInType].Raw, NoSensitiveValue, QuestionMark)), IsTrue)
	err = class.NewStd(errno.ErrTruncatedWrongValue).GenWithStackByArgs(NoSensitiveValue, SensitiveData)
	c.Assert(strings.Contains(err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrTruncatedWrongValue].Raw, NoSensitiveValue, QuestionMark)), IsTrue)
	err = class.NewStd(errno.ErrInvalidCharacterString).FastGenByArgs(NoSensitiveValue, SensitiveData)
	c.Assert(strings.Contains(err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrInvalidCharacterString].Raw, NoSensitiveValue, QuestionMark)), IsTrue)
	err = class.NewStd(errno.ErrTruncatedWrongValueForField).FastGenByArgs(SensitiveData, SensitiveData)
	c.Assert(strings.Contains(err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrTruncatedWrongValueForField].Raw, QuestionMark, QuestionMark)), IsTrue)
	err = class.NewStd(errno.ErrIllegalValueForType).FastGenByArgs(NoSensitiveValue, SensitiveData)
	c.Assert(strings.Contains(err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrIllegalValueForType].Raw, NoSensitiveValue, QuestionMark)), IsTrue)

	err = class.NewStd(errno.ErrPartitionWrongValues).GenWithStackByArgs(NoSensitiveValue, SensitiveData)
	c.Assert(strings.Contains(err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrPartitionWrongValues].Raw, NoSensitiveValue, QuestionMark)), IsTrue)
	err = class.NewStd(errno.ErrNoParts).GenWithStackByArgs(SensitiveData)
	c.Assert(strings.Contains(err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrNoParts].Raw, QuestionMark)), IsTrue)
	err = class.NewStd(errno.ErrWrongValue).GenWithStackByArgs(NoSensitiveValue, SensitiveData)
	c.Assert(strings.Contains(err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrWrongValue].Raw, NoSensitiveValue, QuestionMark)), IsTrue)
	err = class.NewStd(errno.ErrNoPartitionForGivenValue).GenWithStackByArgs(SensitiveData)
	c.Assert(strings.Contains(err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrNoPartitionForGivenValue].Raw, QuestionMark)), IsTrue)
	err = class.NewStd(errno.ErrDataOutOfRange).GenWithStackByArgs(NoSensitiveValue, SensitiveData)
	c.Assert(strings.Contains(err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrDataOutOfRange].Raw, NoSensitiveValue, QuestionMark)), IsTrue)

	err = class.NewStd(errno.ErrRowInWrongPartition).GenWithStackByArgs(SensitiveData)
	c.Assert(strings.Contains(err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrRowInWrongPartition].Raw, QuestionMark)), IsTrue)
	err = class.NewStd(errno.ErrInvalidJSONText).GenWithStackByArgs(SensitiveData)
	c.Assert(strings.Contains(err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrInvalidJSONText].Raw, QuestionMark)), IsTrue)
	err = class.NewStd(errno.ErrTxnRetryable).GenWithStackByArgs(SensitiveData)
	c.Assert(strings.Contains(err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrTxnRetryable].Raw, QuestionMark)), IsTrue)
	c.Assert(strings.Contains(err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrTxnRetryable].Raw, QuestionMark)), IsTrue)
	err = class.NewStd(errno.ErrIncorrectDatetimeValue).GenWithStackByArgs(SensitiveData)
	c.Assert(strings.Contains(err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrIncorrectDatetimeValue].Raw, QuestionMark)), IsTrue)
	err = class.NewStd(errno.ErrInvalidTimeFormat).GenWithStackByArgs(SensitiveData)
	c.Assert(strings.Contains(err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrInvalidTimeFormat].Raw, QuestionMark)), IsTrue)
	err = class.NewStd(errno.ErrRowNotFound).GenWithStackByArgs(SensitiveData)
	c.Assert(strings.Contains(err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrRowNotFound].Raw, QuestionMark)), IsTrue)
	err = class.NewStd(errno.ErrWriteConflict).GenWithStackByArgs(NoSensitiveValue, NoSensitiveValue, NoSensitiveValue, SensitiveData)
	c.Assert(strings.Contains(err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrWriteConflict].Raw, NoSensitiveValue, NoSensitiveValue, NoSensitiveValue, QuestionMark)), IsTrue)
>>>>>>> 1a9852f85... *: redact some error code, part(3/3) (#21866)
}
