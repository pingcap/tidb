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

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/errno"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testkSuite{})

type testkSuite struct{}

func genErrMsg(pattern string, a ...interface{}) string {
	return fmt.Sprintf(pattern, a...)
}

func (s *testkSuite) TestErrorRedact(c *C) {
	original := errors.RedactLogEnabled.Load()
	errors.RedactLogEnabled.Store(true)
	defer func() { errors.RedactLogEnabled.Store(original) }()

	class := ErrClass{}

	NoSensitiveValue := "no_sensitive"
	SensitiveData := "sensitive_data"
	QuestionMark := "?"

	err := class.NewStd(errno.ErrDupEntry).GenWithStackByArgs(SensitiveData, NoSensitiveValue)
	c.Assert(strings.Contains(err.Error(), genErrMsg(errno.MySQLErrName[errno.ErrDupEntry].Raw, QuestionMark, NoSensitiveValue)), IsTrue)
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
}
