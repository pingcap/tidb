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
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/errno"
	"github.com/stretchr/testify/assert"
)

func TestErrorRedact(t *testing.T) {
	original := errors.RedactLogEnabled
	errors.RedactLogEnabled = true
	defer func() { errors.RedactLogEnabled = original }()

	c := ErrClass{}
	err := c.NewStd(errno.ErrDupEntry).GenWithStackByArgs("sensitive", "data")
	assert.Contains(t, err.Error(), "?")
	assert.NotContains(t, err.Error(), "sensitive")
	assert.NotContains(t, err.Error(), "data")
}
