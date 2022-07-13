// Copyright 2021 PingCAP, Inc.
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

package auth

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckSM3Password(t *testing.T) {
	pwd1 := "test"
	pwd2 := "no_pass"
	val := "55e12e91650d2fec56ec74e1d3e4ddbfce2ef3a65890c2a19ecf88a307e76a23"

	sm3pwd := NewSM3Password(pwd1)
	require.Equal(t, val, fmt.Sprintf("%x", sm3pwd))
	r, err := CheckSM3Password([]byte(sm3pwd), pwd1)
	require.NoError(t, err)
	require.True(t, r)

	sm3pwd = NewSM3Password(pwd2)
	require.NotEqual(t, val, fmt.Sprintf("%x", sm3pwd))
	r, err = CheckSM3Password([]byte(sm3pwd), pwd1)
	require.NoError(t, err)
	require.False(t, r)
}
