// Copyright 2022 PingCAP, Inc.
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
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

var foobarPwdSM3Hash, _ = hex.DecodeString("24422430303524031a69251c34295c4b35167c7f1e5a7b63091349536c72627066426a635061762e556e6c63533159414d7762317261324a5a3047756b4244664177434e3043")

func TestCheckSM3PasswordGood(t *testing.T) {
	pwd := "foobar"
	r, err := CheckSM3Password(foobarPwdSM3Hash, pwd)
	require.NoError(t, err)
	require.True(t, r)
}

func TestCheckSM3PasswordBad(t *testing.T) {
	pwd := "not_foobar"
	pwhash, _ := hex.DecodeString("24422430303524031a69251c34295c4b35167c7f1e5a7b63091349536c72627066426a635061762e556e6c63533159414d7762317261324a5a3047756b4244664177434e3043")
	r, err := CheckSM3Password(pwhash, pwd)
	require.NoError(t, err)
	require.False(t, r)
}

func TestCheckSM3PasswordShort(t *testing.T) {
	pwd := "not_foobar"
	pwhash, _ := hex.DecodeString("aaaaaaaa")
	_, err := CheckSM3Password(pwhash, pwd)
	require.Error(t, err)
}

func TestCheckSM3PasswordDigestTypeIncompatible(t *testing.T) {
	pwd := "not_foobar"
	pwhash, _ := hex.DecodeString("24432430303524031A69251C34295C4B35167C7F1E5A7B63091349503974624D34504B5A424679354856336868686F52485A736E4A733368786E427575516C73446469496537")
	_, err := CheckSM3Password(pwhash, pwd)
	require.Error(t, err)
}

func TestCheckSM3PasswordIterationsInvalid(t *testing.T) {
	pwd := "not_foobar"
	pwhash, _ := hex.DecodeString("24412430304124031A69251C34295C4B35167C7F1E5A7B63091349503974624D34504B5A424679354856336868686F52485A736E4A733368786E427575516C73446469496537")
	_, err := CheckSM3Password(pwhash, pwd)
	require.Error(t, err)
}

func TestNewSM3Password(t *testing.T) {
	pwd := "testpwd"
	pwhash := NewSM3Password(pwd)
	r, err := CheckSM3Password([]byte(pwhash), pwd)
	require.NoError(t, err)
	require.True(t, r)

	for r := range pwhash {
		require.Less(t, pwhash[r], uint8(128))
		require.NotEqual(t, pwhash[r], 0)  // NUL
		require.NotEqual(t, pwhash[r], 36) // '$'
	}
}

func BenchmarkSM3Password(b *testing.B) {
	for i := 0; i < b.N; i++ {
		m, err := CheckSM3Password(foobarPwdSM3Hash, "foobar")
		require.Nil(b, err)
		require.True(b, m)
	}
}
