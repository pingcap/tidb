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

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/stretchr/testify/require"
)

var foobarPwdSM3Hash, _ = hex.DecodeString("24412430303524031a69251c34295c4b35167c7f1e5a7b63091349536c72627066426a635061762e556e6c63533159414d7762317261324a5a3047756b4244664177434e3043")

func TestSM3(t *testing.T) {
	testCases := [][]string{
		{"abc", "66c7f0f462eeedd9d1f2d46bdc10e4e24167c4875cf2f7a2297da02b8f4ba8e0"},
		{"abcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcd", "debe9ff92275b8a138604889c18e5a4d6fdb70e5387e5765293dcba39c0c5732"},
	}
	var expect []byte

	for _, testCase := range testCases {
		text := testCase[0]
		expect, _ = hex.DecodeString(testCase[1])
		result := Sm3Hash([]byte(text))
		require.Equal(t, expect, result)
	}
}

func TestCheckSM3PasswordGood(t *testing.T) {
	pwd := "foobar"
	r, err := CheckHashingPassword(foobarPwdSM3Hash, pwd, mysql.AuthTiDBSM3Password)
	require.NoError(t, err)
	require.True(t, r)
}

func TestCheckSM3PasswordBad(t *testing.T) {
	pwd := "not_foobar"
	pwhash, _ := hex.DecodeString("24412430303524031a69251c34295c4b35167c7f1e5a7b6309134956387565426743446d3643446176712f6c4b63323667346e48624872776f39512e4342416a693656676f2f")
	r, err := CheckHashingPassword(pwhash, pwd, mysql.AuthTiDBSM3Password)
	require.NoError(t, err)
	require.False(t, r)
}

func TestCheckSM3PasswordShort(t *testing.T) {
	pwd := "not_foobar"
	pwhash, _ := hex.DecodeString("aaaaaaaa")
	_, err := CheckHashingPassword(pwhash, pwd, mysql.AuthTiDBSM3Password)
	require.Error(t, err)
}

func TestCheckSM3PasswordDigestTypeIncompatible(t *testing.T) {
	pwd := "not_foobar"
	pwhash, _ := hex.DecodeString("24432430303524031A69251C34295C4B35167C7F1E5A7B63091349503974624D34504B5A424679354856336868686F52485A736E4A733368786E427575516C73446469496537")
	_, err := CheckHashingPassword(pwhash, pwd, mysql.AuthTiDBSM3Password)
	require.Error(t, err)
}

func TestCheckSM3PasswordIterationsInvalid(t *testing.T) {
	pwd := "not_foobar"
	pwhash, _ := hex.DecodeString("24412430304124031A69251C34295C4B35167C7F1E5A7B63091349503974624D34504B5A424679354856336868686F52485A736E4A733368786E427575516C73446469496537")
	_, err := CheckHashingPassword(pwhash, pwd, mysql.AuthTiDBSM3Password)
	require.Error(t, err)
}

func TestNewSM3Password(t *testing.T) {
	pwd := "testpwd"
	pwhash := NewHashPassword(pwd, mysql.AuthTiDBSM3Password)
	r, err := CheckHashingPassword([]byte(pwhash), pwd, mysql.AuthTiDBSM3Password)
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
		m, err := CheckHashingPassword(foobarPwdSM3Hash, "foobar", mysql.AuthTiDBSM3Password)
		require.Nil(b, err)
		require.True(b, m)
	}
}
