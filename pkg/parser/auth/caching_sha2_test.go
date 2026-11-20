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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"runtime"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/stretchr/testify/require"
)

var foobarPwdSHA2Hash, _ = hex.DecodeString("24412430303524031A69251C34295C4B35167C7F1E5A7B63091349503974624D34504B5A424679354856336868686F52485A736E4A733368786E427575516C73446469496537")

func TestCheckShaPasswordGood(t *testing.T) {
	pwd := "foobar"
	r, err := CheckHashingPassword(foobarPwdSHA2Hash, pwd, mysql.AuthCachingSha2Password)
	require.NoError(t, err)
	require.True(t, r)
}

func TestCheckShaPasswordBad(t *testing.T) {
	pwd := "not_foobar"
	pwhash, _ := hex.DecodeString("24412430303524031A69251C34295C4B35167C7F1E5A7B63091349503974624D34504B5A424679354856336868686F52485A736E4A733368786E427575516C73446469496537")
	r, err := CheckHashingPassword(pwhash, pwd, mysql.AuthCachingSha2Password)
	require.NoError(t, err)
	require.False(t, r)
}

func TestCheckShaPasswordShort(t *testing.T) {
	pwd := "not_foobar"
	pwhash, _ := hex.DecodeString("aaaaaaaa")
	_, err := CheckHashingPassword(pwhash, pwd, mysql.AuthCachingSha2Password)
	require.Error(t, err)
}

func TestCheckShaPasswordDigestTypeIncompatible(t *testing.T) {
	pwd := "not_foobar"
	pwhash, _ := hex.DecodeString("24422430303524031A69251C34295C4B35167C7F1E5A7B63091349503974624D34504B5A424679354856336868686F52485A736E4A733368786E427575516C73446469496537")
	_, err := CheckHashingPassword(pwhash, pwd, mysql.AuthCachingSha2Password)
	require.Error(t, err)
}

func TestCheckShaPasswordIterationsInvalid(t *testing.T) {
	pwd := "not_foobar"
	pwhash, _ := hex.DecodeString("24412430304724031A69251C34295C4B35167C7F1E5A7B63091349503974624D34504B5A424679354856336868686F52485A736E4A733368786E427575516C73446469496537")
	_, err := CheckHashingPassword(pwhash, pwd, mysql.AuthCachingSha2Password)
	require.Error(t, err)
}

// The output from NewHashPassword is not stable as the hash is based on the generated salt.
// This is why CheckHashingPassword is used here.
func TestNewSha2Password(t *testing.T) {
	pwd := "testpwd"
	pwhash := NewHashPassword(pwd, mysql.AuthCachingSha2Password)
	r, err := CheckHashingPassword([]byte(pwhash), pwd, mysql.AuthCachingSha2Password)
	require.NoError(t, err)
	require.True(t, r)

	for r := range pwhash {
		require.Less(t, pwhash[r], uint8(128))
		require.NotEqual(t, pwhash[r], 0)  // NUL
		require.NotEqual(t, pwhash[r], 36) // '$'
	}
}

func BenchmarkShaPassword(b *testing.B) {
	for i := 0; i < b.N; i++ {
		m, err := CheckHashingPassword(foobarPwdSHA2Hash, "foobar", mysql.AuthCachingSha2Password)
		require.Nil(b, err)
		require.True(b, m)
	}
}

// Fixed fixtures use salt 01234567890123456789 and 1000 rounds. They cover
// digest/block boundaries and byte-oriented passwords (NUL, UTF-8, invalid UTF-8).
func TestHashCryptCompatibility(t *testing.T) {
	cases := []struct {
		length int
		sha2   string
		sm3    string
	}{
		{0, "$A$001$01234567890123456789DNqfbUS3KF2i3eAp7mzNRoCNpPERpdYQN3rQulIHZjA", "$A$001$01234567890123456789QIhtDy6HqozOF2YkdnqZmKm/FimFA/YwhDlrZIhTh8/"},
		{1, "$A$001$01234567890123456789mn.jPpkj5fgNLGBjhW02eOGU4FYNmuSVCJn4ZhLV6QD", "$A$001$01234567890123456789y7/yy2T1er.VDIWjHV4v2.hg1y8Trdwj63QnTaR5OJ6"},
		{31, "$A$001$01234567890123456789IqMNZ9VR2Mr17N7dAiWB1Q2KvjaI/M2MK34R0CheMT/", "$A$001$01234567890123456789Q/b3t9dqfnq3qSkMsSIHyPaiauSh8YojAhu4sDKkpR6"},
		{32, "$A$001$01234567890123456789H.6MH6ooQ5i/wfQExqCV86vdMMB5.aswRG6oa6oGS52", "$A$001$01234567890123456789noOsZ4CEqaBqJbWc4i3DhGF56KXtLShrQO1TKAspEE8"},
		{33, "$A$001$01234567890123456789Hf2TH6ARa.mRB9YprCQvFcfehFH4zAhn5Jhc/TNgmhA", "$A$001$01234567890123456789yZzcfLx2tJrts8.l4kpzAoxLpwhTAh8AkA5/xMsS/36"},
		{63, "$A$001$01234567890123456789w.7bA9.cpGMq8r.efwn6ks5nzIf3T9hwk7HOlKTiT4/", "$A$001$01234567890123456789ZALXIPD6tfilBSDbbS5xqq5tx9c.KAu8U.0Lme.DRp."},
		{64, "$A$001$01234567890123456789VFwC5AGFuEe.noCCbST3TEzXE7/ECfBvWfaCZxXu9J4", "$A$001$01234567890123456789QTmv3U9Of1BBXmCEtuwZq21GxVbiE61mVnRQ/IZJJ/3"},
		{65, "$A$001$01234567890123456789VNi4DnJ3GgqbOaKSOOuSW8V9G1FTHxhq7niEXF7eVwC", "$A$001$01234567890123456789LaZxsx33EfYJ0LDjEB4HPHiQvf67ww87iWlwiZu1Ft/"},
		{255, "$A$001$01234567890123456789hH6mCDzdkKSw.M6r62/nSrr6TNv95/Iy1cpxB5flIkC", "$A$001$01234567890123456789typDgvTgD6I2CIxUz4EamttedRN6/tA4boDZRbjNME6"},
		{256, "$A$001$01234567890123456789cTMpT04nI.0hwMsiFiBsqI.IuTmZKakq3Sese5VBOB6", "$A$001$01234567890123456789Ho0PX1frZX9DCX/vUAb7JXE4z46VYcsiDW3zhRZyocC"},
	}
	for _, tc := range cases {
		password := strings.Repeat("\x00\xc3\xa9\xff", (tc.length+3)/4)[:tc.length]
		for _, algorithm := range []struct{ plugin, digest string }{
			{mysql.AuthCachingSha2Password, tc.sha2},
			{mysql.AuthTiDBSM3Password, tc.sm3},
		} {
			t.Run(fmt.Sprintf("%s/%d", algorithm.plugin, tc.length), func(t *testing.T) {
				ok, err := CheckHashingPassword([]byte(algorithm.digest), password, algorithm.plugin)
				require.NoError(t, err)
				require.True(t, ok)
			})
		}
	}
}

func TestHashCryptAllocations(t *testing.T) {
	for _, algorithm := range []struct {
		name    string
		hash    func([]byte) []byte
		newHash func() hash.Hash
	}{
		{mysql.AuthCachingSha2Password, Sha256Hash, sha256.New},
		{mysql.AuthTiDBSM3Password, Sm3Hash, NewSM3},
	} {
		for _, length := range []int{128, 512, 2048} {
			t.Run(fmt.Sprintf("%s/%d", algorithm.name, length), func(t *testing.T) {
				password := strings.Repeat("p", length)
				salt := []byte("01234567890123456789")
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				// One round isolates password preparation from the stretching rounds.
				result := hashCrypt(password, salt, 1, algorithm.hash, algorithm.newHash)
				runtime.ReadMemStats(&after)
				require.NotEmpty(t, result)
				allocated := after.TotalAlloc - before.TotalAlloc
				t.Logf("%d-byte password: %d bytes allocated", length, allocated)
				// Allow allocator and race-instrumentation overhead while keeping
				// the budget linear and well below the old quadratic allocation.
				require.Less(t, allocated, uint64(64*1024+64*length))
			})
		}
	}
}

func TestHashingPasswordLength(t *testing.T) {
	for _, algorithm := range []struct {
		plugin string
		legacy string
	}{
		{mysql.AuthCachingSha2Password, "$A$001$01234567890123456789Upag/MxAud6snDfLlZtpcfcUH6KrG49kq78PIQlaR77"},
		{mysql.AuthTiDBSM3Password, "$A$001$01234567890123456789cSAm/xjpP6pWCxom0/CiaMY/jThLKK3miAlzMPJtu35"},
	} {
		t.Run(algorithm.plugin, func(t *testing.T) {
			// Preserve authentication of existing passwords longer than 256 bytes.
			ok, err := CheckHashingPassword([]byte(algorithm.legacy), strings.Repeat("p", 257), algorithm.plugin)
			require.NoError(t, err)
			require.True(t, ok)
			for _, password := range []string{strings.Repeat("p", 257), strings.Repeat("界", 86), strings.Repeat("p", 512)} {
				digest := NewHashPassword(password, algorithm.plugin)
				require.NotEmpty(t, digest)
				ok, err := CheckHashingPassword([]byte(digest), password, algorithm.plugin)
				require.NoError(t, err)
				require.True(t, ok)
				// A long password must be hashed in full, without truncation.
				ok, err = CheckHashingPassword([]byte(digest), password[:256], algorithm.plugin)
				require.NoError(t, err)
				require.False(t, ok)
			}
		})
	}
}

func BenchmarkCheckHashingPassword(b *testing.B) {
	for _, algorithm := range []struct {
		plugin string
		digest []byte
	}{
		{mysql.AuthCachingSha2Password, foobarPwdSHA2Hash},
		{mysql.AuthTiDBSM3Password, foobarPwdSM3Hash},
	} {
		for _, length := range []int{32, 512, 2048} {
			b.Run(fmt.Sprintf("%s/%d", algorithm.plugin, length), func(b *testing.B) {
				password := strings.Repeat("p", length)
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					ok, err := CheckHashingPassword(algorithm.digest, password, algorithm.plugin)
					if err != nil || ok {
						b.Fatalf("unexpected password verification result: match=%t, err=%v", ok, err)
					}
				}
			})
		}
	}
}
