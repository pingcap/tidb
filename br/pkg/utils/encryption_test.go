// Copyright 2026 PingCAP, Inc.
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

package utils_test

import (
	"crypto/aes"
	"fmt"
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/util/encrypt"
	"github.com/stretchr/testify/require"
)

func TestDecryptIVLength(t *testing.T) {
	for _, tc := range []struct {
		method encryptionpb.EncryptionMethod
		keyLen int
	}{
		{encryptionpb.EncryptionMethod_AES128_CTR, 16},
		{encryptionpb.EncryptionMethod_AES192_CTR, 24},
		{encryptionpb.EncryptionMethod_AES256_CTR, 32},
	} {
		t.Run(tc.method.String(), func(t *testing.T) {
			cipher := &backuppb.CipherInfo{CipherType: tc.method, CipherKey: make([]byte, tc.keyLen)}
			plaintext := []byte("data")
			iv := make([]byte, aes.BlockSize)
			ciphertext, err := encrypt.AESEncryptWithCTR(plaintext, cipher.CipherKey, iv)
			require.NoError(t, err)
			data, err := utils.Decrypt(ciphertext, cipher, iv)
			require.NoError(t, err)
			require.Equal(t, plaintext, data)
			for _, size := range []int{0, 12, 15, 17} {
				t.Run(fmt.Sprint(size), func(t *testing.T) {
					require.NotPanics(t, func() {
						data, err := utils.Decrypt(ciphertext, cipher, make([]byte, size))
						require.ErrorContains(t, err, "IV")
						require.Nil(t, data)
					})
				})
			}
		})
	}
	// Plaintext and unencrypted data do not require an IV.
	for _, cipher := range []*backuppb.CipherInfo{nil, {CipherType: encryptionpb.EncryptionMethod_PLAINTEXT}} {
		data, err := utils.Decrypt([]byte("data"), cipher, nil)
		require.NoError(t, err)
		require.Equal(t, []byte("data"), data)
	}
}
