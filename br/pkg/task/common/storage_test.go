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

package common

import (
	"context"
	"fmt"
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/stretchr/testify/require"
)

func TestReadBackupMetaFromStorage(t *testing.T) {
	ctx := context.Background()
	storage, err := objstore.NewLocalStorage(t.TempDir())
	require.NoError(t, err)
	for _, method := range []encryptionpb.EncryptionMethod{
		encryptionpb.EncryptionMethod_AES128_CTR,
		encryptionpb.EncryptionMethod_AES192_CTR,
		encryptionpb.EncryptionMethod_AES256_CTR,
	} {
		for _, size := range []int{0, 1, 15, 16} {
			t.Run(fmt.Sprintf("%s/%d", method, size), func(t *testing.T) {
				require.NoError(t, storage.WriteFile(ctx, metautil.MetaFile, make([]byte, size)))
				var meta *backuppb.BackupMeta
				var readErr error
				require.NotPanics(t, func() {
					meta, readErr = ReadBackupMetaFromStorage(ctx, metautil.MetaFile, storage,
						&backuppb.CipherInfo{CipherType: method})
				})
				if size < metautil.CrypterIvLen {
					require.ErrorContains(t, readErr, "backupmeta is too short")
					require.Nil(t, meta)
				} else {
					// Preserve the existing behavior for an IV with an empty payload.
					require.NoError(t, readErr)
					require.Equal(t, &backuppb.BackupMeta{}, meta)
				}
			})
		}
	}

	expected := &backuppb.BackupMeta{StartVersion: 42, EndVersion: 84}
	data, err := expected.Marshal()
	require.NoError(t, err)
	for _, cipher := range []*backuppb.CipherInfo{
		nil,
		{CipherType: encryptionpb.EncryptionMethod_PLAINTEXT},
		{CipherType: encryptionpb.EncryptionMethod_AES128_CTR, CipherKey: make([]byte, 16)},
		{CipherType: encryptionpb.EncryptionMethod_AES192_CTR, CipherKey: make([]byte, 24)},
		{CipherType: encryptionpb.EncryptionMethod_AES256_CTR, CipherKey: make([]byte, 32)},
	} {
		encrypted, iv, err := metautil.Encrypt(data, cipher)
		require.NoError(t, err)
		require.NoError(t, storage.WriteFile(ctx, metautil.MetaFile, append(iv, encrypted...)))
		actual, err := ReadBackupMetaFromStorage(ctx, metautil.MetaFile, storage, cipher)
		require.NoError(t, err)
		require.Equal(t, expected, actual)
	}
}
