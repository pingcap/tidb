// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package metautil

import (
	"context"
	"crypto/sha256"
	"testing"

	"github.com/golang/mock/gomock"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	mockstorage "github.com/pingcap/tidb/br/pkg/mock/storage"
	"github.com/stretchr/testify/require"
)

func checksum(m *backuppb.MetaFile) []byte {
	b, err := m.Marshal()
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256(b)
	return sum[:]
}

func TestWalkMetaFileEmpty(t *testing.T) {
	files := []*backuppb.MetaFile{}
	collect := func(m *backuppb.MetaFile) { files = append(files, m) }
	cipher := backuppb.CipherInfo{
		CipherType: encryptionpb.EncryptionMethod_PLAINTEXT,
	}

	err := walkLeafMetaFile(context.Background(), nil, nil, &cipher, collect)

	require.NoError(t, err)
	require.Len(t, files, 0)

	empty := &backuppb.MetaFile{}
	err = walkLeafMetaFile(context.Background(), nil, empty, &cipher, collect)

	require.NoError(t, err)
	require.Len(t, files, 1)
	require.Equal(t, empty, files[0])
}

func TestWalkMetaFileLeaf(t *testing.T) {
	leaf := &backuppb.MetaFile{Schemas: []*backuppb.Schema{
		{Db: []byte("db"), Table: []byte("table")},
	}}
	files := []*backuppb.MetaFile{}
	cipher := backuppb.CipherInfo{
		CipherType: encryptionpb.EncryptionMethod_PLAINTEXT,
	}
	collect := func(m *backuppb.MetaFile) { files = append(files, m) }
	err := walkLeafMetaFile(context.Background(), nil, leaf, &cipher, collect)

	require.NoError(t, err)
	require.Len(t, files, 1)
	require.Equal(t, leaf, files[0])
}

func TestWalkMetaFileInvalid(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()
	mockStorage := mockstorage.NewMockExternalStorage(controller)

	ctx := context.Background()
	leaf := &backuppb.MetaFile{Schemas: []*backuppb.Schema{
		{Db: []byte("db"), Table: []byte("table")},
	}}
	mockStorage.EXPECT().ReadFile(ctx, "leaf").Return(leaf.Marshal())

	root := &backuppb.MetaFile{MetaFiles: []*backuppb.File{
		{Name: "leaf", Sha256: []byte{}},
	}}

	cipher := backuppb.CipherInfo{
		CipherType: encryptionpb.EncryptionMethod_PLAINTEXT,
	}
	collect := func(m *backuppb.MetaFile) { panic("unreachable") }
	err := walkLeafMetaFile(ctx, mockStorage, root, &cipher, collect)

	require.Error(t, err)
	require.Contains(t, err.Error(), "ErrInvalidMetaFile")
}

func TestWalkMetaFile(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()
	mockStorage := mockstorage.NewMockExternalStorage(controller)

	ctx := context.Background()
	expect := make([]*backuppb.MetaFile, 0, 6)
	leaf31S1 := &backuppb.MetaFile{Schemas: []*backuppb.Schema{
		{Db: []byte("db31S1"), Table: []byte("table31S1")},
	}}
	mockStorage.EXPECT().ReadFile(ctx, "leaf31S1").Return(leaf31S1.Marshal())
	expect = append(expect, leaf31S1)

	leaf31S2 := &backuppb.MetaFile{Schemas: []*backuppb.Schema{
		{Db: []byte("db31S2"), Table: []byte("table31S2")},
	}}
	mockStorage.EXPECT().ReadFile(ctx, "leaf31S2").Return(leaf31S2.Marshal())
	expect = append(expect, leaf31S2)

	leaf32S1 := &backuppb.MetaFile{Schemas: []*backuppb.Schema{
		{Db: []byte("db32S1"), Table: []byte("table32S1")},
	}}
	mockStorage.EXPECT().ReadFile(ctx, "leaf32S1").Return(leaf32S1.Marshal())
	expect = append(expect, leaf32S1)

	node21 := &backuppb.MetaFile{MetaFiles: []*backuppb.File{
		{Name: "leaf31S1", Sha256: checksum(leaf31S1)},
		{Name: "leaf31S2", Sha256: checksum(leaf31S2)},
	}}
	mockStorage.EXPECT().ReadFile(ctx, "node21").Return(node21.Marshal())

	node22 := &backuppb.MetaFile{MetaFiles: []*backuppb.File{
		{Name: "leaf32S1", Sha256: checksum(leaf32S1)},
	}}
	mockStorage.EXPECT().ReadFile(ctx, "node22").Return(node22.Marshal())

	leaf23S1 := &backuppb.MetaFile{Schemas: []*backuppb.Schema{
		{Db: []byte("db23S1"), Table: []byte("table23S1")},
	}}
	mockStorage.EXPECT().ReadFile(ctx, "leaf23S1").Return(leaf23S1.Marshal())
	expect = append(expect, leaf23S1)

	root := &backuppb.MetaFile{MetaFiles: []*backuppb.File{
		{Name: "node21", Sha256: checksum(node21)},
		{Name: "node22", Sha256: checksum(node22)},
		{Name: "leaf23S1", Sha256: checksum(leaf23S1)},
	}}

	files := []*backuppb.MetaFile{}
	cipher := backuppb.CipherInfo{
		CipherType: encryptionpb.EncryptionMethod_PLAINTEXT,
	}
	collect := func(m *backuppb.MetaFile) { files = append(files, m) }
	err := walkLeafMetaFile(ctx, mockStorage, root, &cipher, collect)
	require.NoError(t, err)

	require.Len(t, files, len(expect))
	for i := range expect {
		require.Equal(t, expect[i], files[i])
	}
}

type encryptTest struct {
	method   encryptionpb.EncryptionMethod
	rightKey string
	wrongKey string
}

func TestEncryptAndDecrypt(t *testing.T) {
	originalData := []byte("pingcap")
	testCases := []encryptTest{
		{
			method: encryptionpb.EncryptionMethod_UNKNOWN,
		},
		{
			method: encryptionpb.EncryptionMethod_PLAINTEXT,
		},
		{
			method:   encryptionpb.EncryptionMethod_AES128_CTR,
			rightKey: "0123456789012345",
			wrongKey: "012345678901234",
		},
		{
			method:   encryptionpb.EncryptionMethod_AES192_CTR,
			rightKey: "012345678901234567890123",
			wrongKey: "0123456789012345678901234",
		},
		{
			method:   encryptionpb.EncryptionMethod_AES256_CTR,
			rightKey: "01234567890123456789012345678901",
			wrongKey: "01234567890123456789012345678902",
		},
	}

	for _, v := range testCases {
		cipher := backuppb.CipherInfo{
			CipherType: v.method,
			CipherKey:  []byte(v.rightKey),
		}
		encryptData, iv, err := Encrypt(originalData, &cipher)
		if v.method == encryptionpb.EncryptionMethod_UNKNOWN {
			require.Error(t, err)
		} else if v.method == encryptionpb.EncryptionMethod_PLAINTEXT {
			require.NoError(t, err)
			require.Equal(t, originalData, encryptData)

			decryptData, err := Decrypt(encryptData, &cipher, iv)
			require.NoError(t, err)
			require.Equal(t, decryptData, originalData)
		} else {
			require.NoError(t, err)
			require.NotEqual(t, originalData, encryptData)

			decryptData, err := Decrypt(encryptData, &cipher, iv)
			require.NoError(t, err)
			require.Equal(t, decryptData, originalData)

			wrongCipher := backuppb.CipherInfo{
				CipherType: v.method,
				CipherKey:  []byte(v.wrongKey),
			}
			decryptData, err = Decrypt(encryptData, &wrongCipher, iv)
			if len(v.rightKey) != len(v.wrongKey) {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotEqual(t, decryptData, originalData)
			}
		}
	}
}
