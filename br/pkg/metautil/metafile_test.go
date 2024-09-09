// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package metautil

import (
	"context"
	"crypto/sha256"
	"sync"
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/tidb/br/pkg/storage"
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

func marshal(t *testing.T, m *backuppb.MetaFile) []byte {
	data, err := m.Marshal()
	require.NoError(t, err)
	return data
}

func TestWalkMetaFileEmpty(t *testing.T) {
	var mu sync.Mutex
	files := []*backuppb.MetaFile{}
	collect := func(m *backuppb.MetaFile) { mu.Lock(); defer mu.Unlock(); files = append(files, m) }
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
	var mu sync.Mutex
	leaf := &backuppb.MetaFile{Schemas: []*backuppb.Schema{
		{Db: []byte("db"), Table: []byte("table")},
	}}
	files := []*backuppb.MetaFile{}
	cipher := backuppb.CipherInfo{
		CipherType: encryptionpb.EncryptionMethod_PLAINTEXT,
	}
	collect := func(m *backuppb.MetaFile) { mu.Lock(); defer mu.Unlock(); files = append(files, m) }
	err := walkLeafMetaFile(context.Background(), nil, leaf, &cipher, collect)

	require.NoError(t, err)
	require.Len(t, files, 1)
	require.Equal(t, leaf, files[0])
}

func TestWalkMetaFileInvalid(t *testing.T) {
	fakeDataDir := t.TempDir()
	store, err := storage.NewLocalStorage(fakeDataDir)
	require.NoError(t, err)

	ctx := context.Background()
	leaf := &backuppb.MetaFile{Schemas: []*backuppb.Schema{
		{Db: []byte("db"), Table: []byte("table")},
	}}
	store.WriteFile(ctx, "leaf", marshal(t, leaf))

	root := &backuppb.MetaFile{MetaFiles: []*backuppb.File{
		{Name: "leaf", Sha256: []byte{}},
	}}

	cipher := backuppb.CipherInfo{
		CipherType: encryptionpb.EncryptionMethod_PLAINTEXT,
	}
	collect := func(m *backuppb.MetaFile) { panic("unreachable") }
	err = walkLeafMetaFile(ctx, store, root, &cipher, collect)

	require.Error(t, err)
	require.Contains(t, err.Error(), "ErrInvalidMetaFile")
}

func TestWalkMetaFile(t *testing.T) {
	fakeDataDir := t.TempDir()
	store, err := storage.NewLocalStorage(fakeDataDir)
	require.NoError(t, err)

	ctx := context.Background()
	expect := make(map[string]*backuppb.MetaFile)
	leaf31S1 := &backuppb.MetaFile{Schemas: []*backuppb.Schema{
		{Db: []byte("db31S1"), Table: []byte("table31S1")},
	}}
	store.WriteFile(ctx, "leaf31S1", marshal(t, leaf31S1))
	expect["db31S1"] = leaf31S1

	leaf31S2 := &backuppb.MetaFile{Schemas: []*backuppb.Schema{
		{Db: []byte("db31S2"), Table: []byte("table31S2")},
	}}
	store.WriteFile(ctx, "leaf31S2", marshal(t, leaf31S2))
	expect["db31S2"] = leaf31S2

	leaf32S1 := &backuppb.MetaFile{Schemas: []*backuppb.Schema{
		{Db: []byte("db32S1"), Table: []byte("table32S1")},
	}}
	store.WriteFile(ctx, "leaf32S1", marshal(t, leaf32S1))
	expect["db32S1"] = leaf32S1

	node21 := &backuppb.MetaFile{MetaFiles: []*backuppb.File{
		{Name: "leaf31S1", Sha256: checksum(leaf31S1)},
		{Name: "leaf31S2", Sha256: checksum(leaf31S2)},
	}}
	store.WriteFile(ctx, "node21", marshal(t, node21))

	node22 := &backuppb.MetaFile{MetaFiles: []*backuppb.File{
		{Name: "leaf32S1", Sha256: checksum(leaf32S1)},
	}}
	store.WriteFile(ctx, "node22", marshal(t, node22))

	leaf23S1 := &backuppb.MetaFile{Schemas: []*backuppb.Schema{
		{Db: []byte("db23S1"), Table: []byte("table23S1")},
	}}
	store.WriteFile(ctx, "leaf23S1", marshal(t, leaf23S1))
	expect["db23S1"] = leaf23S1

	root := &backuppb.MetaFile{MetaFiles: []*backuppb.File{
		{Name: "node21", Sha256: checksum(node21)},
		{Name: "node22", Sha256: checksum(node22)},
		{Name: "leaf23S1", Sha256: checksum(leaf23S1)},
	}}

	var mu sync.Mutex
	files := []*backuppb.MetaFile{}
	cipher := backuppb.CipherInfo{
		CipherType: encryptionpb.EncryptionMethod_PLAINTEXT,
	}
	collect := func(m *backuppb.MetaFile) {
		mu.Lock()
		files = append(files, m)
		mu.Unlock()
	}
	err = walkLeafMetaFile(ctx, store, root, &cipher, collect)
	require.NoError(t, err)

	require.Len(t, files, len(expect))
	for _, file := range files {
		require.Equal(t, expect[string(file.Schemas[0].Db)], file)
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

func TestMetaFileSize(t *testing.T) {
	files := []*backuppb.File{
		{Name: "f0", Size_: 99999}, // Size() is 8
		{Name: "f1", Size_: 99999},
		{Name: "f2", Size_: 99999},
		{Name: "f3", Size_: 99999},
		{Name: "f4", Size_: 99999},
		{Name: "f5", Size_: 99999},
	}
	metafiles := NewSizedMetaFile(50) // >= 50, then flush

	needFlush := metafiles.append(files, AppendDataFile)
	t.Logf("needFlush: %v, %+v", needFlush, metafiles)
	require.False(t, needFlush)

	needFlush = metafiles.append([]*backuppb.File{
		{Name: "f5", Size_: 99999},
	}, AppendDataFile)
	t.Logf("needFlush: %v, %+v", needFlush, metafiles)
	require.True(t, needFlush)

	metas := []*backuppb.File{
		{Name: "meta0", Size_: 99999}, // Size() is 11
		{Name: "meta1", Size_: 99999},
		{Name: "meta2", Size_: 99999},
		{Name: "meta3", Size_: 99999},
	}
	metafiles = NewSizedMetaFile(50)
	for _, meta := range metas {
		needFlush = metafiles.append(meta, AppendMetaFile)
		t.Logf("needFlush: %v, %+v", needFlush, metafiles)
		require.False(t, needFlush)
	}
	needFlush = metafiles.append(&backuppb.File{Name: "meta4", Size_: 99999}, AppendMetaFile)
	t.Logf("needFlush: %v, %+v", needFlush, metafiles)
	require.True(t, needFlush)
}
