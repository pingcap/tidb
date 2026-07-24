// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package metautil

import (
	"context"
	"crypto/sha256"
	"sync"
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
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

func appendVarintField(dst []byte, fieldNumber protowire.Number, v uint64) []byte {
	dst = protowire.AppendTag(dst, fieldNumber, protowire.VarintType)
	dst = protowire.AppendVarint(dst, v)
	return dst
}

func appendBytesField(dst []byte, fieldNumber protowire.Number, v []byte) []byte {
	dst = protowire.AppendTag(dst, fieldNumber, protowire.BytesType)
	dst = protowire.AppendBytes(dst, v)
	return dst
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
	store, err := objstore.NewLocalStorage(fakeDataDir)
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
	store, err := objstore.NewLocalStorage(fakeDataDir)
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

			decryptData, err := utils.Decrypt(encryptData, &cipher, iv)
			require.NoError(t, err)
			require.Equal(t, decryptData, originalData)
		} else {
			require.NoError(t, err)
			require.NotEqual(t, originalData, encryptData)

			decryptData, err := utils.Decrypt(encryptData, &cipher, iv)
			require.NoError(t, err)
			require.Equal(t, decryptData, originalData)

			wrongCipher := backuppb.CipherInfo{
				CipherType: v.method,
				CipherKey:  []byte(v.wrongKey),
			}
			decryptData, err = utils.Decrypt(encryptData, &wrongCipher, iv)
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

func TestCheckBackupMetaCompatibility(t *testing.T) {
	baseMeta := &backuppb.BackupMeta{
		BackupSchemaVersion: backuppb.BackupSchemaVersion,
		ClusterVersion:      "8.5.6",
		BrVersion:           "v8.5.6",
	}

	baseMetaBytes, err := baseMeta.Marshal()
	require.NoError(t, err)
	newerSchemaMeta := &backuppb.BackupMeta{
		BackupSchemaVersion: backuppb.BackupSchemaVersion + 1,
		ClusterVersion:      "8.5.6",
		BrVersion:           "v8.5.6",
	}
	newerSchemaMetaBytes, err := newerSchemaMeta.Marshal()
	require.NoError(t, err)

	// Cover the compatibility contract for backupmeta:
	// 1. current schema version should pass;
	// 2. newer schema version should be rejected;
	// 3. compatibility check requires raw bytes.
	cases := []struct {
		name        string
		backupBytes []byte
		backupMeta  *backuppb.BackupMeta
		expectedErr string
	}{
		{
			name:        "compatible backupmeta",
			backupBytes: baseMetaBytes,
			backupMeta:  baseMeta,
		},
		{
			name:        "reject newer schema version",
			backupBytes: newerSchemaMetaBytes,
			backupMeta:  newerSchemaMeta,
			expectedErr: "requires schema version",
		},
		{
			name:        "reject empty bytes input",
			backupBytes: nil,
			backupMeta:  baseMeta,
			expectedErr: "bytes are required",
		},
	}

	for _, ca := range cases {
		t.Run(ca.name, func(t *testing.T) {
			err := CheckBackupMetaCompatibilityFromBytes(ca.backupBytes, ca.backupMeta)
			if ca.expectedErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, ca.expectedErr)
		})
	}
}

func TestCheckBackupMetaCompatibilityFromBytesDetectsNestedUnknownFields(t *testing.T) {
	// Build a nested File message and append an unknown field(200) into it.
	nestedFile := make([]byte, 0, 64)
	nestedFile = appendBytesField(nestedFile, 1, []byte("nested-file"))
	nestedFile = appendVarintField(nestedFile, 200, 1)

	// Build BackupMeta bytes manually to keep the nested unknown field in wire bytes.
	backupMetaBytes := make([]byte, 0, 128)
	backupMetaBytes = appendBytesField(backupMetaBytes, 2, []byte("8.5.6"))
	backupMetaBytes = appendBytesField(backupMetaBytes, 11, []byte("v8.5.6"))
	backupMetaBytes = appendVarintField(backupMetaBytes, 26, uint64(backuppb.BackupSchemaVersion))
	backupMetaBytes = appendBytesField(backupMetaBytes, 4, nestedFile)

	backupMeta := &backuppb.BackupMeta{}
	require.NoError(t, backupMeta.Unmarshal(backupMetaBytes))

	err := CheckBackupMetaCompatibilityFromBytes(backupMetaBytes, backupMeta)
	require.ErrorContains(t, err, "unknown protobuf fields")
}

func TestCheckBackupMetaCompatibilityFromBytesDetectsTopLevelUnknownFields(t *testing.T) {
	backupMetaBytes := make([]byte, 0, 128)
	backupMetaBytes = appendBytesField(backupMetaBytes, 2, []byte("8.5.6"))
	backupMetaBytes = appendBytesField(backupMetaBytes, 11, []byte("v8.5.6"))
	backupMetaBytes = appendVarintField(backupMetaBytes, 26, uint64(backuppb.BackupSchemaVersion))
	backupMetaBytes = appendVarintField(backupMetaBytes, 200, 1)

	backupMeta := &backuppb.BackupMeta{}
	require.NoError(t, backupMeta.Unmarshal(backupMetaBytes))

	err := CheckBackupMetaCompatibilityFromBytes(backupMetaBytes, backupMeta)
	require.ErrorContains(t, err, "unknown protobuf fields")
}

func TestCheckBackupMetaCompatibilityFromBytesDetectsDeepNestedUnknownFields(t *testing.T) {
	// Build File bytes with unknown field(201).
	nestedFile := make([]byte, 0, 64)
	nestedFile = appendBytesField(nestedFile, 1, []byte("deep-file"))
	nestedFile = appendVarintField(nestedFile, 201, 1)

	// Build MetaFile bytes that contains the File in meta_files.
	nestedMetaFile := make([]byte, 0, 96)
	nestedMetaFile = appendBytesField(nestedMetaFile, 1, nestedFile)

	// Build BackupMeta bytes that contains the MetaFile in file_index.
	backupMetaBytes := make([]byte, 0, 192)
	backupMetaBytes = appendBytesField(backupMetaBytes, 2, []byte("8.5.6"))
	backupMetaBytes = appendBytesField(backupMetaBytes, 11, []byte("v8.5.6"))
	backupMetaBytes = appendVarintField(backupMetaBytes, 26, uint64(backuppb.BackupSchemaVersion))
	backupMetaBytes = appendBytesField(backupMetaBytes, 13, nestedMetaFile)

	backupMeta := &backuppb.BackupMeta{}
	require.NoError(t, backupMeta.Unmarshal(backupMetaBytes))

	err := CheckBackupMetaCompatibilityFromBytes(backupMetaBytes, backupMeta)
	require.ErrorContains(t, err, "unknown protobuf fields")
}

func TestNewMetaWriterInitializesBackupSchemaVersion(t *testing.T) {
	// New backupmeta files should carry the current compatibility version by default
	// so readers can distinguish new semantics from pre-versioned backups.
	writer := NewMetaWriter(nil, MetaFileSize, false, "", nil)
	require.Equal(t, backuppb.BackupSchemaVersion, writer.backupMeta.BackupSchemaVersion)
}
