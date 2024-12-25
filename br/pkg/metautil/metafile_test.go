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
	"github.com/pingcap/tidb/br/pkg/utils"
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

func TestMergePhysicalRangeLink(t *testing.T) {
	// 1 - 3 - 5 - 7 - 9
	// 2 - 6
	// 4 - 8
	m := map[int64]int64{
		1: 3,
		4: 8,
		7: 9,
		5: 7,
		2: 6,
		3: 5,
	}
	mergePhysicalRangeLink(m)
	require.Len(t, m, 3)
	require.Equal(t, map[int64]int64{1: 9, 2: 6, 4: 8}, m)
}

func TestCalculateChecksumStatsOnFiles(t *testing.T) {
	generateTableMeta := func(id int64, kvs, bytes, checksum uint64) *backuppb.TableMeta {
		return &backuppb.TableMeta{
			PhysicalId: id,
			TotalKvs:   kvs,
			TotalBytes: bytes,
			Crc64Xor:   checksum,
		}
	}
	file1 := &backuppb.File{
		TableMetas: []*backuppb.TableMeta{
			generateTableMeta(1, 10, 20, 30),
			generateTableMeta(3, 30, 40, 50),
		},
	}
	file2 := &backuppb.File{
		TableMetas: []*backuppb.TableMeta{
			generateTableMeta(3, 40, 50, 60),
		},
	}
	file3 := &backuppb.File{
		TableMetas: []*backuppb.TableMeta{
			generateTableMeta(3, 50, 60, 70),
			generateTableMeta(4, 40, 50, 60),
			generateTableMeta(100, 1000, 2000, 3000),
		},
	}
	file4 := &backuppb.File{
		TableMetas: []*backuppb.TableMeta{
			generateTableMeta(102, 1002, 2002, 3002),
			generateTableMeta(103, 1003, 2003, 3003),
		},
	}
	// table id : 1, partition ids: 3, 100, 102
	tbl := &Table{
		FilesOfPhysicals: map[int64][]*backuppb.File{
			1:   {file1},
			3:   {file1, file2, file3},
			100: {file3},
			102: {file4},
		},
	}
	checksumStats := tbl.CalculateChecksumStatsOnFiles()
	require.Equal(t, uint64(10+30+40+50+1000+1002), checksumStats.TotalKvs)
	require.Equal(t, uint64(20+40+50+60+2000+2002), checksumStats.TotalBytes)
	require.Equal(t, uint64(30^50^60^70^3000^3002), checksumStats.Crc64Xor)
}

func TestCalculateKvStatsOnFile(t *testing.T) {
	totalKvs, totalBytes := CalculateKvStatsOnFile(&backuppb.File{
		TableMetas: []*backuppb.TableMeta{
			{
				PhysicalId: 1,
				TotalKvs:   100,
				TotalBytes: 100,
				Crc64Xor:   100,
			},
			{
				PhysicalId: 2,
				TotalKvs:   200,
				TotalBytes: 200,
				Crc64Xor:   200,
			},
			{
				PhysicalId: 3,
				TotalKvs:   300,
				TotalBytes: 300,
				Crc64Xor:   300,
			},
		},
	})
	require.Equal(t, 600, totalKvs)
	require.Equal(t, 600, totalBytes)
	totalKvs, totalBytes = CalculateKvStatsOnFile(&backuppb.File{
		TotalKvs:   100,
		TotalBytes: 100,
	})
	require.Equal(t, 100, totalKvs)
	require.Equal(t, 100, totalBytes)
}

func TestCalculateKvStatsOnFiles(t *testing.T) {
	totalKvs, totalBytes := CalculateKvStatsOnFiles([]*backuppb.File{
		{
			TableMetas: []*backuppb.TableMeta{
				{
					PhysicalId: 1,
					TotalKvs:   100,
					TotalBytes: 100,
					Crc64Xor:   100,
				},
				{
					PhysicalId: 2,
					TotalKvs:   200,
					TotalBytes: 200,
					Crc64Xor:   200,
				},
				{
					PhysicalId: 3,
					TotalKvs:   300,
					TotalBytes: 300,
					Crc64Xor:   300,
				},
			},
		},
		{
			TableMetas: []*backuppb.TableMeta{
				{
					PhysicalId: 4,
					TotalKvs:   400,
					TotalBytes: 400,
					Crc64Xor:   400,
				},
				{
					PhysicalId: 5,
					TotalKvs:   500,
					TotalBytes: 500,
					Crc64Xor:   500,
				},
				{
					PhysicalId: 6,
					TotalKvs:   600,
					TotalBytes: 600,
					Crc64Xor:   600,
				},
			},
		},
	})
	require.Equal(t, uint64(2100), totalKvs)
	require.Equal(t, uint64(2100), totalBytes)
	totalKvs, totalBytes = CalculateKvStatsOnFiles([]*backuppb.File{
		{
			TotalKvs:   100,
			TotalBytes: 100,
		},
		{
			TotalKvs:   200,
			TotalBytes: 200,
		},
	})
	require.Equal(t, uint64(300), totalKvs)
	require.Equal(t, uint64(300), totalBytes)
}
