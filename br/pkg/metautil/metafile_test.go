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
	"github.com/pingcap/tidb/br/pkg/rtree"
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

func TestTransformRestoreRanges(t *testing.T) {
	items := []*BackupItem{
		{File: &backuppb.File{Name: "f0", StartKey: []byte("a"), EndKey: []byte("b")}},
		{File: &backuppb.File{Name: "f1", StartKey: []byte("c"), EndKey: []byte("d")}},
		{File: &backuppb.File{Name: "f2", StartKey: []byte("e"), EndKey: []byte("f")}},
	}
	mergeFn := func(files []*backuppb.File) []rtree.Range {
		return []rtree.Range{
			{
				StartKey: files[0].GetStartKey(),
				EndKey:   files[len(files)-1].GetEndKey(),
				Files:    files,
			},
		}
	}
	rgs := BackupItems(items).ToRestoreRanges(mergeFn)
	require.Equal(t, rgs, &RestoreRanges{
		Typ: MergedFile,
		Ranges: []rtree.Range{
			{
				StartKey: items[0].File.GetStartKey(),
				EndKey:   items[2].File.GetEndKey(),
				Files:    BackupItems(items).GetOriginFiles(),
			},
		},
		ImportedRangeIndex: 0,
		ImportedFileIndex:  0,
	})

	items = []*BackupItem{
		{Range: &backuppb.BackupRange{StartKey: []byte("a"), EndKey: []byte("b"), Files: []*backuppb.File{{Name: "f0"}}}},
		{Range: &backuppb.BackupRange{StartKey: []byte("c"), EndKey: []byte("d"), Files: []*backuppb.File{{Name: "f1"}}}},
		{Range: &backuppb.BackupRange{StartKey: []byte("e"), EndKey: []byte("f"), Files: []*backuppb.File{{Name: "f2"}}}},
	}
	rgs = BackupItems(items).ToRestoreRanges(mergeFn)
	require.Equal(t, rgs, &RestoreRanges{
		Typ: OriginalFile,
		Ranges: []rtree.Range{
			{
				StartKey: items[0].Range.GetStartKey(),
				EndKey:   items[0].Range.GetEndKey(),
				Files:    items[0].Range.Files,
			},
			{
				StartKey: items[1].Range.GetStartKey(),
				EndKey:   items[1].Range.GetEndKey(),
				Files:    items[1].Range.Files,
			},
			{
				StartKey: items[2].Range.GetStartKey(),
				EndKey:   items[2].Range.GetEndKey(),
				Files:    items[2].Range.Files,
			},
		},
		ImportedRangeIndex: 0,
		ImportedFileIndex:  0,
	})

	// For Backup range, don't need mergeFn.
	rgs = BackupItems(items).ToRestoreRanges(nil)
	require.Equal(t, rgs, &RestoreRanges{
		Typ: OriginalFile,
		Ranges: []rtree.Range{
			{
				StartKey: items[0].Range.GetStartKey(),
				EndKey:   items[0].Range.GetEndKey(),
				Files:    items[0].Range.Files,
			},
			{
				StartKey: items[1].Range.GetStartKey(),
				EndKey:   items[1].Range.GetEndKey(),
				Files:    items[1].Range.Files,
			},
			{
				StartKey: items[2].Range.GetStartKey(),
				EndKey:   items[2].Range.GetEndKey(),
				Files:    items[2].Range.Files,
			},
		},
		ImportedRangeIndex: 0,
		ImportedFileIndex:  0,
	})

	// not support mixed item for now
	items = []*BackupItem{
		{File: &backuppb.File{Name: "f0", StartKey: []byte("a"), EndKey: []byte("b")}},
		{Range: &backuppb.BackupRange{StartKey: []byte("c"), EndKey: []byte("d"), Files: []*backuppb.File{{Name: "f1"}}}},
		{Range: &backuppb.BackupRange{StartKey: []byte("e"), EndKey: []byte("f"), Files: []*backuppb.File{{Name: "f2"}}}},
	}
	require.Panics(t, func() { BackupItems(items).ToRestoreRanges(mergeFn) })

	// not support item with both File and Range for now
	items = []*BackupItem{
		{
			File:  &backuppb.File{Name: "f0", StartKey: []byte("a"), EndKey: []byte("b")},
			Range: &backuppb.BackupRange{StartKey: []byte("a"), EndKey: []byte("b"), Files: []*backuppb.File{{Name: "f0"}}},
		},
		{Range: &backuppb.BackupRange{StartKey: []byte("c"), EndKey: []byte("d"), Files: []*backuppb.File{{Name: "f1"}}}},
		{Range: &backuppb.BackupRange{StartKey: []byte("e"), EndKey: []byte("f"), Files: []*backuppb.File{{Name: "f2"}}}},
	}
	require.Panics(t, func() { BackupItems(items).ToRestoreRanges(mergeFn) })

	// not support item with both File and Range for now
	items = []*BackupItem{
		{Range: &backuppb.BackupRange{StartKey: []byte("c"), EndKey: []byte("d"), Files: []*backuppb.File{{Name: "f1"}}}},
		{
			File:  &backuppb.File{Name: "f0", StartKey: []byte("a"), EndKey: []byte("b")},
			Range: &backuppb.BackupRange{StartKey: []byte("a"), EndKey: []byte("b"), Files: []*backuppb.File{{Name: "f0"}}},
		},
		{Range: &backuppb.BackupRange{StartKey: []byte("e"), EndKey: []byte("f"), Files: []*backuppb.File{{Name: "f2"}}}},
	}
	require.Panics(t, func() { BackupItems(items).ToRestoreRanges(mergeFn) })

}

func TestRestoreRangesIterator(t *testing.T) {
	cases := []struct {
		rg  *RestoreRanges
		res [][]*backuppb.File
	}{
		{
			&RestoreRanges{
				Typ: MergedFile,
				Ranges: []rtree.Range{
					{
						StartKey: []byte("a"),
						EndKey:   []byte("bb"),
						Files: []*backuppb.File{
							{
								Name:     "f0_write.sst",
								StartKey: []byte("a"),
								EndKey:   []byte("b"),
							},
							{
								Name:     "f0_default.sst",
								StartKey: []byte("a"),
								EndKey:   []byte("b"),
							},
							{
								Name:     "f1_default.sst",
								StartKey: []byte("b"),
								EndKey:   []byte("bb"),
							},
						},
					},
				},
			},
			[][]*backuppb.File{
				// the files with the same name belong to the same batch
				{
					{
						Name:     "f0_write.sst",
						StartKey: []byte("a"),
						EndKey:   []byte("b"),
					},
					{
						Name:     "f0_default.sst",
						StartKey: []byte("a"),
						EndKey:   []byte("b"),
					},
				},
				{
					{
						Name:     "f1_default.sst",
						StartKey: []byte("b"),
						EndKey:   []byte("bb"),
					},
				},
			},
		},
		{
			&RestoreRanges{
				Typ: MergedFile,
				Ranges: []rtree.Range{
					{
						StartKey: []byte("a"),
						EndKey:   []byte("bb"),
						Files: []*backuppb.File{
							{
								Name:     "f0_write.sst",
								StartKey: []byte("a"),
								EndKey:   []byte("b"),
							},
							{
								Name:     "f0_default.sst",
								StartKey: []byte("a"),
								EndKey:   []byte("b"),
							},
							{
								Name:     "f1_default.sst",
								StartKey: []byte("b"),
								EndKey:   []byte("bb"),
							},
						},
					},
					{
						StartKey: []byte("bb"),
						EndKey:   []byte("c"),
						Files: []*backuppb.File{
							{
								Name:     "f2_write.sst",
								StartKey: []byte("bb"),
								EndKey:   []byte("c"),
							},
							{
								Name:     "f2_default.sst",
								StartKey: []byte("bb"),
								EndKey:   []byte("c"),
							},
						},
					},
				},
			},
			[][]*backuppb.File{
				// the files with the same name belong to the same batch
				{
					{
						Name:     "f0_write.sst",
						StartKey: []byte("a"),
						EndKey:   []byte("b"),
					},
					{
						Name:     "f0_default.sst",
						StartKey: []byte("a"),
						EndKey:   []byte("b"),
					},
				},
				{
					{
						Name:     "f1_default.sst",
						StartKey: []byte("b"),
						EndKey:   []byte("bb"),
					},
				},
				{
					{
						Name:     "f2_write.sst",
						StartKey: []byte("bb"),
						EndKey:   []byte("c"),
					},
					{
						Name:     "f2_default.sst",
						StartKey: []byte("bb"),
						EndKey:   []byte("c"),
					},
				},
			},
		},
		{
			&RestoreRanges{
				Typ: OriginalFile,
				Ranges: []rtree.Range{
					{
						StartKey: []byte("a"),
						EndKey:   []byte("bb"),
						Files: []*backuppb.File{
							{
								Name:     "f0_write.sst",
								StartKey: []byte("a"),
								EndKey:   []byte("b"),
							},
							{
								Name:     "f0_default.sst",
								StartKey: []byte("a"),
								EndKey:   []byte("b"),
							},
							{
								Name:     "f1_default.sst",
								StartKey: []byte("b"),
								EndKey:   []byte("bb"),
							},
						},
					},
				},
			},
			[][]*backuppb.File{
				{
					{
						Name:     "f0_write.sst",
						StartKey: []byte("a"),
						EndKey:   []byte("b"),
					},
					{
						Name:     "f0_default.sst",
						StartKey: []byte("a"),
						EndKey:   []byte("b"),
					},
					{
						Name:     "f1_default.sst",
						StartKey: []byte("b"),
						EndKey:   []byte("bb"),
					},
				},
			},
		},
	}

	for _, ca := range cases {
		res := make([][]*backuppb.File, 0, 1)
		for {
			files := ca.rg.NextFiles()
			if len(files) == 0 {
				break
			}
			res = append(res, files)
		}
		require.ElementsMatch(t, res, ca.res)
	}
}
