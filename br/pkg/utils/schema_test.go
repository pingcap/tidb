// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/golang/protobuf/proto"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/stretchr/testify/require"
)

func mockBackupMeta(mockSchemas []*backuppb.Schema, mockFiles []*backuppb.File) *backuppb.BackupMeta {
	return &backuppb.BackupMeta{
		Files:   mockFiles,
		Schemas: mockSchemas,
	}
}

func TestLoadBackupMeta(t *testing.T) {
	testDir := t.TempDir()
	store, err := storage.NewLocalStorage(testDir)
	require.NoError(t, err)

	tblName := model.NewCIStr("t1")
	dbName := model.NewCIStr("test")
	tblID := int64(123)
	mockTbl := &model.TableInfo{
		ID:   tblID,
		Name: tblName,
	}
	mockStats := handle.JSONTable{
		DatabaseName: dbName.String(),
		TableName:    tblName.String(),
	}
	mockDB := model.DBInfo{
		ID:   1,
		Name: dbName,
		Tables: []*model.TableInfo{
			mockTbl,
		},
	}
	dbBytes, err := json.Marshal(mockDB)
	require.NoError(t, err)
	tblBytes, err := json.Marshal(mockTbl)
	require.NoError(t, err)
	statsBytes, err := json.Marshal(mockStats)
	require.NoError(t, err)

	mockSchemas := []*backuppb.Schema{
		{
			Db:    dbBytes,
			Table: tblBytes,
			Stats: statsBytes,
		},
	}

	mockFiles := []*backuppb.File{
		// should include 1.sst
		{
			Name:     "1.sst",
			StartKey: tablecodec.EncodeRowKey(tblID, []byte("a")),
			EndKey:   tablecodec.EncodeRowKey(tblID+1, []byte("a")),
		},
		// shouldn't include 2.sst
		{
			Name:     "2.sst",
			StartKey: tablecodec.EncodeRowKey(tblID-1, []byte("a")),
			EndKey:   tablecodec.EncodeRowKey(tblID, []byte("a")),
		},
	}

	meta := mockBackupMeta(mockSchemas, mockFiles)
	data, err := proto.Marshal(meta)
	require.NoError(t, err)

	ctx := context.Background()
	err = store.WriteFile(ctx, metautil.MetaFile, data)
	require.NoError(t, err)

	dbs, err := LoadBackupTables(
		ctx,
		metautil.NewMetaReader(
			meta,
			store,
			&backuppb.CipherInfo{
				CipherType: encryptionpb.EncryptionMethod_PLAINTEXT,
			}),
	)
	tbl := dbs[dbName.String()].GetTable(tblName.String())
	require.NoError(t, err)
	require.Len(t, tbl.Files, 1)
	require.Equal(t, "1.sst", tbl.Files[0].Name)
}

func TestLoadBackupMetaPartionTable(t *testing.T) {
	testDir := t.TempDir()
	store, err := storage.NewLocalStorage(testDir)
	require.NoError(t, err)

	tblName := model.NewCIStr("t1")
	dbName := model.NewCIStr("test")
	tblID := int64(123)
	partID1 := int64(124)
	partID2 := int64(125)
	mockTbl := &model.TableInfo{
		ID:   tblID,
		Name: tblName,
		Partition: &model.PartitionInfo{
			Definitions: []model.PartitionDefinition{
				{ID: partID1},
				{ID: partID2},
			},
		},
	}
	mockStats := handle.JSONTable{
		DatabaseName: dbName.String(),
		TableName:    tblName.String(),
	}
	mockDB := model.DBInfo{
		ID:   1,
		Name: dbName,
		Tables: []*model.TableInfo{
			mockTbl,
		},
	}
	dbBytes, err := json.Marshal(mockDB)
	require.NoError(t, err)
	tblBytes, err := json.Marshal(mockTbl)
	require.NoError(t, err)
	statsBytes, err := json.Marshal(mockStats)
	require.NoError(t, err)

	mockSchemas := []*backuppb.Schema{
		{
			Db:    dbBytes,
			Table: tblBytes,
			Stats: statsBytes,
		},
	}

	mockFiles := []*backuppb.File{
		// should include 1.sst - 3.sst
		{
			Name:     "1.sst",
			StartKey: tablecodec.EncodeRowKey(partID1, []byte("a")),
			EndKey:   tablecodec.EncodeRowKey(partID1, []byte("b")),
		},
		{
			Name:     "2.sst",
			StartKey: tablecodec.EncodeRowKey(partID1, []byte("b")),
			EndKey:   tablecodec.EncodeRowKey(partID2, []byte("a")),
		},
		{
			Name:     "3.sst",
			StartKey: tablecodec.EncodeRowKey(partID2, []byte("a")),
			EndKey:   tablecodec.EncodeRowKey(partID2+1, []byte("b")),
		},
		// shouldn't include 4.sst
		{
			Name:     "4.sst",
			StartKey: tablecodec.EncodeRowKey(tblID-1, []byte("a")),
			EndKey:   tablecodec.EncodeRowKey(tblID, []byte("a")),
		},
	}

	meta := mockBackupMeta(mockSchemas, mockFiles)

	data, err := proto.Marshal(meta)
	require.NoError(t, err)

	ctx := context.Background()
	err = store.WriteFile(ctx, metautil.MetaFile, data)
	require.NoError(t, err)

	dbs, err := LoadBackupTables(
		ctx,
		metautil.NewMetaReader(
			meta,
			store,
			&backuppb.CipherInfo{
				CipherType: encryptionpb.EncryptionMethod_PLAINTEXT,
			},
		),
	)
	tbl := dbs[dbName.String()].GetTable(tblName.String())
	require.NoError(t, err)
	require.Len(t, tbl.Files, 3)
	contains := func(name string) bool {
		for i := range tbl.Files {
			if tbl.Files[i].Name == name {
				return true
			}
		}
		return false
	}
	require.True(t, contains("1.sst"))
	require.True(t, contains("2.sst"))
	require.True(t, contains("3.sst"))
}

func buildTableAndFiles(name string, tableID, fileCount int) (*model.TableInfo, []*backuppb.File) {
	tblName := model.NewCIStr(name)
	tblID := int64(tableID)
	mockTbl := &model.TableInfo{
		ID:   tblID,
		Name: tblName,
	}

	mockFiles := make([]*backuppb.File, 0, fileCount)
	for i := 0; i < fileCount; i++ {
		mockFiles = append(mockFiles, &backuppb.File{
			Name:     fmt.Sprintf("%d-%d.sst", tableID, i),
			StartKey: tablecodec.EncodeRowKey(tblID, []byte(fmt.Sprintf("%09d", i))),
			EndKey:   tablecodec.EncodeRowKey(tblID, []byte(fmt.Sprintf("%09d", i+1))),
		})
	}
	return mockTbl, mockFiles
}

func buildBenchmarkBackupmeta(b *testing.B, dbName string, tableCount, fileCountPerTable int) *backuppb.BackupMeta {
	mockFiles := make([]*backuppb.File, 0, tableCount*fileCountPerTable)
	mockSchemas := make([]*backuppb.Schema, 0, tableCount)
	for i := 1; i <= tableCount; i++ {
		mockTbl, files := buildTableAndFiles(fmt.Sprintf("mock%d", i), i, fileCountPerTable)
		mockFiles = append(mockFiles, files...)

		mockDB := model.DBInfo{
			ID:   1,
			Name: model.NewCIStr(dbName),
			Tables: []*model.TableInfo{
				mockTbl,
			},
		}
		dbBytes, err := json.Marshal(mockDB)
		require.NoError(b, err)
		tblBytes, err := json.Marshal(mockTbl)
		require.NoError(b, err)
		mockSchemas = append(mockSchemas, &backuppb.Schema{
			Db:    dbBytes,
			Table: tblBytes,
		})
	}
	return mockBackupMeta(mockSchemas, mockFiles)
}

func BenchmarkLoadBackupMeta64(b *testing.B) {
	testDir := b.TempDir()
	store, err := storage.NewLocalStorage(testDir)
	require.NoError(b, err)

	meta := buildBenchmarkBackupmeta(b, "bench", 64, 64)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, err := proto.Marshal(meta)
		require.NoError(b, err)

		ctx := context.Background()
		err = store.WriteFile(ctx, metautil.MetaFile, data)
		require.NoError(b, err)

		dbs, err := LoadBackupTables(
			ctx,
			metautil.NewMetaReader(
				meta,
				store,
				&backuppb.CipherInfo{
					CipherType: encryptionpb.EncryptionMethod_PLAINTEXT,
				},
			),
		)
		require.NoError(b, err)
		require.Len(b, dbs, 1)
		require.Contains(b, dbs, "bench")
		require.Len(b, dbs["bench"].Tables, 64)
	}
}

func BenchmarkLoadBackupMeta1024(b *testing.B) {
	testDir := b.TempDir()
	store, err := storage.NewLocalStorage(testDir)
	require.NoError(b, err)

	meta := buildBenchmarkBackupmeta(b, "bench", 1024, 64)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, err := proto.Marshal(meta)
		require.NoError(b, err)

		ctx := context.Background()
		err = store.WriteFile(ctx, metautil.MetaFile, data)
		require.NoError(b, err)

		dbs, err := LoadBackupTables(
			ctx,
			metautil.NewMetaReader(
				meta,
				store,
				&backuppb.CipherInfo{
					CipherType: encryptionpb.EncryptionMethod_PLAINTEXT,
				},
			),
		)
		require.NoError(b, err)
		require.Len(b, dbs, 1)
		require.Contains(b, dbs, "bench")
		require.Len(b, dbs["bench"].Tables, 1024)
	}
}

func BenchmarkLoadBackupMeta10240(b *testing.B) {
	testDir := b.TempDir()
	store, err := storage.NewLocalStorage(testDir)
	require.NoError(b, err)

	meta := buildBenchmarkBackupmeta(b, "bench", 10240, 64)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, err := proto.Marshal(meta)
		require.NoError(b, err)

		ctx := context.Background()
		err = store.WriteFile(ctx, metautil.MetaFile, data)
		require.NoError(b, err)

		dbs, err := LoadBackupTables(
			ctx,
			metautil.NewMetaReader(
				meta,
				store,
				&backuppb.CipherInfo{
					CipherType: encryptionpb.EncryptionMethod_PLAINTEXT,
				},
			),
		)
		require.NoError(b, err)
		require.Len(b, dbs, 1)
		require.Contains(b, dbs, "bench")
		require.Len(b, dbs["bench"].Tables, 10240)
	}
}
