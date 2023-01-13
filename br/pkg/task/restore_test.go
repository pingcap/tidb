// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/golang/protobuf/proto"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/conn"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	"google.golang.org/grpc/keepalive"
)

func TestRestoreConfigAdjust(t *testing.T) {
	cfg := &RestoreConfig{}
	cfg.Adjust()

	require.Equal(t, uint32(defaultRestoreConcurrency), cfg.Config.Concurrency)
	require.Equal(t, defaultSwitchInterval, cfg.Config.SwitchModeInterval)
	require.Equal(t, conn.DefaultMergeRegionKeyCount, cfg.MergeSmallRegionKeyCount)
	require.Equal(t, conn.DefaultMergeRegionSizeBytes, cfg.MergeSmallRegionSizeBytes)
}

type mockPDClient struct {
	pd.Client
}

func (m mockPDClient) GetAllStores(ctx context.Context, opts ...pd.GetStoreOption) ([]*metapb.Store, error) {
	return []*metapb.Store{}, nil
}

func TestConfigureRestoreClient(t *testing.T) {
	cfg := Config{
		Concurrency: 1024,
	}
	restoreComCfg := RestoreCommonConfig{
		Online: true,
	}
	restoreCfg := &RestoreConfig{
		Config:              cfg,
		RestoreCommonConfig: restoreComCfg,
		DdlBatchSize:        128,
	}
	client := restore.NewRestoreClient(mockPDClient{}, nil, keepalive.ClientParameters{}, false)
	ctx := context.Background()
	err := configureRestoreClient(ctx, client, restoreCfg)
	require.NoError(t, err)
	require.Equal(t, uint(128), client.GetBatchDdlSize())
	require.True(t, client.IsOnline())
}

func TestAdjustRestoreConfigForStreamRestore(t *testing.T) {
	restoreCfg := RestoreConfig{}

	restoreCfg.adjustRestoreConfigForStreamRestore()
	require.Equal(t, restoreCfg.PitrBatchCount, uint32(defaultPiTRBatchCount))
	require.Equal(t, restoreCfg.PitrBatchSize, uint32(defaultPiTRBatchSize))
	require.Equal(t, restoreCfg.PitrConcurrency, uint32(defaultPiTRConcurrency))
	require.Equal(t, restoreCfg.Concurrency, restoreCfg.PitrConcurrency)
}

func TestCheckRestoreDBAndTable(t *testing.T) {
	cases := []struct {
		cfgSchemas map[string]struct{}
		cfgTables  map[string]struct{}
		backupDBs  map[string]*utils.Database
	}{
		{
			cfgSchemas: map[string]struct{}{
				utils.EncloseName("test"): {},
			},
			cfgTables: map[string]struct{}{
				utils.EncloseDBAndTable("test", "t"):  {},
				utils.EncloseDBAndTable("test", "t2"): {},
			},
			backupDBs: mockReadSchemasFromBackupMeta(t, map[string][]string{
				"test": {"T", "T2"},
			}),
		},
		{
			cfgSchemas: map[string]struct{}{
				utils.EncloseName("mysql"): {},
			},
			cfgTables: map[string]struct{}{
				utils.EncloseDBAndTable("mysql", "t"):  {},
				utils.EncloseDBAndTable("mysql", "t2"): {},
			},
			backupDBs: mockReadSchemasFromBackupMeta(t, map[string][]string{
				"__TiDB_BR_Temporary_mysql": {"T", "T2"},
			}),
		},
		{
			cfgSchemas: map[string]struct{}{
				utils.EncloseName("test"): {},
			},
			cfgTables: map[string]struct{}{
				utils.EncloseDBAndTable("test", "T"):  {},
				utils.EncloseDBAndTable("test", "T2"): {},
			},
			backupDBs: mockReadSchemasFromBackupMeta(t, map[string][]string{
				"test": {"t", "t2"},
			}),
		},
		{
			cfgSchemas: map[string]struct{}{
				utils.EncloseName("TEST"): {},
			},
			cfgTables: map[string]struct{}{
				utils.EncloseDBAndTable("TEST", "t"):  {},
				utils.EncloseDBAndTable("TEST", "T2"): {},
			},
			backupDBs: mockReadSchemasFromBackupMeta(t, map[string][]string{
				"test": {"t", "t2"},
			}),
		},
		{
			cfgSchemas: map[string]struct{}{
				utils.EncloseName("TeSt"): {},
			},
			cfgTables: map[string]struct{}{
				utils.EncloseDBAndTable("TeSt", "tabLe"):  {},
				utils.EncloseDBAndTable("TeSt", "taBle2"): {},
			},
			backupDBs: mockReadSchemasFromBackupMeta(t, map[string][]string{
				"TesT": {"TablE", "taBle2"},
			}),
		},
		{
			cfgSchemas: map[string]struct{}{
				utils.EncloseName("TeSt"):  {},
				utils.EncloseName("MYSQL"): {},
			},
			cfgTables: map[string]struct{}{
				utils.EncloseDBAndTable("TeSt", "tabLe"):  {},
				utils.EncloseDBAndTable("TeSt", "taBle2"): {},
				utils.EncloseDBAndTable("MYSQL", "taBle"): {},
			},
			backupDBs: mockReadSchemasFromBackupMeta(t, map[string][]string{
				"TesT":                      {"table", "TaBLE2"},
				"__TiDB_BR_Temporary_mysql": {"tablE"},
			}),
		},
	}

	cfg := &RestoreConfig{}
	for _, ca := range cases {
		cfg.Schemas = ca.cfgSchemas
		cfg.Tables = ca.cfgTables
		client := restore.MockClient(ca.backupDBs)

		err := CheckRestoreDBAndTable(client, cfg)
		require.NoError(t, err)
	}
}

func mockReadSchemasFromBackupMeta(t *testing.T, db2Tables map[string][]string) map[string]*utils.Database {
	testDir := t.TempDir()
	store, err := storage.NewLocalStorage(testDir)
	require.NoError(t, err)

	mockSchemas := make([]*backuppb.Schema, 0)
	var dbID int64 = 1
	for db, tables := range db2Tables {
		dbName := model.NewCIStr(db)
		mockTblList := make([]*model.TableInfo, 0)
		tblBytesList, statsBytesList := make([][]byte, 0), make([][]byte, 0)

		for i, table := range tables {
			tblName := model.NewCIStr(table)
			mockTbl := &model.TableInfo{
				ID:   dbID*100 + int64(i),
				Name: tblName,
			}
			mockTblList = append(mockTblList, mockTbl)

			mockStats := handle.JSONTable{
				DatabaseName: dbName.String(),
				TableName:    tblName.String(),
			}

			tblBytes, err := json.Marshal(mockTbl)
			require.NoError(t, err)
			tblBytesList = append(tblBytesList, tblBytes)

			statsBytes, err := json.Marshal(mockStats)
			require.NoError(t, err)
			statsBytesList = append(statsBytesList, statsBytes)
		}

		mockDB := model.DBInfo{
			ID:     dbID,
			Name:   dbName,
			Tables: mockTblList,
		}
		dbID++
		dbBytes, err := json.Marshal(mockDB)
		require.NoError(t, err)

		for i := 0; i < len(tblBytesList); i++ {
			mockSchemas = append(mockSchemas, &backuppb.Schema{
				Db:    dbBytes,
				Table: tblBytesList[i],
				Stats: statsBytesList[i],
			},
			)
		}
	}

	mockFiles := []*backuppb.File{
		{
			Name:     fmt.Sprintf("%p.sst", &mockSchemas),
			StartKey: tablecodec.EncodeRowKey(1, []byte("a")),
			EndKey:   tablecodec.EncodeRowKey(2, []byte("a")),
		},
	}

	meta := mockBackupMeta(mockSchemas, mockFiles)
	data, err := proto.Marshal(meta)
	require.NoError(t, err)

	ctx := context.Background()
	err = store.WriteFile(ctx, metautil.MetaFile, data)
	require.NoError(t, err)

	dbs, err := utils.LoadBackupTables(
		ctx,
		metautil.NewMetaReader(
			meta,
			store,
			&backuppb.CipherInfo{
				CipherType: encryptionpb.EncryptionMethod_PLAINTEXT,
			}),
	)
	require.NoError(t, err)
	return dbs
}

func mockBackupMeta(mockSchemas []*backuppb.Schema, mockFiles []*backuppb.File) *backuppb.BackupMeta {
	return &backuppb.BackupMeta{
		Files:   mockFiles,
		Schemas: mockSchemas,
	}
}
