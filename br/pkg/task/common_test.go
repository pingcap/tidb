// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"context"
	"fmt"
	"testing"

	backup "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	kvconfig "github.com/pingcap/tidb/br/pkg/config"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

// RebuildRecordedMaterializedViewsForTest rebuilds the final recorder definitions.
var RebuildRecordedMaterializedViewsForTest = repairMaterializedViews

// PrepareMaterializedViewTablesForTest returns the ordinary snapshot schemas.
func PrepareMaterializedViewTablesForTest(tables []*metautil.Table) ([]*metautil.Table, error) {
	_, prepared, err := newMaterializedViewRestorePlan(tables, false)
	return prepared, err
}

// RestoreMaterializedViewsForTest exercises snapshot restore against existing tables.
func RestoreMaterializedViewsForTest(ctx context.Context, g glue.Glue, dom *domain.Domain, tables []*metautil.Table) error {
	return RestoreMaterializedViewsWithConfigForTest(ctx, g, dom, tables, &RestoreConfig{})
}

// RestoreMaterializedViewsWithConfigForTest exercises snapshot rebuild with restore options.
func RestoreMaterializedViewsWithConfigForTest(
	ctx context.Context, g glue.Glue, dom *domain.Domain, tables []*metautil.Table, cfg *RestoreConfig,
) error {
	plan, _, err := newMaterializedViewRestorePlan(tables, false)
	if err != nil {
		return err
	}
	return plan.restore(ctx, g, dom, cfg)
}

func TestSnapshotMaterializedViewsUseLogicalRebuild(t *testing.T) {
	db := &model.DBInfo{Name: pmodel.NewCIStr("test")}
	tables := []*metautil.Table{
		{DB: db, Info: &model.TableInfo{ID: 1, State: model.StatePublic, MaterializedViewBase: &model.MaterializedViewBaseInfo{MLogID: 2, MViewIDs: []int64{3}}}},
		{DB: db, Info: &model.TableInfo{ID: 2, State: model.StatePublic, MaterializedViewLog: &model.MaterializedViewLogInfo{BaseTableID: 1}}},
		{DB: db, Info: &model.TableInfo{ID: 3, State: model.StatePublic, MaterializedView: &model.MaterializedViewInfo{BaseTableIDs: []int64{1}}}},
	}
	for i, name := range []string{"tidb_mview_refresh_info", "tidb_mlog_purge_info", "tidb_mlog_purge_hist"} {
		tables = append(tables, &metautil.Table{DB: &model.DBInfo{Name: utils.TemporaryDBName("mysql")}, Info: &model.TableInfo{ID: int64(30 + i), Name: pmodel.NewCIStr(name)}})
	}
	plan, prepared, err := newMaterializedViewRestorePlan(tables, false)
	require.NoError(t, err)
	require.Len(t, prepared, 4, "even a full snapshot with progress tables must rebuild derived objects logically")
	require.False(t, hasMaterializedViewDefinition(prepared[0].Info))
	require.Len(t, plan.objects, 2)
}

func TestMaterializedViewRestorePlan(t *testing.T) {
	db := &model.DBInfo{Name: pmodel.NewCIStr("test")}
	base := &metautil.Table{DB: db, Info: &model.TableInfo{ID: 11, State: model.StatePublic, MaterializedViewBase: &model.MaterializedViewBaseInfo{MLogID: 12, MViewIDs: []int64{13, 14}}}}
	mlog := &metautil.Table{DB: db, Info: &model.TableInfo{ID: 12, State: model.StatePublic, MaterializedViewLog: &model.MaterializedViewLogInfo{BaseTableID: 11, DependentMViewIDs: []int64{13, 14}}}}
	mv := &metautil.Table{DB: db, Info: &model.TableInfo{ID: 13, State: model.StatePublic, MaterializedView: &model.MaterializedViewInfo{BaseTableIDs: []int64{11}}}}
	tables := []*metautil.Table{mv, mlog, base}
	ids := func(tables []*metautil.Table) []int64 {
		var result []int64
		for _, table := range tables {
			result = append(result, table.Info.ID)
		}
		return result
	}
	for _, pitr := range []bool{false, true} {
		plan, prepared, err := newMaterializedViewRestorePlan(tables, pitr)
		require.NoError(t, err)
		require.Equal(t, []int64{11}, ids(prepared))
		require.False(t, hasMaterializedViewDefinition(prepared[0].Info))
		if pitr {
			require.Empty(t, plan.objects)
		} else {
			require.Equal(t, []int64{12, 13}, ids(plan.objects))
		}
		require.Equal(t, []int64{13, 14}, base.Info.MaterializedViewBase.MViewIDs, "source remains immutable")
	}
	t.Run("PITR defers dependency validation until after replay", func(t *testing.T) {
		view := *mv
		view.Info = mv.Info.Clone()
		view.Info.MaterializedView.BaseTableIDs = nil
		plan, prepared, err := newMaterializedViewRestorePlan([]*metautil.Table{&view, mlog, base}, true)
		require.NoError(t, err)
		require.Empty(t, plan.objects)
		require.Equal(t, []int64{11}, ids(prepared))
		require.False(t, hasMaterializedViewDefinition(prepared[0].Info))
		require.Equal(t, []int64{13, 14}, base.Info.MaterializedViewBase.MViewIDs, "source remains immutable")
	})
	t.Run("validate downstream references", func(t *testing.T) {
		for _, invalid := range []string{"missing", "ordinary", "different base", "no base", "multiple bases", "missing reverse reference"} {
			t.Run(invalid, func(t *testing.T) {
				var input []*metautil.Table
				for _, table := range tables {
					clone := *table
					clone.Info = table.Info.Clone()
					input = append(input, &clone)
				}
				if invalid != "missing" {
					view := &metautil.Table{DB: db, Info: &model.TableInfo{ID: 14, State: model.StatePublic}}
					if invalid != "ordinary" {
						view.Info.MaterializedView = &model.MaterializedViewInfo{}
					}
					switch invalid {
					case "different base":
						view.Info.MaterializedView.BaseTableIDs = []int64{21}
						input = append(input,
							&metautil.Table{DB: db, Info: &model.TableInfo{ID: 21, State: model.StatePublic, MaterializedViewBase: &model.MaterializedViewBaseInfo{MLogID: 22, MViewIDs: []int64{14}}}},
							&metautil.Table{DB: db, Info: &model.TableInfo{ID: 22, State: model.StatePublic, MaterializedViewLog: &model.MaterializedViewLogInfo{BaseTableID: 21, DependentMViewIDs: []int64{14}}}})
					case "multiple bases":
						view.Info.MaterializedView.BaseTableIDs = []int64{11, 21}
					case "missing reverse reference":
						view.Info.MaterializedView.BaseTableIDs = []int64{11}
						input[2].Info.MaterializedViewBase.MViewIDs = []int64{13}
					}
					input = append(input, view)
				}
				plan, prepared, err := newMaterializedViewRestorePlan(input, false)
				if invalid == "no base" || invalid == "multiple bases" {
					require.ErrorContains(t, err, "must have exactly one base table")
					return
				}
				require.NoError(t, err)
				expectedObjects, expectedTables := []int64{12, 13}, []int64{11}
				if invalid == "different base" {
					expectedObjects = append(expectedObjects, 22, 14)
					expectedTables = append(expectedTables, 21)
				}
				require.Equal(t, expectedObjects, ids(plan.objects))
				require.Equal(t, expectedTables, ids(prepared))
				for _, table := range prepared {
					require.False(t, hasMaterializedViewDefinition(table.Info))
				}
				require.Equal(t, []int64{13, 14}, base.Info.MaterializedViewBase.MViewIDs, "source remains immutable")
			})
		}
	})
	for _, input := range [][]*metautil.Table{{base}, {base, mv}, {mlog}, {mv}} {
		plan, prepared, err := newMaterializedViewRestorePlan(input, false)
		require.NoError(t, err)
		require.Empty(t, plan.objects)
		for _, table := range prepared {
			require.Equal(t, int64(11), table.Info.ID)
			require.False(t, hasMaterializedViewDefinition(table.Info))
		}
	}
	for _, state := range []model.SchemaState{model.StateNone, model.StateWriteOnly, model.StateDeleteOnly} {
		for _, invalidID := range []int64{11, 12, 13} {
			var input []*metautil.Table
			for _, table := range tables {
				clone := *table
				clone.Info = table.Info.Clone()
				if clone.Info.ID == invalidID {
					clone.Info.State = state
				}
				input = append(input, &clone)
			}
			plan, prepared, err := newMaterializedViewRestorePlan(input, false)
			require.NoError(t, err)
			if invalidID == 13 {
				require.Equal(t, []int64{12}, ids(plan.objects))
			} else {
				require.Empty(t, plan.objects)
			}
			require.Equal(t, []int64{11}, ids(prepared))
			require.False(t, hasMaterializedViewDefinition(prepared[0].Info))
			require.Equal(t, model.StatePublic, prepared[0].Info.State)
		}
	}
	parent := *mv
	parent.Info = mv.Info.Clone()
	parent.Info.MaterializedViewBase = &model.MaterializedViewBaseInfo{MLogID: 15, MViewIDs: []int64{16}}
	childLog := &metautil.Table{DB: db, Info: &model.TableInfo{ID: 15, State: model.StatePublic, MaterializedViewLog: &model.MaterializedViewLogInfo{BaseTableID: 13}}}
	child := &metautil.Table{DB: db, Info: &model.TableInfo{ID: 16, State: model.StatePublic, MaterializedView: &model.MaterializedViewInfo{BaseTableIDs: []int64{13}}}}
	for _, state := range []model.MViewInitBuildState{model.MViewInitBuildDeferred, model.MViewInitBuildBuilding} {
		building := parent
		building.Info = parent.Info.Clone()
		building.Info.MaterializedView.InitBuildState = state
		plan, prepared, err := newMaterializedViewRestorePlan([]*metautil.Table{child, childLog, &building, mlog, base}, false)
		require.NoError(t, err)
		require.Equal(t, []int64{12}, ids(plan.objects))
		require.Equal(t, []int64{11}, ids(prepared))
	}
	// Ready cascades are rebuilt in dependency order, importing only the base.
	input := []*metautil.Table{child, childLog, &parent, mlog, base}
	plan, prepared, err := newMaterializedViewRestorePlan(input, false)
	require.NoError(t, err)
	require.Equal(t, []int64{11}, ids(prepared))
	require.Equal(t, []int64{12, 13, 15, 16}, ids(plan.objects))
	// Dependency order follows relationships rather than IDs or backup order.
	childWithLowerID := *child
	childWithLowerID.Info = child.Info.Clone()
	childWithLowerID.Info.ID = 1
	parentWithLowerChildID := parent
	parentWithLowerChildID.Info = parent.Info.Clone()
	parentWithLowerChildID.Info.MaterializedViewBase.MViewIDs = []int64{1}
	plan, _, err = newMaterializedViewRestorePlan([]*metautil.Table{&childWithLowerID, childLog, &parentWithLowerChildID, mlog, base}, false)
	require.NoError(t, err)
	require.Equal(t, []int64{12, 13, 15, 1}, ids(plan.objects))
	plan, prepared, err = newMaterializedViewRestorePlan([]*metautil.Table{&parent, mlog, base}, false)
	require.NoError(t, err)
	require.Equal(t, []int64{12, 13}, ids(plan.objects), "a missing downstream log does not invalidate its ready parent MV")
	require.Equal(t, []int64{11}, ids(prepared))
	for _, invalid := range []struct {
		name    string
		tableID int64
		objects []int64
	}{
		{name: "plain base", tableID: 11},
		{name: "plain root log", tableID: 12},
		{name: "plain parent MV", tableID: 13, objects: []int64{12}},
		{name: "plain downstream log", tableID: 15, objects: []int64{12, 13}},
		{name: "plain leaf MV", tableID: 16, objects: []int64{12, 13, 15}},
	} {
		t.Run(invalid.name, func(t *testing.T) {
			var input []*metautil.Table
			for _, table := range []*metautil.Table{child, childLog, &parent, mlog, base} {
				clone := *table
				clone.Info = table.Info.Clone()
				if clone.Info.ID == invalid.tableID {
					clone.Info.MaterializedView = nil
					clone.Info.MaterializedViewLog = nil
					if invalid.tableID == 11 {
						clone.Info.MaterializedViewBase = nil
					}
				}
				// A log's reverse links alone must also identify a plain former MV.
				if invalid.tableID == 13 && clone.Info.ID == 11 {
					clone.Info.MaterializedViewBase.MViewIDs = nil
				}
				input = append(input, &clone)
			}
			plan, prepared, err := newMaterializedViewRestorePlan(input, false)
			require.NoError(t, err)
			require.Equal(t, invalid.objects, ids(plan.objects))
			require.Equal(t, []int64{11}, ids(prepared))
		})
	}
}

type fakeValue string

func (f fakeValue) String() string {
	return string(f)
}

func (f fakeValue) Set(string) error {
	panic("implement me")
}

func (f fakeValue) Type() string {
	panic("implement me")
}

type fakeRestoreFileClient struct {
	dbs []*metautil.Database
}

func (c fakeRestoreFileClient) GetDatabases() []*metautil.Database {
	return c.dbs
}

func TestUrlNoQuery(t *testing.T) {
	testCases := []struct {
		inputName     string
		expectedName  string
		inputValue    string
		expectedValue string
	}{
		{
			inputName:     flagSendCreds,
			expectedName:  "send-credentials-to-tikv",
			inputValue:    "true",
			expectedValue: "true",
		},
		{
			inputName:     flagStorage,
			expectedName:  "storage",
			inputValue:    "s3://some/what?secret=a123456789&key=987654321",
			expectedValue: "s3://some/what",
		},
		{
			inputName:     FlagStreamFullBackupStorage,
			expectedName:  "full-backup-storage",
			inputValue:    "s3://bucket/prefix/?access-key=1&secret-key=2",
			expectedValue: "s3://bucket/prefix/",
		},
		{
			inputName:     flagFullBackupCipherKey,
			expectedName:  "crypter.key",
			inputValue:    "537570657253656372657456616C7565",
			expectedValue: "<redacted>",
		},
		{
			inputName:     flagLogBackupCipherKey,
			expectedName:  "log.crypter.key",
			inputValue:    "537570657253656372657456616C7565",
			expectedValue: "<redacted>",
		},
		{
			inputName:     "azblob.encryption-key",
			expectedName:  "azblob.encryption-key",
			inputValue:    "SUPERSECRET_AZURE_ENCRYPTION_KEY",
			expectedValue: "<redacted>",
		},
		{
			inputName:     flagMasterKeyConfig,
			expectedName:  "master-key",
			inputValue:    "local:///path/abcd,aws-kms:///abcd?AWS_ACCESS_KEY_ID=SECRET1&AWS_SECRET_ACCESS_KEY=SECRET2&REGION=us-east-1,azure-kms:///abcd/v1?AZURE_TENANT_ID=tenant-id&AZURE_CLIENT_ID=client-id&AZURE_CLIENT_SECRET=client-secret&AZURE_VAULT_NAME=vault-name",
			expectedValue: "<redacted>",
			// expectedValue: "local:///path/abcd,aws-kms:///abcd,azure-kms:///abcd/v1"
		},
	}

	for _, tc := range testCases {
		flag := pflag.Flag{
			Name:  tc.inputName,
			Value: fakeValue(tc.inputValue),
		}
		field := flagToZapField(&flag)
		require.Equal(t, tc.expectedName, field.Key, `test-case [%s="%s"]`, tc.expectedName, tc.expectedValue)
		if stringer, ok := field.Interface.(fmt.Stringer); ok {
			field.String = stringer.String()
		}
		require.Equal(t, tc.expectedValue, field.String, `test-case [%s="%s"]`, tc.expectedName, tc.expectedValue)
	}
}

func TestFilterRestoreFilesKeepsMaterializedObjects(t *testing.T) {
	dbInfo := &model.DBInfo{ID: 1, Name: pmodel.NewCIStr("test")}
	baseTable := &metautil.Table{
		DB: dbInfo,
		Info: &model.TableInfo{
			ID:    11,
			Name:  pmodel.NewCIStr("base_table"),
			State: model.StatePublic,
			MaterializedViewBase: &model.MaterializedViewBaseInfo{
				MLogID:   12,
				MViewIDs: []int64{13},
			},
		},
		Files: []*backup.File{{Name: "base_table_write.sst"}},
	}
	database := &metautil.Database{
		Info: dbInfo,
		Tables: []*metautil.Table{
			baseTable,
			{
				DB:    dbInfo,
				Info:  &model.TableInfo{ID: 12, State: model.StatePublic, Name: pmodel.NewCIStr("$mlog$base_table"), MaterializedViewLog: &model.MaterializedViewLogInfo{BaseTableID: 11}},
				Files: []*backup.File{{Name: "mlog_write.sst"}},
			},
			{
				DB:    dbInfo,
				Info:  &model.TableInfo{ID: 13, State: model.StatePublic, Name: pmodel.NewCIStr("mv_table"), MaterializedView: &model.MaterializedViewInfo{BaseTableIDs: []int64{11}}},
				Files: []*backup.File{{Name: "mv_write.sst"}},
			},
			{
				DB:    dbInfo,
				Info:  &model.TableInfo{ID: 14, Name: pmodel.NewCIStr("_mv_shadow"), MaterializedViewShadow: &model.MaterializedViewShadowInfo{SourceMViewID: 13}},
				Files: []*backup.File{{Name: "shadow_write.sst"}},
			},
		},
	}
	cfg := &RestoreConfig{
		Config: Config{
			TableFilter: filter.CaseInsensitive(must(filter.Parse([]string{"*.*"}))),
		},
	}

	files, tables, dbs := filterRestoreFiles(fakeRestoreFileClient{dbs: []*metautil.Database{database}}, cfg)

	require.Len(t, dbs, 1)
	require.Len(t, tables, 3)
	require.Same(t, baseTable, tables[0])
	require.NotNil(t, baseTable.Info.MaterializedViewBase)
	require.Equal(t, []*backup.File{{Name: "base_table_write.sst"}, {Name: "mlog_write.sst"}, {Name: "mv_write.sst"}}, files)

	for _, state := range []model.SchemaState{model.StateNone, model.StateWriteOnly, model.StateDeleteOnly} {
		t.Run("dropping objects "+state.String(), func(t *testing.T) {
			database.Tables[1].Info.State = state
			database.Tables[2].Info.State = state
			files, tables, _ := filterRestoreFiles(fakeRestoreFileClient{dbs: []*metautil.Database{database}}, cfg)
			require.Equal(t, database.Tables[:3], tables)
			require.Equal(t, []*backup.File{{Name: "base_table_write.sst"}, {Name: "mlog_write.sst"}, {Name: "mv_write.sst"}}, files)
			plan, prepared, err := newMaterializedViewRestorePlan(tables, false)
			require.NoError(t, err)
			require.Empty(t, plan.objects)
			require.Len(t, prepared, 1)
			require.Nil(t, prepared[0].Info.MaterializedViewBase)
			require.Equal(t, baseTable.Info.ID, prepared[0].Info.ID)
			require.Equal(t, []*backup.File{{Name: "base_table_write.sst"}}, prepared[0].Files)
			require.Equal(t, int64(12), baseTable.Info.MaterializedViewBase.MLogID)
		})
	}
}

func TestTiDBConfigUnchanged(t *testing.T) {
	cfg := config.GetGlobalConfig()
	restoreConfig := tweakLocalConfForRestore()
	require.NotEqual(t, config.GetGlobalConfig(), cfg)
	restoreConfig()
	require.Equal(t, config.GetGlobalConfig(), cfg)
}

func TestStripingPDURL(t *testing.T) {
	nor1, err := normalizePDURL("https://pd:5432", true)
	require.NoError(t, err)
	require.Equal(t, "pd:5432", nor1)
	_, err = normalizePDURL("https://pd.pingcap.com", false)
	require.Error(t, err)
	require.Regexp(t, ".*pd url starts with https while TLS disabled.*", err.Error())
	_, err = normalizePDURL("http://127.0.0.1:2379", true)
	require.Error(t, err)
	require.Regexp(t, ".*pd url starts with http while TLS enabled.*", err.Error())
	nor, err := normalizePDURL("http://127.0.0.1", false)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1", nor)
	noChange, err := normalizePDURL("127.0.0.1:2379", false)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1:2379", noChange)
}

func TestCheckCipherKeyMatch(t *testing.T) {
	cases := []struct {
		name       string
		cipherInfo *backup.CipherInfo
		expectErr  bool
		errMsg     string
	}{
		{
			name: "PLAINTEXT",
			cipherInfo: &backup.CipherInfo{
				CipherType: encryptionpb.EncryptionMethod_PLAINTEXT,
			},
			expectErr: false,
		},
		{
			name: "UNKNOWN",
			cipherInfo: &backup.CipherInfo{
				CipherType: encryptionpb.EncryptionMethod_UNKNOWN,
			},
			expectErr: true,
			errMsg:    "Unknown encryption method: UNKNOWN",
		},
		{
			name: "AES128_CTR valid",
			cipherInfo: &backup.CipherInfo{
				CipherType: encryptionpb.EncryptionMethod_AES128_CTR,
				CipherKey:  make([]byte, crypterAES128KeyLen),
			},
			expectErr: false,
		},
		{
			name: "AES128_CTR invalid length",
			cipherInfo: &backup.CipherInfo{
				CipherType: encryptionpb.EncryptionMethod_AES128_CTR,
				CipherKey:  make([]byte, crypterAES128KeyLen-1),
			},
			expectErr: true,
			errMsg:    fmt.Sprintf("AES-128 key length mismatch: expected %d, got %d", crypterAES128KeyLen, crypterAES128KeyLen-1),
		},
		{
			name: "AES192_CTR valid",
			cipherInfo: &backup.CipherInfo{
				CipherType: encryptionpb.EncryptionMethod_AES192_CTR,
				CipherKey:  make([]byte, crypterAES192KeyLen),
			},
			expectErr: false,
		},
		{
			name: "AES192_CTR invalid length",
			cipherInfo: &backup.CipherInfo{
				CipherType: encryptionpb.EncryptionMethod_AES192_CTR,
				CipherKey:  make([]byte, crypterAES192KeyLen+1),
			},
			expectErr: true,
			errMsg:    fmt.Sprintf("AES-192 key length mismatch: expected %d, got %d", crypterAES192KeyLen, crypterAES192KeyLen+1),
		},
		{
			name: "AES256_CTR valid",
			cipherInfo: &backup.CipherInfo{
				CipherType: encryptionpb.EncryptionMethod_AES256_CTR,
				CipherKey:  make([]byte, crypterAES256KeyLen),
			},
			expectErr: false,
		},
		{
			name: "AES256_CTR invalid length",
			cipherInfo: &backup.CipherInfo{
				CipherType: encryptionpb.EncryptionMethod_AES256_CTR,
				CipherKey:  make([]byte, 0),
			},
			expectErr: true,
			errMsg:    fmt.Sprintf("AES-256 key length mismatch: expected %d, got %d", crypterAES256KeyLen, 0),
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := checkCipherKeyMatch(c.cipherInfo)
			if c.expectErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), c.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCheckCipherKey(t *testing.T) {
	cases := []struct {
		cipherKey string
		keyFile   string
		ok        bool
	}{
		{
			cipherKey: "0123456789abcdef0123456789abcdef",
			keyFile:   "",
			ok:        true,
		},
		{
			cipherKey: "0123456789abcdef0123456789abcdef",
			keyFile:   "/tmp/abc",
			ok:        false,
		},
		{
			cipherKey: "",
			keyFile:   "/tmp/abc",
			ok:        true,
		},
		{
			cipherKey: "",
			keyFile:   "",
			ok:        false,
		},
	}

	for _, c := range cases {
		err := checkCipherKey(c.cipherKey, c.keyFile)
		if c.ok {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
		}
	}
}

func TestGetCipherKey(t *testing.T) {
	nonHexKey := "this is not a hex string"
	_, err := GetCipherKeyContent(nonHexKey, "")
	require.Error(t, err)
	require.Contains(t, err.Error(), cipherKeyNonHexErrorMsg)
}

func must[T any](t T, err error) T {
	if err != nil {
		panic(err)
	}
	return t
}

func expectedDefaultConfig() Config {
	return Config{
		BackendOptions:            storage.BackendOptions{S3: storage.S3BackendOptions{ForcePathStyle: true}},
		PD:                        []string{"127.0.0.1:2379"},
		ChecksumConcurrency:       4,
		Checksum:                  true,
		SendCreds:                 true,
		CheckRequirements:         true,
		FilterStr:                 []string(nil),
		TableFilter:               filter.CaseInsensitive(must(filter.Parse([]string{"*.*"}))),
		Schemas:                   map[string]struct{}{},
		Tables:                    map[string]struct{}{},
		SwitchModeInterval:        300000000000,
		GRPCKeepaliveTime:         10000000000,
		GRPCKeepaliveTimeout:      3000000000,
		CipherInfo:                backup.CipherInfo{CipherType: 1},
		LogBackupCipherInfo:       backup.CipherInfo{CipherType: 1},
		MetadataDownloadBatchSize: 0x80,
	}
}

func expectedDefaultBackupConfig() BackupConfig {
	defaultConfig := expectedDefaultConfig()
	defaultConfig.Checksum = false
	return BackupConfig{
		Config: defaultConfig,
		GCTTL:  utils.DefaultBRGCSafePointTTL,
		CompressionConfig: CompressionConfig{
			CompressionType: backup.CompressionType_ZSTD,
		},
		IgnoreStats:     true,
		UseBackupMetaV2: true,
		UseCheckpoint:   true,
	}
}

func expectedDefaultRestoreConfig() RestoreConfig {
	defaultConfig := expectedDefaultConfig()
	defaultConfig.Concurrency = defaultRestoreConcurrency
	return RestoreConfig{
		Config: defaultConfig,
		RestoreCommonConfig: RestoreCommonConfig{Online: false,
			Granularity:               "fine-grained",
			MergeSmallRegionSizeBytes: kvconfig.ConfigTerm[uint64]{Value: 0x6000000},
			MergeSmallRegionKeyCount:  kvconfig.ConfigTerm[uint64]{Value: 0xea600},
			WithSysTable:              true,
			ResetSysUsers:             []string{"cloud_admin", "root"}},
		NoSchema:            false,
		LoadStats:           true,
		PDConcurrency:       0x1,
		StatsConcurrency:    0xc,
		BatchFlushInterval:  16000000000,
		DdlBatchSize:        0x80,
		WithPlacementPolicy: "STRICT",
		UseCheckpoint:       true,
	}
}

func TestDefault(t *testing.T) {
	def := DefaultConfig()
	defaultConfig := expectedDefaultConfig()
	require.Equal(t, defaultConfig, def)
}

func TestDefaultBackup(t *testing.T) {
	commonConfig := DefaultConfig()
	commonConfig.OverrideDefaultForBackup()
	def := DefaultBackupConfig(commonConfig)
	defaultConfig := expectedDefaultBackupConfig()
	require.Equal(t, defaultConfig, def)
}

func TestDefaultRestore(t *testing.T) {
	commonConfig := DefaultConfig()
	def := DefaultRestoreConfig(commonConfig)
	defaultConfig := expectedDefaultRestoreConfig()
	require.Equal(t, defaultConfig, def)
}

func TestParseAndValidateMasterKeyInfo(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		expectedKeys []*encryptionpb.MasterKey
		expectError  bool
	}{
		{
			name:         "Empty input",
			input:        "",
			expectedKeys: nil,
			expectError:  false,
		},
		{
			name:  "Single local config",
			input: "local:///path/to/key",
			expectedKeys: []*encryptionpb.MasterKey{
				{
					Backend: &encryptionpb.MasterKey_File{
						File: &encryptionpb.MasterKeyFile{Path: "/path/to/key"},
					},
				},
			},
			expectError: false,
		},
		{
			name:  "Single AWS config",
			input: "aws-kms:///key-id?AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE&AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY&REGION=us-west-2",
			expectedKeys: []*encryptionpb.MasterKey{
				{
					Backend: &encryptionpb.MasterKey_Kms{
						Kms: &encryptionpb.MasterKeyKms{
							Vendor: "aws",
							KeyId:  "key-id",
							Region: "us-west-2",
						},
					},
				},
			},
			expectError: false,
		},
		{
			name:  "Single Azure config",
			input: "azure-kms:///key-name/key-version?AZURE_TENANT_ID=tenant-id&AZURE_CLIENT_ID=client-id&AZURE_CLIENT_SECRET=client-secret&AZURE_VAULT_NAME=vault-name",
			expectedKeys: []*encryptionpb.MasterKey{
				{
					Backend: &encryptionpb.MasterKey_Kms{
						Kms: &encryptionpb.MasterKeyKms{
							Vendor: "azure",
							KeyId:  "key-name/key-version",
							AzureKms: &encryptionpb.AzureKms{
								TenantId:     "tenant-id",
								ClientId:     "client-id",
								ClientSecret: "client-secret",
								KeyVaultUrl:  "vault-name",
							},
						},
					},
				},
			},
			expectError: false,
		},
		{
			name:  "Single GCP config",
			input: "gcp-kms:///projects/project-id/locations/global/keyRings/ring-name/cryptoKeys/key-name?CREDENTIALS=credentials",
			expectedKeys: []*encryptionpb.MasterKey{
				{
					Backend: &encryptionpb.MasterKey_Kms{
						Kms: &encryptionpb.MasterKeyKms{
							Vendor: "gcp",
							KeyId:  "projects/project-id/locations/global/keyRings/ring-name/cryptoKeys/key-name",
							GcpKms: &encryptionpb.GcpKms{
								Credential: "credentials",
							},
						},
					},
				},
			},
			expectError: false,
		},
		{
			name: "Multiple configs",
			input: "local:///path/to/key," +
				"aws-kms:///key-id?AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE&AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY&REGION=us-west-2",
			expectedKeys: []*encryptionpb.MasterKey{
				{
					Backend: &encryptionpb.MasterKey_File{
						File: &encryptionpb.MasterKeyFile{Path: "/path/to/key"},
					},
				},
				{
					Backend: &encryptionpb.MasterKey_Kms{
						Kms: &encryptionpb.MasterKeyKms{
							Vendor: "aws",
							KeyId:  "key-id",
							Region: "us-west-2",
						},
					},
				},
			},
			expectError: false,
		},
		{
			name:         "Invalid config",
			input:        "invalid:///config",
			expectedKeys: nil,
			expectError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{}
			flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
			flags.String(flagMasterKeyConfig, tt.input, "")
			flags.String(flagMasterKeyCipherType, "aes256-ctr", "")

			err := cfg.parseAndValidateMasterKeyInfo(false, flags)

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expectedKeys, cfg.MasterKeyConfig.MasterKeys)
			}
		})
	}
}
