// Copyright 2024 PingCAP, Inc.
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

package snapclient_test

import (
	"context"
	"math"
	"testing"

	"github.com/pingcap/errors"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/gluetidb"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/restore"
	snapclient "github.com/pingcap/tidb/br/pkg/restore/snap_client"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/stretchr/testify/require"
)

func TestCheckSysTableCompatibility(t *testing.T) {
	cluster := mc
	g := gluetidb.New()
	client := snapclient.NewRestoreClient(cluster.PDClient, cluster.PDHTTPCli, nil, split.DefaultTestKeepaliveCfg)
	err := client.InitConnections(g, cluster.Storage)
	require.NoError(t, err)

	info, err := cluster.Domain.GetSnapshotInfoSchema(math.MaxUint64)
	require.NoError(t, err)
	dbSchema, isExist := info.SchemaByName(pmodel.NewCIStr(mysql.SystemDB))
	require.True(t, isExist)
	tmpSysDB := dbSchema.Clone()
	tmpSysDB.Name = utils.TemporaryDBName(mysql.SystemDB)
	sysDB := pmodel.NewCIStr(mysql.SystemDB)
	userTI, err := restore.GetTableSchema(cluster.Domain, sysDB, pmodel.NewCIStr("user"))
	require.NoError(t, err)

	// user table in cluster have more columns(success)
	mockedUserTI := userTI.Clone()
	userTI.Columns = append(userTI.Columns, &model.ColumnInfo{Name: pmodel.NewCIStr("new-name")})
	err = snapclient.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedUserTI,
	}})
	require.NoError(t, err)
	userTI.Columns = userTI.Columns[:len(userTI.Columns)-1]

	// user table in cluster have less columns(failed)
	mockedUserTI = userTI.Clone()
	mockedUserTI.Columns = append(mockedUserTI.Columns, &model.ColumnInfo{Name: pmodel.NewCIStr("new-name")})
	err = snapclient.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedUserTI,
	}})
	require.True(t, berrors.ErrRestoreIncompatibleSys.Equal(err))

	// column order mismatch(success)
	mockedUserTI = userTI.Clone()
	mockedUserTI.Columns[4], mockedUserTI.Columns[5] = mockedUserTI.Columns[5], mockedUserTI.Columns[4]
	err = snapclient.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedUserTI,
	}})
	require.NoError(t, err)

	// incompatible column type
	mockedUserTI = userTI.Clone()
	mockedUserTI.Columns[0].FieldType.SetFlen(2000) // Columns[0] is `Host` char(255)
	err = snapclient.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedUserTI,
	}})
	require.True(t, berrors.ErrRestoreIncompatibleSys.Equal(err))

	// compatible
	mockedUserTI = userTI.Clone()
	err = snapclient.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedUserTI,
	}})
	require.NoError(t, err)

	// use the mysql.db table to test for column count mismatch.
	dbTI, err := restore.GetTableSchema(cluster.Domain, sysDB, pmodel.NewCIStr("db"))
	require.NoError(t, err)

	// other system tables in cluster have more columns(failed)
	mockedDBTI := dbTI.Clone()
	dbTI.Columns = append(dbTI.Columns, &model.ColumnInfo{Name: pmodel.NewCIStr("new-name")})
	err = snapclient.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedDBTI,
	}})
	require.True(t, berrors.ErrRestoreIncompatibleSys.Equal(err))
}

// NOTICE: Once there is a new system table, BR needs to ensure that it is correctly classified:
//
// - IF it is an unrecoverable table, please add the table name into `unRecoverableTable`.
// - IF it is an system privilege table, please add the table name into `sysPrivilegeTableMap`.
// - IF it is an statistics table, please add the table name into `statsTables`.
//
// NOTICE: Once the schema of the statistics table updates, please update the `upgradeStatsTableSchemaList`
// and `downgradeStatsTableSchemaList`.
//
// The above variables are in the file br/pkg/restore/systable_restore.go
func TestMonitorTheSystemTableIncremental(t *testing.T) {
	require.Equal(t, int64(222), session.CurrentBootstrapVersion)
}

func TestIsStatsTemporaryTable(t *testing.T) {
	require.False(t, snapclient.IsStatsTemporaryTable("", ""))
	require.False(t, snapclient.IsStatsTemporaryTable("", "stats_meta"))
	require.False(t, snapclient.IsStatsTemporaryTable("mysql", "stats_meta"))
	require.False(t, snapclient.IsStatsTemporaryTable("__TiDB_BR_Temporary_test", "stats_meta"))
	require.True(t, snapclient.IsStatsTemporaryTable("__TiDB_BR_Temporary_mysql", "stats_meta"))
	require.False(t, snapclient.IsStatsTemporaryTable("__TiDB_BR_Temporary_mysql", "test"))
}

func TestGetDBNameIfStatsTemporaryTable(t *testing.T) {
	_, ok := snapclient.GetDBNameIfStatsTemporaryTable("", "")
	require.False(t, ok)
	_, ok = snapclient.GetDBNameIfStatsTemporaryTable("", "stats_meta")
	require.False(t, ok)
	_, ok = snapclient.GetDBNameIfStatsTemporaryTable("mysql", "stats_meta")
	require.False(t, ok)
	_, ok = snapclient.GetDBNameIfStatsTemporaryTable("__TiDB_BR_Temporary_test", "stats_meta")
	require.False(t, ok)
	name, ok := snapclient.GetDBNameIfStatsTemporaryTable("__TiDB_BR_Temporary_mysql", "stats_meta")
	require.True(t, ok)
	require.Equal(t, "mysql", name)
	_, ok = snapclient.GetDBNameIfStatsTemporaryTable("__TiDB_BR_Temporary_mysql", "test")
	require.False(t, ok)
}

func TestTemporaryTableCheckerForStatsTemporaryTable(t *testing.T) {
	checker := snapclient.NewTemporaryTableChecker(true, false)
	_, ok := checker.CheckTemporaryTables("", "")
	require.False(t, ok)
	_, ok = checker.CheckTemporaryTables("", "stats_meta")
	require.False(t, ok)
	_, ok = checker.CheckTemporaryTables("mysql", "stats_meta")
	require.False(t, ok)
	_, ok = checker.CheckTemporaryTables("__TiDB_BR_Temporary_test", "stats_meta")
	require.False(t, ok)
	name, ok := checker.CheckTemporaryTables("__TiDB_BR_Temporary_mysql", "stats_meta")
	require.True(t, ok)
	require.Equal(t, "mysql", name)
	_, ok = checker.CheckTemporaryTables("__TiDB_BR_Temporary_mysql", "test")
	require.False(t, ok)

	_, ok = checker.CheckTemporaryTables("", "user")
	require.False(t, ok)
	_, ok = checker.CheckTemporaryTables("mysql", "user")
	require.False(t, ok)
	_, ok = checker.CheckTemporaryTables("__TiDB_BR_Temporary_test", "user")
	require.False(t, ok)
	_, ok = checker.CheckTemporaryTables("__TiDB_BR_Temporary_mysql", "user")
	require.False(t, ok)
	_, ok = checker.CheckTemporaryTables("__TiDB_BR_Temporary_mysql", "test")
	require.False(t, ok)
}

func TestIsRenameableSysTemporaryTable(t *testing.T) {
	require.False(t, snapclient.IsRenameableSysTemporaryTable("", ""))
	require.False(t, snapclient.IsRenameableSysTemporaryTable("", "user"))
	require.False(t, snapclient.IsRenameableSysTemporaryTable("mysql", "user"))
	require.False(t, snapclient.IsRenameableSysTemporaryTable("__TiDB_BR_Temporary_test", "user"))
	require.True(t, snapclient.IsRenameableSysTemporaryTable("__TiDB_BR_Temporary_mysql", "user"))
	require.False(t, snapclient.IsRenameableSysTemporaryTable("__TiDB_BR_Temporary_mysql", "test"))
}

func TestGetDBNameIfRenameableSysTemporaryTable(t *testing.T) {
	_, ok := snapclient.GetDBNameIfRenameableSysTemporaryTable("", "")
	require.False(t, ok)
	_, ok = snapclient.GetDBNameIfRenameableSysTemporaryTable("", "user")
	require.False(t, ok)
	_, ok = snapclient.GetDBNameIfRenameableSysTemporaryTable("mysql", "user")
	require.False(t, ok)
	_, ok = snapclient.GetDBNameIfRenameableSysTemporaryTable("__TiDB_BR_Temporary_test", "user")
	require.False(t, ok)
	name, ok := snapclient.GetDBNameIfRenameableSysTemporaryTable("__TiDB_BR_Temporary_mysql", "user")
	require.True(t, ok)
	require.Equal(t, "mysql", name)
	_, ok = snapclient.GetDBNameIfRenameableSysTemporaryTable("__TiDB_BR_Temporary_mysql", "test")
	require.False(t, ok)
}

func TestTemporaryTableCheckerForRenameableSysTemporaryTable(t *testing.T) {
	checker := snapclient.NewTemporaryTableChecker(false, true)
	_, ok := checker.CheckTemporaryTables("", "")
	require.False(t, ok)
	_, ok = checker.CheckTemporaryTables("", "user")
	require.False(t, ok)
	_, ok = checker.CheckTemporaryTables("mysql", "user")
	require.False(t, ok)
	_, ok = checker.CheckTemporaryTables("__TiDB_BR_Temporary_test", "user")
	require.False(t, ok)
	name, ok := checker.CheckTemporaryTables("__TiDB_BR_Temporary_mysql", "user")
	require.True(t, ok)
	require.Equal(t, "mysql", name)
	_, ok = checker.CheckTemporaryTables("__TiDB_BR_Temporary_mysql", "test")
	require.False(t, ok)

	_, ok = checker.CheckTemporaryTables("", "stats_meta")
	require.False(t, ok)
	_, ok = checker.CheckTemporaryTables("mysql", "stats_meta")
	require.False(t, ok)
	_, ok = checker.CheckTemporaryTables("__TiDB_BR_Temporary_test", "stats_meta")
	require.False(t, ok)
	_, ok = checker.CheckTemporaryTables("__TiDB_BR_Temporary_mysql", "stats_meta")
	require.False(t, ok)
	_, ok = checker.CheckTemporaryTables("__TiDB_BR_Temporary_mysql", "test")
	require.False(t, ok)
}

func TestTemporaryTableChecker(t *testing.T) {
	checker := snapclient.NewTemporaryTableChecker(true, true)
	_, ok := checker.CheckTemporaryTables("", "")
	require.False(t, ok)
	_, ok = checker.CheckTemporaryTables("", "user")
	require.False(t, ok)
	_, ok = checker.CheckTemporaryTables("mysql", "user")
	require.False(t, ok)
	_, ok = checker.CheckTemporaryTables("__TiDB_BR_Temporary_test", "user")
	require.False(t, ok)
	name, ok := checker.CheckTemporaryTables("__TiDB_BR_Temporary_mysql", "user")
	require.True(t, ok)
	require.Equal(t, "mysql", name)
	_, ok = checker.CheckTemporaryTables("__TiDB_BR_Temporary_mysql", "test")
	require.False(t, ok)

	_, ok = checker.CheckTemporaryTables("", "stats_meta")
	require.False(t, ok)
	_, ok = checker.CheckTemporaryTables("mysql", "stats_meta")
	require.False(t, ok)
	_, ok = checker.CheckTemporaryTables("__TiDB_BR_Temporary_test", "stats_meta")
	require.False(t, ok)
	name, ok = checker.CheckTemporaryTables("__TiDB_BR_Temporary_mysql", "stats_meta")
	require.True(t, ok)
	require.Equal(t, "mysql", name)
	_, ok = checker.CheckTemporaryTables("__TiDB_BR_Temporary_mysql", "test")
	require.False(t, ok)
}

func TestGenerateMoveRenamedTableSQLPair(t *testing.T) {
	renameSQL := snapclient.GenerateMoveRenamedTableSQLPair(123, map[string]map[string]struct{}{
		"mysql": {"stats_meta": struct{}{}, "stats_buckets": struct{}{}, "stats_top_n": struct{}{}},
	})
	require.Contains(t, renameSQL, "mysql.stats_meta TO __TiDB_BR_Temporary_mysql.stats_meta_deleted_123")
	require.Contains(t, renameSQL, "__TiDB_BR_Temporary_mysql.stats_meta TO mysql.stats_meta")
	require.Contains(t, renameSQL, "mysql.stats_buckets TO __TiDB_BR_Temporary_mysql.stats_buckets_deleted_123")
	require.Contains(t, renameSQL, "__TiDB_BR_Temporary_mysql.stats_buckets TO mysql.stats_buckets")
	require.Contains(t, renameSQL, "mysql.stats_top_n TO __TiDB_BR_Temporary_mysql.stats_top_n_deleted_123")
	require.Contains(t, renameSQL, "__TiDB_BR_Temporary_mysql.stats_top_n TO mysql.stats_top_n")
}

func TestUpdateStatsTableSchema(t *testing.T) {
	ctx := context.Background()
	expectedSQLs := []string{}
	execution := func(_ context.Context, sql string) error {
		require.Equal(t, expectedSQLs[0], sql)
		expectedSQLs = expectedSQLs[1:]
		return nil
	}

	// schema name is mismatch
	err := snapclient.UpdateStatsTableSchema(ctx, map[string]map[string]struct{}{
		"test": {"stats_meta": struct{}{}},
	}, snapclient.SchemaVersionPairT{
		UpstreamVersionMajor:   7,
		UpstreamVersionMinor:   5,
		DownstreamVersionMajor: 8,
		DownstreamVersionMinor: 5,
	}, execution)
	require.NoError(t, err)
	err = snapclient.UpdateStatsTableSchema(ctx, map[string]map[string]struct{}{
		"test": {"stats_meta": struct{}{}},
	}, snapclient.SchemaVersionPairT{
		UpstreamVersionMajor:   8,
		UpstreamVersionMinor:   5,
		DownstreamVersionMajor: 7,
		DownstreamVersionMinor: 5,
	}, execution)
	require.NoError(t, err)

	// table name is mismatch
	err = snapclient.UpdateStatsTableSchema(ctx, map[string]map[string]struct{}{
		"mysql": {"stats_meta2": struct{}{}},
	}, snapclient.SchemaVersionPairT{
		UpstreamVersionMajor:   7,
		UpstreamVersionMinor:   5,
		DownstreamVersionMajor: 8,
		DownstreamVersionMinor: 5,
	}, execution)
	require.NoError(t, err)
	err = snapclient.UpdateStatsTableSchema(ctx, map[string]map[string]struct{}{
		"mysql": {"stats_meta2": struct{}{}},
	}, snapclient.SchemaVersionPairT{
		UpstreamVersionMajor:   8,
		UpstreamVersionMinor:   5,
		DownstreamVersionMajor: 7,
		DownstreamVersionMinor: 5,
	}, execution)
	require.NoError(t, err)

	// version range is mismatch
	err = snapclient.UpdateStatsTableSchema(ctx, map[string]map[string]struct{}{
		"mysql": {"stats_meta": struct{}{}},
	}, snapclient.SchemaVersionPairT{
		UpstreamVersionMajor:   7,
		UpstreamVersionMinor:   5,
		DownstreamVersionMajor: 8,
		DownstreamVersionMinor: 1,
	}, execution)
	require.NoError(t, err)
	err = snapclient.UpdateStatsTableSchema(ctx, map[string]map[string]struct{}{
		"mysql": {"stats_meta": struct{}{}},
	}, snapclient.SchemaVersionPairT{
		UpstreamVersionMajor:   8,
		UpstreamVersionMinor:   1,
		DownstreamVersionMajor: 7,
		DownstreamVersionMinor: 5,
	}, execution)
	require.NoError(t, err)
	err = snapclient.UpdateStatsTableSchema(ctx, map[string]map[string]struct{}{
		"mysql": {"stats_meta": struct{}{}},
	}, snapclient.SchemaVersionPairT{
		UpstreamVersionMajor:   9,
		UpstreamVersionMinor:   1,
		DownstreamVersionMajor: 9,
		DownstreamVersionMinor: 5,
	}, execution)
	require.NoError(t, err)
	err = snapclient.UpdateStatsTableSchema(ctx, map[string]map[string]struct{}{
		"mysql": {"stats_meta": struct{}{}},
	}, snapclient.SchemaVersionPairT{
		UpstreamVersionMajor:   9,
		UpstreamVersionMinor:   5,
		DownstreamVersionMajor: 9,
		DownstreamVersionMinor: 1,
	}, execution)
	require.NoError(t, err)

	// match
	expectedSQLs = []string{
		"ALTER TABLE __TiDB_BR_Temporary_mysql.stats_meta ADD COLUMN IF NOT EXISTS last_stats_histograms_version bigint unsigned DEFAULT NULL",
		"ALTER TABLE __TiDB_BR_Temporary_mysql.stats_meta DROP COLUMN IF EXISTS last_stats_histograms_version",
		"ALTER TABLE __TiDB_BR_Temporary_mysql.stats_meta ADD COLUMN IF NOT EXISTS last_stats_histograms_version bigint unsigned DEFAULT NULL",
		"ALTER TABLE __TiDB_BR_Temporary_mysql.stats_meta DROP COLUMN IF EXISTS last_stats_histograms_version",
	}
	err = snapclient.UpdateStatsTableSchema(ctx, map[string]map[string]struct{}{
		"mysql": {"stats_meta": struct{}{}},
	}, snapclient.SchemaVersionPairT{
		UpstreamVersionMajor:   7,
		UpstreamVersionMinor:   5,
		DownstreamVersionMajor: 8,
		DownstreamVersionMinor: 5,
	}, execution)
	require.NoError(t, err)
	err = snapclient.UpdateStatsTableSchema(ctx, map[string]map[string]struct{}{
		"mysql": {"stats_meta": struct{}{}},
	}, snapclient.SchemaVersionPairT{
		UpstreamVersionMajor:   8,
		UpstreamVersionMinor:   5,
		DownstreamVersionMajor: 8,
		DownstreamVersionMinor: 1,
	}, execution)
	require.NoError(t, err)
	err = snapclient.UpdateStatsTableSchema(ctx, map[string]map[string]struct{}{
		"mysql": {"stats_meta": struct{}{}, "test": struct{}{}},
		"test":  {"stats_meta": struct{}{}},
	}, snapclient.SchemaVersionPairT{
		UpstreamVersionMajor:   8,
		UpstreamVersionMinor:   1,
		DownstreamVersionMajor: 8,
		DownstreamVersionMinor: 5,
	}, execution)
	require.NoError(t, err)
	err = snapclient.UpdateStatsTableSchema(ctx, map[string]map[string]struct{}{
		"mysql": {"stats_meta": struct{}{}, "test": struct{}{}},
		"test":  {"stats_meta": struct{}{}},
	}, snapclient.SchemaVersionPairT{
		UpstreamVersionMajor:   8,
		UpstreamVersionMinor:   5,
		DownstreamVersionMajor: 7,
		DownstreamVersionMinor: 5,
	}, execution)
	require.NoError(t, err)
	err = snapclient.UpdateStatsTableSchema(ctx, map[string]map[string]struct{}{
		"mysql": {"stats_meta": struct{}{}, "test": struct{}{}},
		"test":  {"stats_meta": struct{}{}},
	}, snapclient.SchemaVersionPairT{
		UpstreamVersionMajor:   8,
		UpstreamVersionMinor:   5,
		DownstreamVersionMajor: 8,
		DownstreamVersionMinor: 5,
	}, execution)
	require.NoError(t, err)
}

func TestNotifyUpdateAllUsersPrivilege(t *testing.T) {
	notifier := func() error {
		return errors.Errorf("test")
	}
	err := snapclient.NotifyUpdateAllUsersPrivilege(map[string]map[string]struct{}{
		"test": {"user": {}},
	}, notifier)
	require.NoError(t, err)
	err = snapclient.NotifyUpdateAllUsersPrivilege(map[string]map[string]struct{}{
		"mysql": {"use": {}, "test": {}},
	}, notifier)
	require.NoError(t, err)
	err = snapclient.NotifyUpdateAllUsersPrivilege(map[string]map[string]struct{}{
		"mysql": {"test": {}, "user": {}, "db": {}},
	}, notifier)
	require.Error(t, err)
}
