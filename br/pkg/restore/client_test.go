// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore_test

import (
	"context"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/gluetidb"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/types"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	"google.golang.org/grpc/keepalive"
)

var mc *mock.Cluster

var defaultKeepaliveCfg = keepalive.ClientParameters{
	Time:    3 * time.Second,
	Timeout: 10 * time.Second,
}

func TestCreateTables(t *testing.T) {
	m := mc
	g := gluetidb.New()
	client := restore.NewRestoreClient(m.PDClient, nil, defaultKeepaliveCfg, false)
	err := client.Init(g, m.Storage)
	require.NoError(t, err)

	info, err := m.Domain.GetSnapshotInfoSchema(math.MaxUint64)
	require.NoError(t, err)
	dbSchema, isExist := info.SchemaByName(model.NewCIStr("test"))
	require.True(t, isExist)

	client.SetBatchDdlSize(1)
	tables := make([]*metautil.Table, 4)
	intField := types.NewFieldType(mysql.TypeLong)
	intField.SetCharset("binary")
	for i := len(tables) - 1; i >= 0; i-- {
		tables[i] = &metautil.Table{
			DB: dbSchema,
			Info: &model.TableInfo{
				ID:   int64(i),
				Name: model.NewCIStr("test" + strconv.Itoa(i)),
				Columns: []*model.ColumnInfo{{
					ID:        1,
					Name:      model.NewCIStr("id"),
					FieldType: *intField,
					State:     model.StatePublic,
				}},
				Charset: "utf8mb4",
				Collate: "utf8mb4_bin",
			},
		}
	}
	rules, newTables, err := client.CreateTables(m.Domain, tables, 0)
	require.NoError(t, err)
	// make sure tables and newTables have same order
	for i, tbl := range tables {
		require.Equal(t, tbl.Info.Name, newTables[i].Name)
	}
	for _, nt := range newTables {
		require.Regexp(t, "test[0-3]", nt.Name.String())
	}
	oldTableIDExist := make(map[int64]bool)
	newTableIDExist := make(map[int64]bool)
	for _, tr := range rules.Data {
		oldTableID := tablecodec.DecodeTableID(tr.GetOldKeyPrefix())
		require.False(t, oldTableIDExist[oldTableID], "table rule duplicate old table id")
		oldTableIDExist[oldTableID] = true

		newTableID := tablecodec.DecodeTableID(tr.GetNewKeyPrefix())
		require.False(t, newTableIDExist[newTableID], "table rule duplicate new table id")
		newTableIDExist[newTableID] = true
	}

	for i := 0; i < len(tables); i++ {
		require.True(t, oldTableIDExist[int64(i)], "table rule does not exist")
	}
}

func TestIsOnline(t *testing.T) {
	m := mc
	g := gluetidb.New()
	client := restore.NewRestoreClient(m.PDClient, nil, defaultKeepaliveCfg, false)
	err := client.Init(g, m.Storage)
	require.NoError(t, err)

	require.False(t, client.IsOnline())
	client.EnableOnline()
	require.True(t, client.IsOnline())
}

func getStartedMockedCluster(t *testing.T) *mock.Cluster {
	t.Helper()
	cluster, err := mock.NewCluster()
	require.NoError(t, err)
	err = cluster.Start()
	require.NoError(t, err)
	return cluster
}

func TestCheckTargetClusterFresh(t *testing.T) {
	// cannot use shared `mc`, other parallel case may change it.
	cluster := getStartedMockedCluster(t)
	defer cluster.Stop()

	g := gluetidb.New()
	client := restore.NewRestoreClient(cluster.PDClient, nil, defaultKeepaliveCfg, false)
	err := client.Init(g, cluster.Storage)
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, client.CheckTargetClusterFresh(ctx))

	require.NoError(t, client.CreateDatabase(ctx, &model.DBInfo{Name: model.NewCIStr("user_db")}))
	require.True(t, berrors.ErrRestoreNotFreshCluster.Equal(client.CheckTargetClusterFresh(ctx)))
}

func TestCheckTargetClusterFreshWithTable(t *testing.T) {
	testFunc := func(targetDBName string) {
		// cannot use shared `mc`, other parallel case may change it.
		cluster := getStartedMockedCluster(t)
		defer cluster.Stop()

		g := gluetidb.New()
		client := restore.NewRestoreClient(cluster.PDClient, nil, defaultKeepaliveCfg, false)
		err := client.Init(g, cluster.Storage)
		require.NoError(t, err)

		ctx := context.Background()
		info, err := cluster.Domain.GetSnapshotInfoSchema(math.MaxUint64)
		require.NoError(t, err)
		dbSchema, isExist := info.SchemaByName(model.NewCIStr(targetDBName))
		require.True(t, isExist)
		intField := types.NewFieldType(mysql.TypeLong)
		intField.SetCharset("binary")
		table := &metautil.Table{
			DB: dbSchema,
			Info: &model.TableInfo{
				ID:   int64(1),
				Name: model.NewCIStr("t"),
				Columns: []*model.ColumnInfo{{
					ID:        1,
					Name:      model.NewCIStr("id"),
					FieldType: *intField,
					State:     model.StatePublic,
				}},
				Charset: "utf8mb4",
				Collate: "utf8mb4_bin",
			},
		}
		_, _, err = client.CreateTables(cluster.Domain, []*metautil.Table{table}, 0)
		require.NoError(t, err)

		require.True(t, berrors.ErrRestoreNotFreshCluster.Equal(client.CheckTargetClusterFresh(ctx)))
	}
	testFunc("test")
	testFunc(mysql.SystemDB)
}

func TestCheckSysTableCompatibility(t *testing.T) {
	cluster := mc
	g := gluetidb.New()
	client := restore.NewRestoreClient(cluster.PDClient, nil, defaultKeepaliveCfg, false)
	err := client.Init(g, cluster.Storage)
	require.NoError(t, err)

	info, err := cluster.Domain.GetSnapshotInfoSchema(math.MaxUint64)
	require.NoError(t, err)
	dbSchema, isExist := info.SchemaByName(model.NewCIStr(mysql.SystemDB))
	require.True(t, isExist)
	tmpSysDB := dbSchema.Clone()
	tmpSysDB.Name = utils.TemporaryDBName(mysql.SystemDB)
	sysDB := model.NewCIStr(mysql.SystemDB)
	userTI, err := client.GetTableSchema(cluster.Domain, sysDB, model.NewCIStr("user"))
	require.NoError(t, err)

	// column count mismatch
	mockedUserTI := userTI.Clone()
	mockedUserTI.Columns = mockedUserTI.Columns[:len(mockedUserTI.Columns)-1]
	err = client.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedUserTI,
	}})
	require.True(t, berrors.ErrRestoreIncompatibleSys.Equal(err))

	// column order mismatch(success)
	mockedUserTI = userTI.Clone()
	mockedUserTI.Columns[4], mockedUserTI.Columns[5] = mockedUserTI.Columns[5], mockedUserTI.Columns[4]
	err = client.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedUserTI,
	}})
	require.NoError(t, err)

	// missing column
	mockedUserTI = userTI.Clone()
	mockedUserTI.Columns[0].Name = model.NewCIStr("new-name")
	err = client.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedUserTI,
	}})
	require.True(t, berrors.ErrRestoreIncompatibleSys.Equal(err))

	// incompatible column type
	mockedUserTI = userTI.Clone()
	mockedUserTI.Columns[0].FieldType.SetFlen(2000) // Columns[0] is `Host` char(255)
	err = client.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedUserTI,
	}})
	require.True(t, berrors.ErrRestoreIncompatibleSys.Equal(err))

	// compatible
	mockedUserTI = userTI.Clone()
	err = client.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedUserTI,
	}})
	require.NoError(t, err)
}

func TestInitFullClusterRestore(t *testing.T) {
	cluster := mc
	g := gluetidb.New()
	client := restore.NewRestoreClient(cluster.PDClient, nil, defaultKeepaliveCfg, false)
	err := client.Init(g, cluster.Storage)
	require.NoError(t, err)

	// explicit filter
	client.InitFullClusterRestore(true)
	require.False(t, client.IsFullClusterRestore())

	client.InitFullClusterRestore(false)
	require.True(t, client.IsFullClusterRestore())
	// set it to false again
	client.InitFullClusterRestore(true)
	require.False(t, client.IsFullClusterRestore())

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/br/pkg/restore/mock-incr-backup-data", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/br/pkg/restore/mock-incr-backup-data"))
	}()
	client.InitFullClusterRestore(false)
	require.False(t, client.IsFullClusterRestore())
}

func TestPreCheckTableClusterIndex(t *testing.T) {
	m := mc
	g := gluetidb.New()
	client := restore.NewRestoreClient(m.PDClient, nil, defaultKeepaliveCfg, false)
	err := client.Init(g, m.Storage)
	require.NoError(t, err)

	info, err := m.Domain.GetSnapshotInfoSchema(math.MaxUint64)
	require.NoError(t, err)
	dbSchema, isExist := info.SchemaByName(model.NewCIStr("test"))
	require.True(t, isExist)

	tables := make([]*metautil.Table, 4)
	intField := types.NewFieldType(mysql.TypeLong)
	intField.SetCharset("binary")
	for i := len(tables) - 1; i >= 0; i-- {
		tables[i] = &metautil.Table{
			DB: dbSchema,
			Info: &model.TableInfo{
				ID:   int64(i),
				Name: model.NewCIStr("test" + strconv.Itoa(i)),
				Columns: []*model.ColumnInfo{{
					ID:        1,
					Name:      model.NewCIStr("id"),
					FieldType: *intField,
					State:     model.StatePublic,
				}},
				Charset: "utf8mb4",
				Collate: "utf8mb4_bin",
			},
		}
	}
	_, _, err = client.CreateTables(m.Domain, tables, 0)
	require.NoError(t, err)

	// exist different tables
	tables[1].Info.IsCommonHandle = true
	err = client.PreCheckTableClusterIndex(tables, nil, m.Domain)
	require.Error(t, err)
	require.Regexp(t, `.*@@tidb_enable_clustered_index should be ON \(backup table = true, created table = false\).*`, err.Error())

	// exist different DDLs
	jobs := []*model.Job{{
		ID:         5,
		Type:       model.ActionCreateTable,
		SchemaName: "test",
		Query:      "",
		BinlogInfo: &model.HistoryInfo{
			TableInfo: &model.TableInfo{
				Name:           model.NewCIStr("test1"),
				IsCommonHandle: true,
			},
		},
	}}
	err = client.PreCheckTableClusterIndex(nil, jobs, m.Domain)
	require.Error(t, err)
	require.Regexp(t, `.*@@tidb_enable_clustered_index should be ON \(backup table = true, created table = false\).*`, err.Error())

	// should pass pre-check cluster index
	tables[1].Info.IsCommonHandle = false
	jobs[0].BinlogInfo.TableInfo.IsCommonHandle = false
	require.Nil(t, client.PreCheckTableClusterIndex(tables, jobs, m.Domain))
}

type fakePDClient struct {
	pd.Client
	stores []*metapb.Store
}

func (fpdc fakePDClient) GetAllStores(context.Context, ...pd.GetStoreOption) ([]*metapb.Store, error) {
	return append([]*metapb.Store{}, fpdc.stores...), nil
}

func TestPreCheckTableTiFlashReplicas(t *testing.T) {
	m := mc
	mockStores := []*metapb.Store{
		{
			Id: 1,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "engine",
					Value: "tiflash",
				},
			},
		},
		{
			Id: 2,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "engine",
					Value: "tiflash",
				},
			},
		},
	}

	g := gluetidb.New()
	client := restore.NewRestoreClient(fakePDClient{
		stores: mockStores,
	}, nil, defaultKeepaliveCfg, false)
	err := client.Init(g, m.Storage)
	require.NoError(t, err)

	tables := make([]*metautil.Table, 4)
	for i := 0; i < len(tables); i++ {
		tiflashReplica := &model.TiFlashReplicaInfo{
			Count: uint64(i),
		}
		if i == 0 {
			tiflashReplica = nil
		}

		tables[i] = &metautil.Table{
			DB: nil,
			Info: &model.TableInfo{
				ID:             int64(i),
				Name:           model.NewCIStr("test" + strconv.Itoa(i)),
				TiFlashReplica: tiflashReplica,
			},
		}
	}
	ctx := context.Background()
	require.Nil(t, client.PreCheckTableTiFlashReplica(ctx, tables, false))

	for i := 0; i < len(tables); i++ {
		if i == 0 || i > 2 {
			require.Nil(t, tables[i].Info.TiFlashReplica)
		} else {
			require.NotNil(t, tables[i].Info.TiFlashReplica)
			obtainCount := int(tables[i].Info.TiFlashReplica.Count)
			require.Equal(t, i, obtainCount)
		}
	}

	require.Nil(t, client.PreCheckTableTiFlashReplica(ctx, tables, true))
	for i := 0; i < len(tables); i++ {
		require.Nil(t, tables[i].Info.TiFlashReplica)
	}
}
