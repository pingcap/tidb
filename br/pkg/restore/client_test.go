// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore_test

import (
	"context"
	"fmt"
	"math"
	"slices"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/gluetidb"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/restore/tiflashrec"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/utils/iter"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/tablecodec"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
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
	client := restore.NewRestoreClient(m.PDClient, m.PDHTTPCli, nil, defaultKeepaliveCfg, false)
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
	client := restore.NewRestoreClient(m.PDClient, m.PDHTTPCli, nil, defaultKeepaliveCfg, false)
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

func TestNeedCheckTargetClusterFresh(t *testing.T) {
	// cannot use shared `mc`, other parallel case may change it.
	cluster := getStartedMockedCluster(t)
	defer cluster.Stop()

	g := gluetidb.New()
	client := restore.NewRestoreClient(cluster.PDClient, cluster.PDHTTPCli, nil, defaultKeepaliveCfg, false)
	err := client.Init(g, cluster.Storage)
	require.NoError(t, err)

	// not set filter and first run with checkpoint
	require.True(t, client.NeedCheckFreshCluster(false, true))

	// skip check when has checkpoint
	require.False(t, client.NeedCheckFreshCluster(false, false))

	// skip check when set --filter
	require.False(t, client.NeedCheckFreshCluster(true, false))

	// skip check when has set --filter and has checkpoint
	require.False(t, client.NeedCheckFreshCluster(true, true))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/br/pkg/restore/mock-incr-backup-data", "return(false)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/br/pkg/restore/mock-incr-backup-data"))
	}()
	// skip check when increment backup
	require.False(t, client.NeedCheckFreshCluster(false, true))
}

func TestCheckTargetClusterFresh(t *testing.T) {
	// cannot use shared `mc`, other parallel case may change it.
	cluster := getStartedMockedCluster(t)
	defer cluster.Stop()

	g := gluetidb.New()
	client := restore.NewRestoreClient(cluster.PDClient, cluster.PDHTTPCli, nil, defaultKeepaliveCfg, false)
	err := client.Init(g, cluster.Storage)
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, client.CheckTargetClusterFresh(ctx))

	require.NoError(t, client.CreateDatabases(ctx, []*metautil.Database{{Info: &model.DBInfo{Name: model.NewCIStr("user_db")}}}))
	require.True(t, berrors.ErrRestoreNotFreshCluster.Equal(client.CheckTargetClusterFresh(ctx)))
}

func TestCheckTargetClusterFreshWithTable(t *testing.T) {
	// cannot use shared `mc`, other parallel case may change it.
	cluster := getStartedMockedCluster(t)
	defer cluster.Stop()

	g := gluetidb.New()
	client := restore.NewRestoreClient(cluster.PDClient, cluster.PDHTTPCli, nil, defaultKeepaliveCfg, false)
	err := client.Init(g, cluster.Storage)
	require.NoError(t, err)

	ctx := context.Background()
	info, err := cluster.Domain.GetSnapshotInfoSchema(math.MaxUint64)
	require.NoError(t, err)
	dbSchema, isExist := info.SchemaByName(model.NewCIStr("test"))
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

func TestCheckSysTableCompatibility(t *testing.T) {
	cluster := mc
	g := gluetidb.New()
	client := restore.NewRestoreClient(cluster.PDClient, cluster.PDHTTPCli, nil, defaultKeepaliveCfg, false)
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

	// user table in cluster have more columns(success)
	mockedUserTI := userTI.Clone()
	userTI.Columns = append(userTI.Columns, &model.ColumnInfo{Name: model.NewCIStr("new-name")})
	err = client.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedUserTI,
	}})
	require.NoError(t, err)
	userTI.Columns = userTI.Columns[:len(userTI.Columns)-1]

	// user table in cluster have less columns(failed)
	mockedUserTI = userTI.Clone()
	mockedUserTI.Columns = append(mockedUserTI.Columns, &model.ColumnInfo{Name: model.NewCIStr("new-name")})
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

	// use the mysql.db table to test for column count mismatch.
	dbTI, err := client.GetTableSchema(cluster.Domain, sysDB, model.NewCIStr("db"))
	require.NoError(t, err)

	// other system tables in cluster have more columns(failed)
	mockedDBTI := dbTI.Clone()
	dbTI.Columns = append(dbTI.Columns, &model.ColumnInfo{Name: model.NewCIStr("new-name")})
	err = client.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedDBTI,
	}})
	require.True(t, berrors.ErrRestoreIncompatibleSys.Equal(err))
}

func TestInitFullClusterRestore(t *testing.T) {
	cluster := mc
	g := gluetidb.New()
	client := restore.NewRestoreClient(cluster.PDClient, cluster.PDHTTPCli, nil, defaultKeepaliveCfg, false)
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
	client := restore.NewRestoreClient(m.PDClient, m.PDHTTPCli, nil, defaultKeepaliveCfg, false)
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

	notLeader  bool
	retryTimes *int
}

func (fpdc fakePDClient) GetAllStores(context.Context, ...pd.GetStoreOption) ([]*metapb.Store, error) {
	return append([]*metapb.Store{}, fpdc.stores...), nil
}

func (fpdc fakePDClient) GetTS(ctx context.Context) (int64, int64, error) {
	(*fpdc.retryTimes)++
	if *fpdc.retryTimes >= 3 { // the mock PD leader switched successfully
		fpdc.notLeader = false
	}

	if fpdc.notLeader {
		return 0, 0, errors.Errorf("rpc error: code = Unknown desc = [PD:tso:ErrGenerateTimestamp]generate timestamp failed, requested pd is not leader of cluster")
	}
	return 1, 1, nil
}

func TestGetTSWithRetry(t *testing.T) {
	t.Run("PD leader is healthy:", func(t *testing.T) {
		retryTimes := -1000
		pDClient := fakePDClient{notLeader: false, retryTimes: &retryTimes}
		client := restore.NewRestoreClient(pDClient, nil, nil, defaultKeepaliveCfg, false)
		_, err := client.GetTSWithRetry(context.Background())
		require.NoError(t, err)
	})

	t.Run("PD leader failure:", func(t *testing.T) {
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/br/pkg/utils/set-attempt-to-one", "1*return(true)"))
		defer func() {
			require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/br/pkg/utils/set-attempt-to-one"))
		}()
		retryTimes := -1000
		pDClient := fakePDClient{notLeader: true, retryTimes: &retryTimes}
		client := restore.NewRestoreClient(pDClient, nil, nil, defaultKeepaliveCfg, false)
		_, err := client.GetTSWithRetry(context.Background())
		require.Error(t, err)
	})

	t.Run("PD leader switch successfully", func(t *testing.T) {
		retryTimes := 0
		pDClient := fakePDClient{notLeader: true, retryTimes: &retryTimes}
		client := restore.NewRestoreClient(pDClient, nil, nil, defaultKeepaliveCfg, false)
		_, err := client.GetTSWithRetry(context.Background())
		require.NoError(t, err)
	})
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
	}, nil, nil, defaultKeepaliveCfg, false)
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
			DB: &model.DBInfo{Name: model.NewCIStr("test")},
			Info: &model.TableInfo{
				ID:             int64(i),
				Name:           model.NewCIStr("test" + strconv.Itoa(i)),
				TiFlashReplica: tiflashReplica,
			},
		}
	}
	ctx := context.Background()
	require.Nil(t, client.PreCheckTableTiFlashReplica(ctx, tables, nil))

	for i := 0; i < len(tables); i++ {
		if i == 0 || i > 2 {
			require.Nil(t, tables[i].Info.TiFlashReplica)
		} else {
			require.NotNil(t, tables[i].Info.TiFlashReplica)
			obtainCount := int(tables[i].Info.TiFlashReplica.Count)
			require.Equal(t, i, obtainCount)
		}
	}

	require.Nil(t, client.PreCheckTableTiFlashReplica(ctx, tables, tiflashrec.New()))
	for i := 0; i < len(tables); i++ {
		require.Nil(t, tables[i].Info.TiFlashReplica)
	}
}

// Mock ImporterClient interface
type FakeImporterClient struct {
	restore.ImporterClient
}

// Record the stores that have communicated
type RecordStores struct {
	mu     sync.Mutex
	stores []uint64
}

func NewRecordStores() RecordStores {
	return RecordStores{stores: make([]uint64, 0)}
}

func (r *RecordStores) put(id uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stores = append(r.stores, id)
}

func (r *RecordStores) sort() {
	r.mu.Lock()
	defer r.mu.Unlock()
	slices.Sort(r.stores)
}

func (r *RecordStores) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.stores)
}

func (r *RecordStores) get(i int) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stores[i]
}

func (r *RecordStores) toString() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return fmt.Sprintf("%v", r.stores)
}

var recordStores RecordStores

const (
	SET_SPEED_LIMIT_ERROR = 999999
	WORKING_TIME          = 100
)

func (fakeImportCli FakeImporterClient) SetDownloadSpeedLimit(
	ctx context.Context,
	storeID uint64,
	req *import_sstpb.SetDownloadSpeedLimitRequest,
) (*import_sstpb.SetDownloadSpeedLimitResponse, error) {
	if storeID == SET_SPEED_LIMIT_ERROR {
		return nil, fmt.Errorf("storeID:%v ERROR", storeID)
	}

	time.Sleep(WORKING_TIME * time.Millisecond) // simulate doing 100 ms work
	recordStores.put(storeID)
	return nil, nil
}

func TestSetSpeedLimit(t *testing.T) {
	mockStores := []*metapb.Store{
		{Id: 1},
		{Id: 2},
		{Id: 3},
		{Id: 4},
		{Id: 5},
		{Id: 6},
		{Id: 7},
		{Id: 8},
		{Id: 9},
		{Id: 10},
	}

	// 1. The cost of concurrent communication is expected to be less than the cost of serial communication.
	client := restore.NewRestoreClient(fakePDClient{
		stores: mockStores,
	}, nil, nil, defaultKeepaliveCfg, false)
	ctx := context.Background()

	recordStores = NewRecordStores()
	start := time.Now()
	err := restore.MockCallSetSpeedLimit(ctx, FakeImporterClient{}, client, 10)
	cost := time.Since(start)
	require.NoError(t, err)

	recordStores.sort()
	t.Logf("Total Cost: %v\n", cost)
	t.Logf("Has Communicated: %v\n", recordStores.toString())

	serialCost := len(mockStores) * WORKING_TIME
	require.Less(t, cost, time.Duration(serialCost)*time.Millisecond)
	require.Equal(t, len(mockStores), recordStores.len())
	for i := 0; i < recordStores.len(); i++ {
		require.Equal(t, mockStores[i].Id, recordStores.get(i))
	}

	// 2. Expect the number of communicated stores to be less than the length of the mockStore
	// Because subsequent unstarted communications are aborted when an error is encountered.
	recordStores = NewRecordStores()
	mockStores[5].Id = SET_SPEED_LIMIT_ERROR // setting a fault store
	client = restore.NewRestoreClient(fakePDClient{
		stores: mockStores,
	}, nil, nil, defaultKeepaliveCfg, false)

	// Concurrency needs to be less than the number of stores
	err = restore.MockCallSetSpeedLimit(ctx, FakeImporterClient{}, client, 2)
	require.Error(t, err)
	t.Log(err)

	recordStores.sort()
	sort.Slice(mockStores, func(i, j int) bool { return mockStores[i].Id < mockStores[j].Id })
	t.Logf("Has Communicated: %v\n", recordStores.toString())
	require.Less(t, recordStores.len(), len(mockStores))
	for i := 0; i < recordStores.len(); i++ {
		require.Equal(t, mockStores[i].Id, recordStores.get(i))
	}
}

var deleteRangeQueryList = []*stream.PreDelRangeQuery{
	{
		Sql: "INSERT IGNORE INTO mysql.gc_delete_range VALUES (%?, %?, %?, %?, %?), (%?, %?, %?, %?, %?)",
		ParamsList: []stream.DelRangeParams{
			{
				JobID:    1,
				ElemID:   1,
				StartKey: "a",
				EndKey:   "b",
			},
			{
				JobID:    1,
				ElemID:   2,
				StartKey: "b",
				EndKey:   "c",
			},
		},
	},
	{
		// When the last table id has no rewrite rule
		Sql: "INSERT IGNORE INTO mysql.gc_delete_range VALUES (%?, %?, %?, %?, %?),",
		ParamsList: []stream.DelRangeParams{
			{
				JobID:    2,
				ElemID:   1,
				StartKey: "a",
				EndKey:   "b",
			},
		},
	},
	{
		// When all the tables have no rewrite rule
		Sql:        "INSERT IGNORE INTO mysql.gc_delete_range VALUES ",
		ParamsList: nil,
	},
}

func TestDeleteRangeQuery(t *testing.T) {
	ctx := context.Background()
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
	}, nil, nil, defaultKeepaliveCfg, false)
	err := client.Init(g, m.Storage)
	require.NoError(t, err)

	client.RunGCRowsLoader(ctx)

	for _, query := range deleteRangeQueryList {
		client.RecordDeleteRange(query)
	}
	querys := client.GetGCRows()
	require.Equal(t, len(querys), len(deleteRangeQueryList))
	for i, query := range querys {
		expected_query := deleteRangeQueryList[i]
		require.Equal(t, expected_query.Sql, query.Sql)
		require.Equal(t, len(expected_query.ParamsList), len(query.ParamsList))
		for j := range expected_query.ParamsList {
			require.Equal(t, expected_query.ParamsList[j], query.ParamsList[j])
		}
	}
}

func TestDeleteRangeQueryExec(t *testing.T) {
	ctx := context.Background()
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
	retryCnt := 1
	client := restore.NewRestoreClient(fakePDClient{
		stores:     mockStores,
		retryTimes: &retryCnt,
	}, nil, nil, defaultKeepaliveCfg, false)
	err := client.Init(g, m.Storage)
	require.NoError(t, err)

	client.RunGCRowsLoader(ctx)

	for _, query := range deleteRangeQueryList {
		client.RecordDeleteRange(query)
	}

	require.NoError(t, client.InsertGCRows(ctx))
}

func MockEmptySchemasReplace() *stream.SchemasReplace {
	dbMap := make(map[stream.UpstreamID]*stream.DBReplace)
	return stream.NewSchemasReplace(
		dbMap,
		true,
		nil,
		9527,
		filter.All(),
		nil,
		nil,
		nil,
	)
}

func TestRestoreBatchMetaKVFiles(t *testing.T) {
	client := restore.MockClient(nil)
	files := []*backuppb.DataFileInfo{}
	// test empty files and entries
	next, err := client.RestoreBatchMetaKVFiles(context.Background(), files[0:], nil, make([]*restore.KvEntryWithTS, 0), math.MaxUint64, nil, nil, "")
	require.NoError(t, err)
	require.Equal(t, 0, len(next))
}

func TestRestoreMetaKVFilesWithBatchMethod1(t *testing.T) {
	files_default := []*backuppb.DataFileInfo{}
	files_write := []*backuppb.DataFileInfo{}
	batchCount := 0

	client := restore.MockClient(nil)
	sr := MockEmptySchemasReplace()
	err := client.RestoreMetaKVFilesWithBatchMethod(
		context.Background(),
		files_default,
		files_write,
		sr,
		nil,
		nil,
		func(
			ctx context.Context,
			files []*backuppb.DataFileInfo,
			schemasReplace *stream.SchemasReplace,
			entries []*restore.KvEntryWithTS,
			filterTS uint64,
			updateStats func(kvCount uint64, size uint64),
			progressInc func(),
			cf string,
		) ([]*restore.KvEntryWithTS, error) {
			require.Equal(t, 0, len(entries))
			require.Equal(t, 0, len(files))
			batchCount++
			return nil, nil
		},
	)
	require.Nil(t, err)
	require.Equal(t, batchCount, 2)
}

func TestRestoreMetaKVFilesWithBatchMethod2_default_empty(t *testing.T) {
	files_default := []*backuppb.DataFileInfo{}
	files_write := []*backuppb.DataFileInfo{
		{
			Path:  "f1",
			MinTs: 100,
			MaxTs: 120,
		},
	}
	batchCount := 0

	client := restore.MockClient(nil)
	sr := MockEmptySchemasReplace()
	err := client.RestoreMetaKVFilesWithBatchMethod(
		context.Background(),
		files_default,
		files_write,
		sr,
		nil,
		nil,
		func(
			ctx context.Context,
			files []*backuppb.DataFileInfo,
			schemasReplace *stream.SchemasReplace,
			entries []*restore.KvEntryWithTS,
			filterTS uint64,
			updateStats func(kvCount uint64, size uint64),
			progressInc func(),
			cf string,
		) ([]*restore.KvEntryWithTS, error) {
			if len(entries) == 0 && len(files) == 0 {
				require.Equal(t, stream.DefaultCF, cf)
				batchCount++
			} else {
				require.Equal(t, 0, len(entries))
				require.Equal(t, 1, len(files))
				require.Equal(t, uint64(100), files[0].MinTs)
				require.Equal(t, stream.WriteCF, cf)
			}
			require.Equal(t, uint64(math.MaxUint64), filterTS)
			return nil, nil
		},
	)
	require.Nil(t, err)
	require.Equal(t, batchCount, 1)
}

func TestRestoreMetaKVFilesWithBatchMethod2_write_empty_1(t *testing.T) {
	files_default := []*backuppb.DataFileInfo{
		{
			Path:  "f1",
			MinTs: 100,
			MaxTs: 120,
		},
	}
	files_write := []*backuppb.DataFileInfo{}
	batchCount := 0

	client := restore.MockClient(nil)
	sr := MockEmptySchemasReplace()
	err := client.RestoreMetaKVFilesWithBatchMethod(
		context.Background(),
		files_default,
		files_write,
		sr,
		nil,
		nil,
		func(
			ctx context.Context,
			files []*backuppb.DataFileInfo,
			schemasReplace *stream.SchemasReplace,
			entries []*restore.KvEntryWithTS,
			filterTS uint64,
			updateStats func(kvCount uint64, size uint64),
			progressInc func(),
			cf string,
		) ([]*restore.KvEntryWithTS, error) {
			if len(entries) == 0 && len(files) == 0 {
				require.Equal(t, stream.WriteCF, cf)
				batchCount++
			} else {
				require.Equal(t, 0, len(entries))
				require.Equal(t, 1, len(files))
				require.Equal(t, uint64(100), files[0].MinTs)
				require.Equal(t, stream.DefaultCF, cf)
			}
			require.Equal(t, uint64(math.MaxUint64), filterTS)
			return nil, nil
		},
	)
	require.Nil(t, err)
	require.Equal(t, batchCount, 1)
}

func TestRestoreMetaKVFilesWithBatchMethod2_write_empty_2(t *testing.T) {
	files_default := []*backuppb.DataFileInfo{
		{
			Path:   "f1",
			MinTs:  100,
			MaxTs:  120,
			Length: restore.MetaKVBatchSize - 1000,
		},
		{
			Path:   "f2",
			MinTs:  110,
			MaxTs:  1100,
			Length: restore.MetaKVBatchSize,
		},
	}
	files_write := []*backuppb.DataFileInfo{}
	emptyCount := 0
	batchCount := 0

	client := restore.MockClient(nil)
	sr := MockEmptySchemasReplace()
	err := client.RestoreMetaKVFilesWithBatchMethod(
		context.Background(),
		files_default,
		files_write,
		sr,
		nil,
		nil,
		func(
			ctx context.Context,
			files []*backuppb.DataFileInfo,
			schemasReplace *stream.SchemasReplace,
			entries []*restore.KvEntryWithTS,
			filterTS uint64,
			updateStats func(kvCount uint64, size uint64),
			progressInc func(),
			cf string,
		) ([]*restore.KvEntryWithTS, error) {
			if len(entries) == 0 && len(files) == 0 {
				// write - write
				require.Equal(t, stream.WriteCF, cf)
				emptyCount++
				if emptyCount == 1 {
					require.Equal(t, uint64(110), filterTS)
				} else {
					require.Equal(t, uint64(math.MaxUint64), filterTS)
				}
			} else {
				// default - default
				batchCount++
				require.Equal(t, 1, len(files))
				require.Equal(t, stream.DefaultCF, cf)
				if batchCount == 1 {
					require.Equal(t, uint64(100), files[0].MinTs)
					require.Equal(t, uint64(110), filterTS)
					return nil, nil
				}
				require.Equal(t, 0, len(entries))
			}
			return nil, nil
		},
	)
	require.Nil(t, err)
	require.Equal(t, batchCount, 2)
	require.Equal(t, emptyCount, 2)
}

func TestRestoreMetaKVFilesWithBatchMethod_with_entries(t *testing.T) {
	files_default := []*backuppb.DataFileInfo{
		{
			Path:   "f1",
			MinTs:  100,
			MaxTs:  120,
			Length: restore.MetaKVBatchSize - 1000,
		},
		{
			Path:   "f2",
			MinTs:  110,
			MaxTs:  1100,
			Length: restore.MetaKVBatchSize,
		},
	}
	files_write := []*backuppb.DataFileInfo{}
	emptyCount := 0
	batchCount := 0

	client := restore.MockClient(nil)
	sr := MockEmptySchemasReplace()
	err := client.RestoreMetaKVFilesWithBatchMethod(
		context.Background(),
		files_default,
		files_write,
		sr,
		nil,
		nil,
		func(
			ctx context.Context,
			files []*backuppb.DataFileInfo,
			schemasReplace *stream.SchemasReplace,
			entries []*restore.KvEntryWithTS,
			filterTS uint64,
			updateStats func(kvCount uint64, size uint64),
			progressInc func(),
			cf string,
		) ([]*restore.KvEntryWithTS, error) {
			if len(entries) == 0 && len(files) == 0 {
				// write - write
				require.Equal(t, stream.WriteCF, cf)
				emptyCount++
				if emptyCount == 1 {
					require.Equal(t, uint64(110), filterTS)
				} else {
					require.Equal(t, uint64(math.MaxUint64), filterTS)
				}
			} else {
				// default - default
				batchCount++
				require.Equal(t, 1, len(files))
				require.Equal(t, stream.DefaultCF, cf)
				if batchCount == 1 {
					require.Equal(t, uint64(100), files[0].MinTs)
					require.Equal(t, uint64(110), filterTS)
					return nil, nil
				}
				require.Equal(t, 0, len(entries))
			}
			return nil, nil
		},
	)
	require.Nil(t, err)
	require.Equal(t, batchCount, 2)
	require.Equal(t, emptyCount, 2)
}

func TestRestoreMetaKVFilesWithBatchMethod3(t *testing.T) {
	defaultFiles := []*backuppb.DataFileInfo{
		{
			Path:  "f1",
			MinTs: 100,
			MaxTs: 120,
		},
		{
			Path:  "f2",
			MinTs: 100,
			MaxTs: 120,
		},
		{
			Path:  "f3",
			MinTs: 110,
			MaxTs: 130,
		},
		{
			Path:  "f4",
			MinTs: 140,
			MaxTs: 150,
		},
		{
			Path:  "f5",
			MinTs: 150,
			MaxTs: 160,
		},
	}
	writeFiles := []*backuppb.DataFileInfo{
		{
			Path:  "f1",
			MinTs: 100,
			MaxTs: 120,
		},
		{
			Path:  "f2",
			MinTs: 100,
			MaxTs: 120,
		},
		{
			Path:  "f3",
			MinTs: 110,
			MaxTs: 130,
		},
		{
			Path:  "f4",
			MinTs: 135,
			MaxTs: 150,
		},
		{
			Path:  "f5",
			MinTs: 150,
			MaxTs: 160,
		},
	}

	batchCount := 0
	result := make(map[int][]*backuppb.DataFileInfo)
	resultKV := make(map[int]int)

	client := restore.MockClient(nil)
	sr := MockEmptySchemasReplace()
	err := client.RestoreMetaKVFilesWithBatchMethod(
		context.Background(),
		defaultFiles,
		writeFiles,
		sr,
		nil,
		nil,
		func(
			ctx context.Context,
			fs []*backuppb.DataFileInfo,
			schemasReplace *stream.SchemasReplace,
			entries []*restore.KvEntryWithTS,
			filterTS uint64,
			updateStats func(kvCount uint64, size uint64),
			progressInc func(),
			cf string,
		) ([]*restore.KvEntryWithTS, error) {
			result[batchCount] = fs
			t.Log(filterTS)
			resultKV[batchCount] = len(entries)
			batchCount++
			return make([]*restore.KvEntryWithTS, batchCount), nil
		},
	)
	require.Nil(t, err)
	require.Equal(t, len(result), 4)
	require.Equal(t, result[0], defaultFiles[0:3])
	require.Equal(t, resultKV[0], 0)
	require.Equal(t, result[1], writeFiles[0:4])
	require.Equal(t, resultKV[1], 0)
	require.Equal(t, result[2], defaultFiles[3:])
	require.Equal(t, resultKV[2], 1)
	require.Equal(t, result[3], writeFiles[4:])
	require.Equal(t, resultKV[3], 2)
}

func TestRestoreMetaKVFilesWithBatchMethod4(t *testing.T) {
	defaultFiles := []*backuppb.DataFileInfo{
		{
			Path:  "f1",
			MinTs: 100,
			MaxTs: 100,
		},
		{
			Path:  "f2",
			MinTs: 100,
			MaxTs: 100,
		},
		{
			Path:  "f3",
			MinTs: 110,
			MaxTs: 130,
		},
		{
			Path:  "f4",
			MinTs: 110,
			MaxTs: 150,
		},
	}

	writeFiles := []*backuppb.DataFileInfo{
		{
			Path:  "f1",
			MinTs: 100,
			MaxTs: 100,
		},
		{
			Path:  "f2",
			MinTs: 100,
			MaxTs: 100,
		},
		{
			Path:  "f3",
			MinTs: 110,
			MaxTs: 130,
		},
		{
			Path:  "f4",
			MinTs: 110,
			MaxTs: 150,
		},
	}
	batchCount := 0
	result := make(map[int][]*backuppb.DataFileInfo)

	client := restore.MockClient(nil)
	sr := MockEmptySchemasReplace()
	err := client.RestoreMetaKVFilesWithBatchMethod(
		context.Background(),
		defaultFiles,
		writeFiles,
		sr,
		nil,
		nil,
		func(
			ctx context.Context,
			fs []*backuppb.DataFileInfo,
			schemasReplace *stream.SchemasReplace,
			entries []*restore.KvEntryWithTS,
			filterTS uint64,
			updateStats func(kvCount uint64, size uint64),
			progressInc func(),
			cf string,
		) ([]*restore.KvEntryWithTS, error) {
			result[batchCount] = fs
			batchCount++
			return nil, nil
		},
	)
	require.Nil(t, err)
	require.Equal(t, len(result), 4)
	require.Equal(t, result[0], defaultFiles[0:2])
	require.Equal(t, result[1], writeFiles[0:2])
	require.Equal(t, result[2], defaultFiles[2:])
	require.Equal(t, result[3], writeFiles[2:])
}

func TestRestoreMetaKVFilesWithBatchMethod5(t *testing.T) {
	defaultFiles := []*backuppb.DataFileInfo{
		{
			Path:  "f1",
			MinTs: 100,
			MaxTs: 100,
		},
		{
			Path:  "f2",
			MinTs: 100,
			MaxTs: 100,
		},
		{
			Path:  "f3",
			MinTs: 110,
			MaxTs: 130,
		},
		{
			Path:  "f4",
			MinTs: 110,
			MaxTs: 150,
		},
	}

	writeFiles := []*backuppb.DataFileInfo{
		{
			Path:  "f1",
			MinTs: 100,
			MaxTs: 100,
		},
		{
			Path:  "f2",
			MinTs: 100,
			MaxTs: 100,
		},
		{
			Path:  "f3",
			MinTs: 100,
			MaxTs: 130,
		},
		{
			Path:  "f4",
			MinTs: 100,
			MaxTs: 150,
		},
	}
	batchCount := 0
	result := make(map[int][]*backuppb.DataFileInfo)

	client := restore.MockClient(nil)
	sr := MockEmptySchemasReplace()
	err := client.RestoreMetaKVFilesWithBatchMethod(
		context.Background(),
		defaultFiles,
		writeFiles,
		sr,
		nil,
		nil,
		func(
			ctx context.Context,
			fs []*backuppb.DataFileInfo,
			schemasReplace *stream.SchemasReplace,
			entries []*restore.KvEntryWithTS,
			filterTS uint64,
			updateStats func(kvCount uint64, size uint64),
			progressInc func(),
			cf string,
		) ([]*restore.KvEntryWithTS, error) {
			result[batchCount] = fs
			batchCount++
			return nil, nil
		},
	)
	require.Nil(t, err)
	require.Equal(t, len(result), 4)
	require.Equal(t, result[0], defaultFiles[0:2])
	require.Equal(t, result[1], writeFiles[0:])
	require.Equal(t, result[2], defaultFiles[2:])
	require.Equal(t, len(result[3]), 0)
}

func TestRestoreMetaKVFilesWithBatchMethod6(t *testing.T) {
	defaultFiles := []*backuppb.DataFileInfo{
		{
			Path:   "f1",
			MinTs:  100,
			MaxTs:  120,
			Length: 100,
		},
		{
			Path:   "f2",
			MinTs:  100,
			MaxTs:  120,
			Length: restore.MetaKVBatchSize - 100,
		},
		{
			Path:   "f3",
			MinTs:  110,
			MaxTs:  130,
			Length: 1,
		},
		{
			Path:   "f4",
			MinTs:  140,
			MaxTs:  150,
			Length: 1,
		},
		{
			Path:   "f5",
			MinTs:  150,
			MaxTs:  160,
			Length: 1,
		},
	}

	writeFiles := []*backuppb.DataFileInfo{
		{
			Path:  "f1",
			MinTs: 100,
			MaxTs: 120,
		},
		{
			Path:  "f2",
			MinTs: 100,
			MaxTs: 120,
		},
		{
			Path:  "f3",
			MinTs: 110,
			MaxTs: 140,
		},
		{
			Path:  "f4",
			MinTs: 120,
			MaxTs: 150,
		},
		{
			Path:  "f5",
			MinTs: 140,
			MaxTs: 160,
		},
	}

	batchCount := 0
	result := make(map[int][]*backuppb.DataFileInfo)
	resultKV := make(map[int]int)

	client := restore.MockClient(nil)
	sr := MockEmptySchemasReplace()
	err := client.RestoreMetaKVFilesWithBatchMethod(
		context.Background(),
		defaultFiles,
		writeFiles,
		sr,
		nil,
		nil,
		func(
			ctx context.Context,
			fs []*backuppb.DataFileInfo,
			schemasReplace *stream.SchemasReplace,
			entries []*restore.KvEntryWithTS,
			filterTS uint64,
			updateStats func(kvCount uint64, size uint64),
			progressInc func(),
			cf string,
		) ([]*restore.KvEntryWithTS, error) {
			result[batchCount] = fs
			t.Log(filterTS)
			resultKV[batchCount] = len(entries)
			batchCount++
			return make([]*restore.KvEntryWithTS, batchCount), nil
		},
	)
	require.Nil(t, err)
	require.Equal(t, len(result), 6)
	require.Equal(t, result[0], defaultFiles[0:2])
	require.Equal(t, resultKV[0], 0)
	require.Equal(t, result[1], writeFiles[0:2])
	require.Equal(t, resultKV[1], 0)
	require.Equal(t, result[2], defaultFiles[2:3])
	require.Equal(t, resultKV[2], 1)
	require.Equal(t, result[3], writeFiles[2:4])
	require.Equal(t, resultKV[3], 2)
	require.Equal(t, result[4], defaultFiles[3:])
	require.Equal(t, resultKV[4], 3)
	require.Equal(t, result[5], writeFiles[4:])
	require.Equal(t, resultKV[5], 4)
}

func TestSortMetaKVFiles(t *testing.T) {
	files := []*backuppb.DataFileInfo{
		{
			Path:       "f5",
			MinTs:      110,
			MaxTs:      150,
			ResolvedTs: 120,
		},
		{
			Path:       "f1",
			MinTs:      100,
			MaxTs:      100,
			ResolvedTs: 80,
		},
		{
			Path:       "f2",
			MinTs:      100,
			MaxTs:      100,
			ResolvedTs: 90,
		},
		{
			Path:       "f4",
			MinTs:      110,
			MaxTs:      130,
			ResolvedTs: 120,
		},
		{
			Path:       "f3",
			MinTs:      105,
			MaxTs:      130,
			ResolvedTs: 100,
		},
	}

	files = restore.SortMetaKVFiles(files)
	require.Equal(t, len(files), 5)
	require.Equal(t, files[0].Path, "f1")
	require.Equal(t, files[1].Path, "f2")
	require.Equal(t, files[2].Path, "f3")
	require.Equal(t, files[3].Path, "f4")
	require.Equal(t, files[4].Path, "f5")
}

func toLogDataFileInfoIter(logIter iter.TryNextor[*backuppb.DataFileInfo]) restore.LogIter {
	return iter.Map(logIter, func(d *backuppb.DataFileInfo) *restore.LogDataFileInfo {
		return &restore.LogDataFileInfo{
			DataFileInfo: d,
		}
	})
}

func TestApplyKVFilesWithSingelMethod(t *testing.T) {
	var (
		totalKVCount int64  = 0
		totalSize    uint64 = 0
		logs                = make([]string, 0)
	)
	ds := []*backuppb.DataFileInfo{
		{
			Path:            "log3",
			NumberOfEntries: 5,
			Length:          100,
			Cf:              stream.WriteCF,
			Type:            backuppb.FileType_Delete,
		},
		{
			Path:            "log1",
			NumberOfEntries: 5,
			Length:          100,
			Cf:              stream.DefaultCF,
			Type:            backuppb.FileType_Put,
		}, {
			Path:            "log2",
			NumberOfEntries: 5,
			Length:          100,
			Cf:              stream.WriteCF,
			Type:            backuppb.FileType_Put,
		},
	}
	var applyWg sync.WaitGroup
	applyFunc := func(
		files []*restore.LogDataFileInfo,
		kvCount int64,
		size uint64,
	) {
		totalKVCount += kvCount
		totalSize += size
		for _, f := range files {
			logs = append(logs, f.GetPath())
		}
	}

	restore.ApplyKVFilesWithSingelMethod(
		context.TODO(),
		toLogDataFileInfoIter(iter.FromSlice(ds)),
		applyFunc,
		&applyWg,
	)

	require.Equal(t, totalKVCount, int64(15))
	require.Equal(t, totalSize, uint64(300))
	require.Equal(t, logs, []string{"log1", "log2", "log3"})
}

func TestApplyKVFilesWithBatchMethod1(t *testing.T) {
	var (
		runCount            = 0
		batchCount   int    = 3
		batchSize    uint64 = 1000
		totalKVCount int64  = 0
		logs                = make([][]string, 0)
	)
	ds := []*backuppb.DataFileInfo{
		{
			Path:            "log5",
			NumberOfEntries: 5,
			Length:          100,
			Cf:              stream.WriteCF,
			Type:            backuppb.FileType_Delete,
			RegionId:        1,
		}, {
			Path:            "log3",
			NumberOfEntries: 5,
			Length:          100,
			Cf:              stream.WriteCF,
			Type:            backuppb.FileType_Put,
			RegionId:        1,
		}, {
			Path:            "log4",
			NumberOfEntries: 5,
			Length:          100,
			Cf:              stream.WriteCF,
			Type:            backuppb.FileType_Put,
			RegionId:        1,
		}, {
			Path:            "log1",
			NumberOfEntries: 5,
			Length:          800,
			Cf:              stream.DefaultCF,
			Type:            backuppb.FileType_Put,
			RegionId:        1,
		},
		{
			Path:            "log2",
			NumberOfEntries: 5,
			Length:          200,
			Cf:              stream.DefaultCF,
			Type:            backuppb.FileType_Put,
			RegionId:        1,
		},
	}
	var applyWg sync.WaitGroup
	applyFunc := func(
		files []*restore.LogDataFileInfo,
		kvCount int64,
		size uint64,
	) {
		runCount += 1
		totalKVCount += kvCount
		log := make([]string, 0, len(files))
		for _, f := range files {
			log = append(log, f.GetPath())
		}
		logs = append(logs, log)
	}

	restore.ApplyKVFilesWithBatchMethod(
		context.TODO(),
		toLogDataFileInfoIter(iter.FromSlice(ds)),
		batchCount,
		batchSize,
		applyFunc,
		&applyWg,
	)

	require.Equal(t, runCount, 3)
	require.Equal(t, totalKVCount, int64(25))
	require.Equal(t,
		logs,
		[][]string{
			{"log1", "log2"},
			{"log3", "log4"},
			{"log5"},
		},
	)
}

func TestApplyKVFilesWithBatchMethod2(t *testing.T) {
	var (
		runCount            = 0
		batchCount   int    = 2
		batchSize    uint64 = 1500
		totalKVCount int64  = 0
		logs                = make([][]string, 0)
	)
	ds := []*backuppb.DataFileInfo{
		{
			Path:            "log1",
			NumberOfEntries: 5,
			Length:          100,
			Cf:              stream.WriteCF,
			Type:            backuppb.FileType_Delete,
			RegionId:        1,
		}, {
			Path:            "log2",
			NumberOfEntries: 5,
			Length:          100,
			Cf:              stream.WriteCF,
			Type:            backuppb.FileType_Put,
			RegionId:        1,
		}, {
			Path:            "log3",
			NumberOfEntries: 5,
			Length:          100,
			Cf:              stream.WriteCF,
			Type:            backuppb.FileType_Put,
			RegionId:        1,
		}, {
			Path:            "log4",
			NumberOfEntries: 5,
			Length:          100,
			Cf:              stream.WriteCF,
			Type:            backuppb.FileType_Put,
			RegionId:        1,
		}, {
			Path:            "log5",
			NumberOfEntries: 5,
			Length:          800,
			Cf:              stream.DefaultCF,
			Type:            backuppb.FileType_Put,
			RegionId:        1,
		},
		{
			Path:            "log6",
			NumberOfEntries: 5,
			Length:          200,
			Cf:              stream.DefaultCF,
			Type:            backuppb.FileType_Put,
			RegionId:        1,
		},
	}
	var applyWg sync.WaitGroup
	applyFunc := func(
		files []*restore.LogDataFileInfo,
		kvCount int64,
		size uint64,
	) {
		runCount += 1
		totalKVCount += kvCount
		log := make([]string, 0, len(files))
		for _, f := range files {
			log = append(log, f.GetPath())
		}
		logs = append(logs, log)
	}

	restore.ApplyKVFilesWithBatchMethod(
		context.TODO(),
		toLogDataFileInfoIter(iter.FromSlice(ds)),
		batchCount,
		batchSize,
		applyFunc,
		&applyWg,
	)

	require.Equal(t, runCount, 4)
	require.Equal(t, totalKVCount, int64(30))
	require.Equal(t,
		logs,
		[][]string{
			{"log2", "log3"},
			{"log5", "log6"},
			{"log4"},
			{"log1"},
		},
	)
}

func TestApplyKVFilesWithBatchMethod3(t *testing.T) {
	var (
		runCount            = 0
		batchCount   int    = 2
		batchSize    uint64 = 1500
		totalKVCount int64  = 0
		logs                = make([][]string, 0)
	)
	ds := []*backuppb.DataFileInfo{
		{
			Path:            "log1",
			NumberOfEntries: 5,
			Length:          2000,
			Cf:              stream.WriteCF,
			Type:            backuppb.FileType_Delete,
			RegionId:        1,
		}, {
			Path:            "log2",
			NumberOfEntries: 5,
			Length:          2000,
			Cf:              stream.WriteCF,
			Type:            backuppb.FileType_Put,
			RegionId:        1,
		}, {
			Path:            "log3",
			NumberOfEntries: 5,
			Length:          100,
			Cf:              stream.WriteCF,
			Type:            backuppb.FileType_Put,
			RegionId:        1,
		}, {
			Path:            "log5",
			NumberOfEntries: 5,
			Length:          800,
			Cf:              stream.DefaultCF,
			Type:            backuppb.FileType_Put,
			RegionId:        3,
		},
		{
			Path:            "log6",
			NumberOfEntries: 5,
			Length:          200,
			Cf:              stream.DefaultCF,
			Type:            backuppb.FileType_Put,
			RegionId:        3,
		},
	}
	var applyWg sync.WaitGroup
	applyFunc := func(
		files []*restore.LogDataFileInfo,
		kvCount int64,
		size uint64,
	) {
		runCount += 1
		totalKVCount += kvCount
		log := make([]string, 0, len(files))
		for _, f := range files {
			log = append(log, f.GetPath())
		}
		logs = append(logs, log)
	}

	restore.ApplyKVFilesWithBatchMethod(
		context.TODO(),
		toLogDataFileInfoIter(iter.FromSlice(ds)),
		batchCount,
		batchSize,
		applyFunc,
		&applyWg,
	)

	require.Equal(t, totalKVCount, int64(25))
	require.Equal(t,
		logs,
		[][]string{
			{"log2"},
			{"log5", "log6"},
			{"log3"},
			{"log1"},
		},
	)
}

func TestApplyKVFilesWithBatchMethod4(t *testing.T) {
	var (
		runCount            = 0
		batchCount   int    = 2
		batchSize    uint64 = 1500
		totalKVCount int64  = 0
		logs                = make([][]string, 0)
	)
	ds := []*backuppb.DataFileInfo{
		{
			Path:            "log1",
			NumberOfEntries: 5,
			Length:          2000,
			Cf:              stream.WriteCF,
			Type:            backuppb.FileType_Delete,
			TableId:         1,
		}, {
			Path:            "log2",
			NumberOfEntries: 5,
			Length:          100,
			Cf:              stream.WriteCF,
			Type:            backuppb.FileType_Put,
			TableId:         1,
		}, {
			Path:            "log3",
			NumberOfEntries: 5,
			Length:          100,
			Cf:              stream.WriteCF,
			Type:            backuppb.FileType_Put,
			TableId:         2,
		}, {
			Path:            "log4",
			NumberOfEntries: 5,
			Length:          100,
			Cf:              stream.WriteCF,
			Type:            backuppb.FileType_Put,
			TableId:         1,
		}, {
			Path:            "log5",
			NumberOfEntries: 5,
			Length:          100,
			Cf:              stream.DefaultCF,
			Type:            backuppb.FileType_Put,
			TableId:         2,
		},
	}
	var applyWg sync.WaitGroup
	applyFunc := func(
		files []*restore.LogDataFileInfo,
		kvCount int64,
		size uint64,
	) {
		runCount += 1
		totalKVCount += kvCount
		log := make([]string, 0, len(files))
		for _, f := range files {
			log = append(log, f.GetPath())
		}
		logs = append(logs, log)
	}

	restore.ApplyKVFilesWithBatchMethod(
		context.TODO(),
		toLogDataFileInfoIter(iter.FromSlice(ds)),
		batchCount,
		batchSize,
		applyFunc,
		&applyWg,
	)

	require.Equal(t, runCount, 4)
	require.Equal(t, totalKVCount, int64(25))
	require.Equal(t,
		logs,
		[][]string{
			{"log2", "log4"},
			{"log5"},
			{"log3"},
			{"log1"},
		},
	)
}

func TestApplyKVFilesWithBatchMethod5(t *testing.T) {
	var lock sync.Mutex
	types := make([]backuppb.FileType, 0)
	ds := []*backuppb.DataFileInfo{
		{
			Path:            "log1",
			NumberOfEntries: 5,
			Length:          2000,
			Cf:              stream.WriteCF,
			Type:            backuppb.FileType_Delete,
			TableId:         1,
		}, {
			Path:            "log2",
			NumberOfEntries: 5,
			Length:          100,
			Cf:              stream.WriteCF,
			Type:            backuppb.FileType_Put,
			TableId:         1,
		}, {
			Path:            "log3",
			NumberOfEntries: 5,
			Length:          100,
			Cf:              stream.WriteCF,
			Type:            backuppb.FileType_Put,
			TableId:         2,
		}, {
			Path:            "log4",
			NumberOfEntries: 5,
			Length:          100,
			Cf:              stream.WriteCF,
			Type:            backuppb.FileType_Put,
			TableId:         1,
		}, {
			Path:            "log5",
			NumberOfEntries: 5,
			Length:          100,
			Cf:              stream.DefaultCF,
			Type:            backuppb.FileType_Put,
			TableId:         2,
		},
	}
	var applyWg sync.WaitGroup
	applyFunc := func(
		files []*restore.LogDataFileInfo,
		kvCount int64,
		size uint64,
	) {
		if len(files) == 0 {
			return
		}
		applyWg.Add(1)
		go func() {
			defer applyWg.Done()
			if files[0].Type == backuppb.FileType_Put {
				time.Sleep(time.Second)
			}
			lock.Lock()
			types = append(types, files[0].Type)
			lock.Unlock()
		}()
	}

	restore.ApplyKVFilesWithBatchMethod(
		context.TODO(),
		toLogDataFileInfoIter(iter.FromSlice(ds)),
		2,
		1500,
		applyFunc,
		&applyWg,
	)

	applyWg.Wait()
	require.Equal(t, backuppb.FileType_Delete, types[len(types)-1])

	types = make([]backuppb.FileType, 0)
	restore.ApplyKVFilesWithSingelMethod(
		context.TODO(),
		toLogDataFileInfoIter(iter.FromSlice(ds)),
		applyFunc,
		&applyWg,
	)

	applyWg.Wait()
	require.Equal(t, backuppb.FileType_Delete, types[len(types)-1])
}

func TestCheckNewCollationEnable(t *testing.T) {
	caseList := []struct {
		backupMeta                  *backuppb.BackupMeta
		newCollationEnableInCluster string
		CheckRequirements           bool
		isErr                       bool
	}{
		{
			backupMeta:                  &backuppb.BackupMeta{NewCollationsEnabled: "True"},
			newCollationEnableInCluster: "True",
			CheckRequirements:           true,
			isErr:                       false,
		},
		{
			backupMeta:                  &backuppb.BackupMeta{NewCollationsEnabled: "True"},
			newCollationEnableInCluster: "False",
			CheckRequirements:           true,
			isErr:                       true,
		},
		{
			backupMeta:                  &backuppb.BackupMeta{NewCollationsEnabled: "False"},
			newCollationEnableInCluster: "True",
			CheckRequirements:           true,
			isErr:                       true,
		},
		{
			backupMeta:                  &backuppb.BackupMeta{NewCollationsEnabled: "False"},
			newCollationEnableInCluster: "false",
			CheckRequirements:           true,
			isErr:                       false,
		},
		{
			backupMeta:                  &backuppb.BackupMeta{NewCollationsEnabled: "False"},
			newCollationEnableInCluster: "True",
			CheckRequirements:           false,
			isErr:                       true,
		},
		{
			backupMeta:                  &backuppb.BackupMeta{NewCollationsEnabled: "True"},
			newCollationEnableInCluster: "False",
			CheckRequirements:           false,
			isErr:                       true,
		},
		{
			backupMeta:                  &backuppb.BackupMeta{NewCollationsEnabled: ""},
			newCollationEnableInCluster: "True",
			CheckRequirements:           false,
			isErr:                       false,
		},
		{
			backupMeta:                  &backuppb.BackupMeta{NewCollationsEnabled: ""},
			newCollationEnableInCluster: "True",
			CheckRequirements:           true,
			isErr:                       true,
		},
		{
			backupMeta:                  &backuppb.BackupMeta{NewCollationsEnabled: ""},
			newCollationEnableInCluster: "False",
			CheckRequirements:           false,
			isErr:                       false,
		},
	}

	for i, ca := range caseList {
		g := &gluetidb.MockGlue{
			GlobalVars: map[string]string{"new_collation_enabled": ca.newCollationEnableInCluster},
		}
		enabled, err := restore.CheckNewCollationEnable(ca.backupMeta.GetNewCollationsEnabled(), g, nil, ca.CheckRequirements)
		t.Logf("[%d] Got Error: %v\n", i, err)
		if ca.isErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
		require.Equal(t, ca.newCollationEnableInCluster == "True", enabled)
	}
}
