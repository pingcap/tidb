// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore_test

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/gluetidb"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/pingcap/tidb/br/pkg/restore"
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
	sort.Slice(r.stores, func(i, j int) bool {
		return r.stores[i] < r.stores[j]
	})
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
		return nil, fmt.Errorf("storeID:%v ERROR.", storeID)
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
	}, nil, defaultKeepaliveCfg, false)
	ctx := context.Background()

	recordStores = NewRecordStores()
	start := time.Now()
	err := restore.MockCallSetSpeedLimit(ctx, FakeImporterClient{}, client, 10)
	cost := time.Since(start)
	require.NoError(t, err)

	recordStores.sort()
	t.Logf("Total Cost: %v\n", cost)
	t.Logf("Has Communicated: %v\n", recordStores.stores)

	serialCost := len(mockStores) * WORKING_TIME
	require.Less(t, cost, time.Duration(serialCost)*time.Millisecond)
	require.Equal(t, len(mockStores), len(recordStores.stores))
	for i := 0; i < len(recordStores.stores); i++ {
		require.Equal(t, mockStores[i].Id, recordStores.stores[i])
	}

	// 2. Expect the number of communicated stores to be less than the length of the mockStore
	// Because subsequent unstarted communications are aborted when an error is encountered.
	recordStores = NewRecordStores()
	mockStores[5].Id = SET_SPEED_LIMIT_ERROR // setting a fault store
	client = restore.NewRestoreClient(fakePDClient{
		stores: mockStores,
	}, nil, defaultKeepaliveCfg, false)

	// Concurrency needs to be less than the number of stores
	err = restore.MockCallSetSpeedLimit(ctx, FakeImporterClient{}, client, 2)
	require.Error(t, err)
	t.Log(err)

	recordStores.sort()
	sort.Slice(mockStores, func(i, j int) bool { return mockStores[i].Id < mockStores[j].Id })
	t.Logf("Has Communicated: %v\n", recordStores.stores)
	require.Less(t, len(recordStores.stores), len(mockStores))
	for i := 0; i < len(recordStores.stores); i++ {
		require.Equal(t, mockStores[i].Id, recordStores.stores[i])
	}
}
