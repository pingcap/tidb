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
	"fmt"
	"math"
	"slices"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/gluetidb"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/mock"
	importclient "github.com/pingcap/tidb/br/pkg/restore/internal/import_client"
	snapclient "github.com/pingcap/tidb/br/pkg/restore/snap_client"
	"github.com/pingcap/tidb/br/pkg/utiltest"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

var mc *mock.Cluster

func TestCreateTables(t *testing.T) {
	m := mc
	g := gluetidb.New()
	client := snapclient.NewRestoreClient(m.PDClient, m.PDHTTPCli, nil, utiltest.DefaultTestKeepaliveCfg)
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
	client := snapclient.NewRestoreClient(cluster.PDClient, cluster.PDHTTPCli, nil, utiltest.DefaultTestKeepaliveCfg)
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

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/br/pkg/restore/snap_client/mock-incr-backup-data", "return(false)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/br/pkg/restore/snap_client/mock-incr-backup-data"))
	}()
	// skip check when increment backup
	require.False(t, client.NeedCheckFreshCluster(false, true))
}

func TestCheckTargetClusterFresh(t *testing.T) {
	// cannot use shared `mc`, other parallel case may change it.
	cluster := getStartedMockedCluster(t)
	defer cluster.Stop()

	g := gluetidb.New()
	client := snapclient.NewRestoreClient(cluster.PDClient, cluster.PDHTTPCli, nil, utiltest.DefaultTestKeepaliveCfg)
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
	client := snapclient.NewRestoreClient(cluster.PDClient, cluster.PDHTTPCli, nil, utiltest.DefaultTestKeepaliveCfg)
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

func TestInitFullClusterRestore(t *testing.T) {
	cluster := mc
	g := gluetidb.New()
	client := snapclient.NewRestoreClient(cluster.PDClient, cluster.PDHTTPCli, nil, utiltest.DefaultTestKeepaliveCfg)
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

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/br/pkg/restore/snap_client/mock-incr-backup-data", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/br/pkg/restore/snap_client/mock-incr-backup-data"))
	}()
	client.InitFullClusterRestore(false)
	require.False(t, client.IsFullClusterRestore())
}

// Mock ImporterClient interface
type FakeImporterClient struct {
	importclient.ImporterClient
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

func (fakeImportCli FakeImporterClient) CheckMultiIngestSupport(ctx context.Context, stores []uint64) error {
	return nil
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
	client := snapclient.NewRestoreClient(
		utiltest.NewFakePDClient(mockStores, false, nil), nil, nil, utiltest.DefaultTestKeepaliveCfg)
	ctx := context.Background()

	recordStores = NewRecordStores()
	start := time.Now()
	err := snapclient.MockCallSetSpeedLimit(ctx, FakeImporterClient{}, client, 10)
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
	client = snapclient.NewRestoreClient(
		utiltest.NewFakePDClient(mockStores, false, nil), nil, nil, utiltest.DefaultTestKeepaliveCfg)

	// Concurrency needs to be less than the number of stores
	err = snapclient.MockCallSetSpeedLimit(ctx, FakeImporterClient{}, client, 2)
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
