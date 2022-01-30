// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore_test

import (
	"context"
	"math"
	"strconv"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/gluetidb"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/types"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/testleak"
	pd "github.com/tikv/pd/client"
	"google.golang.org/grpc/keepalive"
)

var _ = Suite(&testRestoreClientSuite{})

var defaultKeepaliveCfg = keepalive.ClientParameters{
	Time:    3 * time.Second,
	Timeout: 10 * time.Second,
}

type testRestoreClientSuite struct {
	mock *mock.Cluster
}

func (s *testRestoreClientSuite) SetUpTest(c *C) {
	var err error
	s.mock, err = mock.NewCluster()
	c.Assert(err, IsNil)
}

func (s *testRestoreClientSuite) TearDownTest(c *C) {
	testleak.AfterTest(c)()
}

func (s *testRestoreClientSuite) TestCreateTables(c *C) {
	c.Assert(s.mock.Start(), IsNil)
	defer s.mock.Stop()
	client, err := restore.NewRestoreClient(gluetidb.New(), s.mock.PDClient, s.mock.Storage, nil, defaultKeepaliveCfg)
	c.Assert(err, IsNil)

	info, err := s.mock.Domain.GetSnapshotInfoSchema(math.MaxUint64)
	c.Assert(err, IsNil)
	dbSchema, isExist := info.SchemaByName(model.NewCIStr("test"))
	c.Assert(isExist, IsTrue)

	tables := make([]*metautil.Table, 4)
	intField := types.NewFieldType(mysql.TypeLong)
	intField.Charset = "binary"
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
	rules, newTables, err := client.CreateTables(s.mock.Domain, tables, 0)
	c.Assert(err, IsNil)
	// make sure tables and newTables have same order
	for i, t := range tables {
		c.Assert(newTables[i].Name, Equals, t.Info.Name)
	}
	for _, nt := range newTables {
		c.Assert(nt.Name.String(), Matches, "test[0-3]")
	}
	oldTableIDExist := make(map[int64]bool)
	newTableIDExist := make(map[int64]bool)
	for _, tr := range rules.Data {
		oldTableID := tablecodec.DecodeTableID(tr.GetOldKeyPrefix())
		c.Assert(oldTableIDExist[oldTableID], IsFalse, Commentf("table rule duplicate old table id"))
		oldTableIDExist[oldTableID] = true

		newTableID := tablecodec.DecodeTableID(tr.GetNewKeyPrefix())
		c.Assert(newTableIDExist[newTableID], IsFalse, Commentf("table rule duplicate new table id"))
		newTableIDExist[newTableID] = true
	}

	for i := 0; i < len(tables); i++ {
		c.Assert(oldTableIDExist[int64(i)], IsTrue, Commentf("table rule does not exist"))
	}
}

func (s *testRestoreClientSuite) TestIsOnline(c *C) {
	c.Assert(s.mock.Start(), IsNil)
	defer s.mock.Stop()

	client, err := restore.NewRestoreClient(gluetidb.New(), s.mock.PDClient, s.mock.Storage, nil, defaultKeepaliveCfg)
	c.Assert(err, IsNil)

	c.Assert(client.IsOnline(), IsFalse)
	client.EnableOnline()
	c.Assert(client.IsOnline(), IsTrue)
}

func (s *testRestoreClientSuite) TestPreCheckTableClusterIndex(c *C) {
	c.Assert(s.mock.Start(), IsNil)
	defer s.mock.Stop()

	client, err := restore.NewRestoreClient(gluetidb.New(), s.mock.PDClient, s.mock.Storage, nil, defaultKeepaliveCfg)
	c.Assert(err, IsNil)

	info, err := s.mock.Domain.GetSnapshotInfoSchema(math.MaxUint64)
	c.Assert(err, IsNil)
	dbSchema, isExist := info.SchemaByName(model.NewCIStr("test"))
	c.Assert(isExist, IsTrue)

	tables := make([]*metautil.Table, 4)
	intField := types.NewFieldType(mysql.TypeLong)
	intField.Charset = "binary"
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
	_, _, err = client.CreateTables(s.mock.Domain, tables, 0)
	c.Assert(err, IsNil)

	// exist different tables
	tables[1].Info.IsCommonHandle = true
	c.Assert(client.PreCheckTableClusterIndex(tables, nil, s.mock.Domain),
		ErrorMatches, `.*@@tidb_enable_clustered_index should be ON \(backup table = true, created table = false\).*`)

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
	c.Assert(client.PreCheckTableClusterIndex(nil, jobs, s.mock.Domain),
		ErrorMatches, `.*@@tidb_enable_clustered_index should be ON \(backup table = true, created table = false\).*`)

	// should pass pre-check cluster index
	tables[1].Info.IsCommonHandle = false
	jobs[0].BinlogInfo.TableInfo.IsCommonHandle = false
	c.Assert(client.PreCheckTableClusterIndex(tables, jobs, s.mock.Domain), IsNil)
}

type fakePDClient struct {
	pd.Client
	stores []*metapb.Store
}

func (fpdc fakePDClient) GetAllStores(context.Context, ...pd.GetStoreOption) ([]*metapb.Store, error) {
	return append([]*metapb.Store{}, fpdc.stores...), nil
}

func (s *testRestoreClientSuite) TestPreCheckTableTiFlashReplicas(c *C) {
	c.Assert(s.mock.Start(), IsNil)
	defer s.mock.Stop()

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

	client, err := restore.NewRestoreClient(gluetidb.New(), fakePDClient{
		stores: mockStores,
	}, s.mock.Storage, nil, defaultKeepaliveCfg)
	c.Assert(err, IsNil)

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
	c.Assert(client.PreCheckTableTiFlashReplica(ctx, tables), IsNil)

	for i := 0; i < len(tables); i++ {
		if i == 0 || i > 2 {
			c.Assert(tables[i].Info.TiFlashReplica, IsNil)
		} else {
			c.Assert(tables[i].Info.TiFlashReplica, NotNil)
			obtainCount := int(tables[i].Info.TiFlashReplica.Count)
			c.Assert(obtainCount, Equals, i)
		}
	}
}
