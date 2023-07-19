package testutil

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/server"
	"github.com/pingcap/tidb/server/internal/util"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

type BasicHTTPHandlerTestSuite struct {
	*TestServerClient
	server  *server.Server
	store   kv.Storage
	domain  *domain.Domain
	tidbdrv *server.TiDBDriver
	sh      http.Handler
}

func CreateBasicHTTPHandlerTestSuite() *BasicHTTPHandlerTestSuite {
	ts := &BasicHTTPHandlerTestSuite{}
	ts.TestServerClient = NewTestServerClient()
	return ts
}

func (ts *BasicHTTPHandlerTestSuite) Server() *server.Server {
	return ts.server
}

func (ts *BasicHTTPHandlerTestSuite) Store() kv.Storage {
	return ts.store
}

func (ts *BasicHTTPHandlerTestSuite) Domain() *domain.Domain {
	return ts.domain
}

func (ts *BasicHTTPHandlerTestSuite) StartServer(t *testing.T, handlerFn func(*domain.Domain) http.Handler) {
	var err error
	ts.store, err = mockstore.NewMockStore()
	require.NoError(t, err)
	ts.domain, err = session.BootstrapSession(ts.store)
	require.NoError(t, err)
	ts.tidbdrv = server.NewTiDBDriver(ts.store)

	cfg := util.NewTestConfig()
	cfg.Store = "tikv"
	cfg.Port = 0
	cfg.Status.StatusPort = 0
	cfg.Status.ReportStatus = true

	srv, err := server.NewServer(cfg, ts.tidbdrv)
	require.NoError(t, err)
	ts.SetPort(GetPortFromTCPAddr(srv.ListenerAddr()))
	ts.SetStatusPort(GetPortFromTCPAddr(srv.StatusListenerAddr()))
	ts.server = srv
	ts.server.SetDomain(ts.domain)
	go func() {
		err := srv.Run()
		require.NoError(t, err)
	}()
	ts.WaitUntilServerOnline()

	do, err := session.GetDomain(ts.store)
	require.NoError(t, err)
	if handlerFn != nil {
		ts.sh = handlerFn(do)
	}
}

func (ts *BasicHTTPHandlerTestSuite) StopServer(t *testing.T) {
	if ts.server != nil {
		ts.server.Close()
	}
	if ts.domain != nil {
		ts.domain.Close()
	}
	if ts.store != nil {
		require.NoError(t, ts.store.Close())
	}
}

func (ts *BasicHTTPHandlerTestSuite) PrepareData(t *testing.T) {
	db, err := sql.Open("mysql", ts.GetDSN())
	require.NoError(t, err)
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()
	dbt := testkit.NewDBTestKit(t, db)

	dbt.MustExec("create database tidb;")
	dbt.MustExec("use tidb;")
	dbt.MustExec("create table tidb.test (a int auto_increment primary key, b varchar(20));")
	dbt.MustExec("insert tidb.test values (1, 1);")
	txn1, err := dbt.GetDB().Begin()
	require.NoError(t, err)
	_, err = txn1.Exec("update tidb.test set b = b + 1 where a = 1;")
	require.NoError(t, err)
	_, err = txn1.Exec("insert tidb.test values (2, 2);")
	require.NoError(t, err)
	_, err = txn1.Exec("insert tidb.test (a) values (3);")
	require.NoError(t, err)
	_, err = txn1.Exec("insert tidb.test values (4, '');")
	require.NoError(t, err)
	err = txn1.Commit()
	require.NoError(t, err)
	dbt.MustExec("alter table tidb.test add index idx1 (a, b);")
	dbt.MustExec("alter table tidb.test drop index idx1;")
	dbt.MustExec("alter table tidb.test add index idx1 (a, b);")
	dbt.MustExec("alter table tidb.test add unique index idx2 (a, b);")

	dbt.MustExec(`create table tidb.pt (a int primary key, b varchar(20), key idx(a, b))
partition by range (a)
(partition p0 values less than (256),
 partition p1 values less than (512),
 partition p2 values less than (1024))`)

	txn2, err := dbt.GetDB().Begin()
	require.NoError(t, err)
	_, err = txn2.Exec("insert into tidb.pt values (42, '123')")
	require.NoError(t, err)
	_, err = txn2.Exec("insert into tidb.pt values (256, 'b')")
	require.NoError(t, err)
	_, err = txn2.Exec("insert into tidb.pt values (666, 'def')")
	require.NoError(t, err)
	err = txn2.Commit()
	require.NoError(t, err)
	dbt.MustExec("drop table if exists t")
	dbt.MustExec("create table t (a double, b varchar(20), c int, primary key(a,b) clustered, key idx(c))")
	dbt.MustExec("insert into t values(1.1,'111',1),(2.2,'222',2)")
}

func (ts *BasicHTTPHandlerTestSuite) RegionContainsTable(t *testing.T, regionID uint64, tableID int64) bool {
	resp, err := ts.FetchStatus(fmt.Sprintf("/regions/%d", regionID))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	decoder := json.NewDecoder(resp.Body)
	var data server.RegionDetail
	err = decoder.Decode(&data)
	require.NoError(t, err)
	for _, index := range data.Frames {
		if index.TableID == tableID {
			return true
		}
	}
	return false
}
