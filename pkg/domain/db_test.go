// Copyright 2019 PingCAP, Inc.
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

package domain_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"github.com/stretchr/testify/require"
)

func TestDomainSession(t *testing.T) {
	lease := 50 * time.Millisecond
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()
	session.SetSchemaLease(lease)
	domain, err := session.BootstrapSession(store)
	require.NoError(t, err)
	ddl.DisableTiFlashPoll(domain.DDL())
	defer domain.Close()

	// for NotifyUpdatePrivilege
	createRoleSQL := `CREATE ROLE 'test'@'localhost';`
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	_, err = se.Execute(context.Background(), createRoleSQL)
	require.NoError(t, err)

	// for BindHandle
	_, err = se.Execute(context.Background(), "use test")
	require.NoError(t, err)
	_, err = se.Execute(context.Background(), "drop table if exists t")
	require.NoError(t, err)
	_, err = se.Execute(context.Background(), "create table t(i int, s varchar(20), index index_t(i, s))")
	require.NoError(t, err)
	_, err = se.Execute(context.Background(), "create global binding for select * from t where i>100 using select * from t use index(index_t) where i>100")
	require.NoError(t, err)
}

func TestNormalSessionPool(t *testing.T) {
	lease := 100 * time.Millisecond
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()
	session.SetSchemaLease(lease)
	domain, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer domain.Close()
	info, err1 := infosync.GlobalInfoSyncerInit(context.Background(), "t", func() uint64 { return 1 }, nil, nil, nil, nil, keyspace.CodecV1, true, domain.InfoCache())
	require.NoError(t, err1)
	conf := config.GetGlobalConfig()
	conf.Socket = ""
	conf.Port = 0
	conf.Status.ReportStatus = false
	svr, err := server.NewServer(conf, nil)
	require.NoError(t, err)
	svr.SetDomain(domain)
	info.SetSessionManager(svr)

	pool := domain.SysSessionPool()
	se, err := pool.Get()
	require.NoError(t, err)
	require.NotEmpty(t, se)
	require.Equal(t, svr.InternalSessionExists(se), true)

	pool.Put(se)
	require.Equal(t, svr.InternalSessionExists(se), false)
}

func TestAbnormalSessionPool(t *testing.T) {
	lease := 100 * time.Millisecond
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()
	session.SetSchemaLease(lease)
	domain, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer domain.Close()
	info, err1 := infosync.GlobalInfoSyncerInit(context.Background(), "t", func() uint64 { return 1 }, nil, nil, nil, nil, keyspace.CodecV1, true, domain.InfoCache())
	require.NoError(t, err1)
	conf := config.GetGlobalConfig()
	conf.Socket = ""
	conf.Port = 0
	conf.Status.ReportStatus = false
	svr, err := server.NewServer(conf, nil)
	require.NoError(t, err)
	svr.SetDomain(domain)
	info.SetSessionManager(svr)

	pool := domain.SysSessionPool()
	failpoint.Enable("github.com/pingcap/tidb/pkg/util/mockSessionPoolReturnError", "return")
	se, err := pool.Get()
	require.Error(t, err)
	failpoint.Disable("github.com/pingcap/tidb/pkg/util/mockSessionPoolReturnError")
	require.Equal(t, svr.InternalSessionExists(se), false)
}

func TestTetchAllSchemasWithTables(t *testing.T) {
	lease := 100 * time.Millisecond
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()
	session.SetSchemaLease(lease)
	domain, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer domain.Close()

	snapshot := store.GetSnapshot(kv.NewVersion(mathutil.MaxUint))
	m := meta.NewReader(snapshot)
	dbs, err := domain.FetchAllSchemasWithTables(m)
	require.NoError(t, err)
	require.Equal(t, len(dbs), 3)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database test1")
	tk.MustExec("use test1")
	tk.MustExec("create table t1(i int, s varchar(20), index index_t(i, s))")
	tk.MustExec("create table t2(i int, s varchar(20), index index_t(i, s))")
	tk.MustExec("create database test2")
	dbs, err = domain.FetchAllSchemasWithTables(m)
	require.NoError(t, err)
	require.Equal(t, len(dbs), 5)
}

func TestFetchAllSchemasWithTablesWithFailpoint(t *testing.T) {
	lease := 100 * time.Millisecond
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()
	session.SetSchemaLease(lease)
	domain, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer domain.Close()

	snapshot := store.GetSnapshot(kv.NewVersion(mathutil.MaxUint))
	m := meta.NewReader(snapshot)
	dbs, err := domain.FetchAllSchemasWithTables(m)
	require.NoError(t, err)
	require.Equal(t, len(dbs), 3)

	tk := testkit.NewTestKit(t, store)

	for i := 1; i <= 1000; i++ {
		dbName := fmt.Sprintf("test_%d", i)
		tk.MustExec("create database " + dbName)
	}
	vardef.SchemaCacheSize.Store(1000000)

	dbs, err = domain.FetchAllSchemasWithTables(m)
	require.NoError(t, err)
	require.Equal(t, len(dbs), 1003)

	// inject the failpoint
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/domain/failed-fetch-schemas-with-tables", "return()"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/domain/failed-fetch-schemas-with-tables"))
	}()
	dbs, err = domain.FetchAllSchemasWithTables(m)
	require.Error(t, err)
	require.Equal(t, err.Error(), "failpoint: failed to fetch schemas with tables")
	require.Nil(t, dbs)
}
