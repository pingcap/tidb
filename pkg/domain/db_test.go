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
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/store/mockstore"
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
	info, err1 := infosync.GlobalInfoSyncerInit(context.Background(), "t", func() uint64 { return 1 }, nil, nil, nil, nil, keyspace.CodecV1, true)
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
	info, err1 := infosync.GlobalInfoSyncerInit(context.Background(), "t", func() uint64 { return 1 }, nil, nil, nil, nil, keyspace.CodecV1, true)
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
	failpoint.Enable("github.com/pingcap/tidb/pkg/domain/mockSessionPoolReturnError", "return")
	se, err := pool.Get()
	require.Error(t, err)
	failpoint.Disable("github.com/pingcap/tidb/pkg/domain/mockSessionPoolReturnError")
	require.Equal(t, svr.InternalSessionExists(se), false)
}
