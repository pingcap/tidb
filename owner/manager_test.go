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

package owner_test

import (
	"context"
	goctx "context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/pingcap/errors"
	. "github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/owner"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.etcd.io/etcd/tests/v3/integration"
)

const testLease = 5 * time.Millisecond

func TestSingle(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a colon which is not allowed on Windows")
	}
	integration.BeforeTest(t)

	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()

	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(t)

	client := cluster.RandClient()
	ctx := goctx.Background()
	ic := infoschema.NewCache(2)
	ic.Insert(infoschema.MockInfoSchemaWithSchemaVer(nil, 0), 0)
	d := NewDDL(
		ctx,
		WithEtcdClient(client),
		WithStore(store),
		WithLease(testLease),
		WithInfoCache(ic),
	)
	err = d.Start(nil)
	require.NoError(t, err)
	defer func() {
		_ = d.Stop()
	}()

	isOwner := checkOwner(d, true)
	require.True(t, isOwner)

	// test for newSession failed
	ctx, cancel := goctx.WithCancel(ctx)
	manager := owner.NewOwnerManager(ctx, client, "ddl", "ddl_id", DDLOwnerKey)
	cancel()

	err = manager.CampaignOwner()
	comment := fmt.Sprintf("campaigned result don't match, err %v", err)
	require.True(t, terror.ErrorEqual(err, goctx.Canceled) || terror.ErrorEqual(err, goctx.DeadlineExceeded), comment)

	isOwner = checkOwner(d, true)
	require.True(t, isOwner)

	// The test is used to exit campaign loop.
	d.OwnerManager().Cancel()
	isOwner = checkOwner(d, false)
	require.False(t, isOwner)

	time.Sleep(200 * time.Millisecond)

	// err is ok to be not nil since we canceled the manager.
	ownerID, _ := manager.GetOwnerID(goctx.Background())
	require.Equal(t, "", ownerID)
}

func TestCluster(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a colon which is not allowed on Windows")
	}
	integration.BeforeTest(t)

	originalTTL := owner.ManagerSessionTTL
	owner.ManagerSessionTTL = 3
	defer func() {
		owner.ManagerSessionTTL = originalTTL
	}()

	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()

	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 4})
	defer cluster.Terminate(t)

	cli := cluster.Client(0)
	ic := infoschema.NewCache(2)
	ic.Insert(infoschema.MockInfoSchemaWithSchemaVer(nil, 0), 0)
	d := NewDDL(
		goctx.Background(),
		WithEtcdClient(cli),
		WithStore(store),
		WithLease(testLease),
		WithInfoCache(ic),
	)

	err = d.Start(nil)
	require.NoError(t, err)

	isOwner := checkOwner(d, true)
	require.True(t, isOwner)

	cli1 := cluster.Client(1)
	ic2 := infoschema.NewCache(2)
	ic2.Insert(infoschema.MockInfoSchemaWithSchemaVer(nil, 0), 0)
	d1 := NewDDL(
		goctx.Background(),
		WithEtcdClient(cli1),
		WithStore(store),
		WithLease(testLease),
		WithInfoCache(ic2),
	)
	err = d1.Start(nil)
	require.NoError(t, err)

	isOwner = checkOwner(d1, false)
	require.False(t, isOwner)

	// Delete the leader key, the d1 become the owner.
	cliRW := cluster.Client(2)
	err = deleteLeader(cliRW, DDLOwnerKey)
	require.NoError(t, err)

	isOwner = checkOwner(d, false)
	require.False(t, isOwner)

	err = d.Stop()
	require.NoError(t, err)

	// d3 (not owner) stop
	cli3 := cluster.Client(3)
	ic3 := infoschema.NewCache(2)
	ic3.Insert(infoschema.MockInfoSchemaWithSchemaVer(nil, 0), 0)
	d3 := NewDDL(
		goctx.Background(),
		WithEtcdClient(cli3),
		WithStore(store),
		WithLease(testLease),
		WithInfoCache(ic3),
	)
	err = d3.Start(nil)
	require.NoError(t, err)
	defer func() {
		err = d3.Stop()
		require.NoError(t, err)
	}()

	isOwner = checkOwner(d3, false)
	require.False(t, isOwner)

	err = d3.Stop()
	require.NoError(t, err)

	// Cancel the owner context, there is no owner.
	err = d1.Stop()
	require.NoError(t, err)

	session, err := concurrency.NewSession(cliRW)
	require.NoError(t, err)

	election := concurrency.NewElection(session, DDLOwnerKey)
	logPrefix := fmt.Sprintf("[ddl] %s ownerManager %s", DDLOwnerKey, "useless id")
	logCtx := logutil.WithKeyValue(context.Background(), "owner info", logPrefix)
	_, err = owner.GetOwnerInfo(goctx.Background(), logCtx, election, "useless id")
	require.Truef(t, terror.ErrorEqual(err, concurrency.ErrElectionNoLeader), "get owner info result don't match, err %v", err)
}

func checkOwner(d DDL, fbVal bool) (isOwner bool) {
	manager := d.OwnerManager()
	// The longest to wait for 30 seconds to
	// make sure that campaigning owners is completed.
	for i := 0; i < 6000; i++ {
		time.Sleep(5 * time.Millisecond)
		isOwner = manager.IsOwner()
		if isOwner == fbVal {
			break
		}
	}
	return
}

func deleteLeader(cli *clientv3.Client, prefixKey string) error {
	session, err := concurrency.NewSession(cli)
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		_ = session.Close()
	}()
	election := concurrency.NewElection(session, prefixKey)
	resp, err := election.Leader(goctx.Background())
	if err != nil {
		return errors.Trace(err)
	}
	_, err = cli.Delete(goctx.Background(), string(resp.Kvs[0].Key))
	return errors.Trace(err)
}
