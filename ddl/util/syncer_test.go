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

package util_test

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/pingcap/errors"
	. "github.com/pingcap/tidb/ddl"
	. "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/etcdserver"
	"go.etcd.io/etcd/tests/v3/integration"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const minInterval = 10 * time.Nanosecond // It's used to test timeout.
const testLease = 5 * time.Millisecond

func TestSyncerSimple(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a colon which is not allowed on Windows")
	}
	integration.BeforeTest(t)

	origin := CheckVersFirstWaitTime
	CheckVersFirstWaitTime = 0
	defer func() {
		CheckVersFirstWaitTime = origin
	}()

	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()

	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(t)
	cli := cluster.RandClient()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ic := infoschema.NewCache(2)
	ic.Insert(infoschema.MockInfoSchemaWithSchemaVer(nil, 0), 0)
	d := NewDDL(
		ctx,
		WithEtcdClient(cli),
		WithStore(store),
		WithLease(testLease),
		WithInfoCache(ic),
	)
	require.NoError(t, d.Start(nil))
	defer func() {
		require.NoError(t, d.Stop())
	}()

	// for init function
	require.NoError(t, d.SchemaSyncer().Init(ctx))
	resp, err := cli.Get(ctx, DDLAllSchemaVersions, clientv3.WithPrefix())
	require.NoError(t, err)

	key := DDLAllSchemaVersions + "/" + d.OwnerManager().ID()
	checkRespKV(t, 1, key, InitialVersion, resp.Kvs...)
	// for MustGetGlobalVersion function
	globalVer, err := d.SchemaSyncer().MustGetGlobalVersion(ctx)
	require.NoError(t, err)
	require.Equal(t, InitialVersion, fmt.Sprintf("%d", globalVer))

	childCtx, cancel := context.WithTimeout(ctx, minInterval)
	defer cancel()
	_, err = d.SchemaSyncer().MustGetGlobalVersion(childCtx)
	require.True(t, isTimeoutError(err))

	ic2 := infoschema.NewCache(2)
	ic2.Insert(infoschema.MockInfoSchemaWithSchemaVer(nil, 0), 0)
	d1 := NewDDL(
		ctx,
		WithEtcdClient(cli),
		WithStore(store),
		WithLease(testLease),
		WithInfoCache(ic2),
	)
	require.NoError(t, d1.Start(nil))
	defer func() {
		require.NoError(t, d1.Stop())
	}()
	require.NoError(t, d1.SchemaSyncer().Init(ctx))

	// for watchCh
	var wg util.WaitGroupWrapper
	currentVer := int64(123)
	var checkErr string
	wg.Run(func() {
		select {
		case resp := <-d.SchemaSyncer().GlobalVersionCh():
			if len(resp.Events) < 1 {
				checkErr = "get chan events count less than 1"
				return
			}
			checkRespKV(t, 1, DDLGlobalSchemaVersion, fmt.Sprintf("%v", currentVer), resp.Events[0].Kv)
		case <-time.After(3 * time.Second):
			checkErr = "get update version failed"
			return
		}
	})

	// for update latestSchemaVersion
	require.NoError(t, d.SchemaSyncer().OwnerUpdateGlobalVersion(ctx, currentVer))

	wg.Wait()

	require.Equal(t, "", checkErr)

	// for CheckAllVersions
	childCtx, cancel = context.WithTimeout(ctx, 200*time.Millisecond)
	require.Error(t, d.SchemaSyncer().OwnerCheckAllVersions(childCtx, currentVer))
	cancel()

	// for UpdateSelfVersion
	require.NoError(t, d.SchemaSyncer().UpdateSelfVersion(context.Background(), currentVer))
	require.NoError(t, d1.SchemaSyncer().UpdateSelfVersion(context.Background(), currentVer))

	childCtx, cancel = context.WithTimeout(ctx, minInterval)
	defer cancel()
	err = d1.SchemaSyncer().UpdateSelfVersion(childCtx, currentVer)
	require.True(t, isTimeoutError(err))

	// for CheckAllVersions
	require.NoError(t, d.SchemaSyncer().OwnerCheckAllVersions(context.Background(), currentVer-1))
	require.NoError(t, d.SchemaSyncer().OwnerCheckAllVersions(context.Background(), currentVer))

	childCtx, cancel = context.WithTimeout(ctx, minInterval)
	defer cancel()
	err = d.SchemaSyncer().OwnerCheckAllVersions(childCtx, currentVer)
	require.True(t, isTimeoutError(err))

	// for StartCleanWork
	ttl := 10
	// Make sure NeededCleanTTL > ttl, then we definitely clean the ttl.
	NeededCleanTTL = int64(11)
	ttlKey := "session_ttl_key"
	ttlVal := "session_ttl_val"
	session, err := util.NewSession(ctx, "", cli, util.NewSessionDefaultRetryCnt, ttl)
	require.NoError(t, err)
	require.NoError(t, PutKVToEtcd(context.Background(), cli, 5, ttlKey, ttlVal, clientv3.WithLease(session.Lease())))

	// Make sure the ttlKey is existing in etcd.
	resp, err = cli.Get(ctx, ttlKey)
	require.NoError(t, err)
	checkRespKV(t, 1, ttlKey, ttlVal, resp.Kvs...)
	d.SchemaSyncer().NotifyCleanExpiredPaths()
	// Make sure the clean worker is done.
	notifiedCnt := 1
	for i := 0; i < 100; i++ {
		isNotified := d.SchemaSyncer().NotifyCleanExpiredPaths()
		if isNotified {
			notifiedCnt++
		}
		// notifyCleanExpiredPathsCh's length is 1,
		// so when notifiedCnt is 3, we can make sure the clean worker is done at least once.
		if notifiedCnt == 3 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	require.Equal(t, 3, notifiedCnt)

	// Make sure the ttlKey is removed in etcd.
	resp, err = cli.Get(ctx, ttlKey)
	require.NoError(t, err)
	checkRespKV(t, 0, ttlKey, "", resp.Kvs...)

	// for Close
	resp, err = cli.Get(context.Background(), key)
	require.NoError(t, err)

	currVer := fmt.Sprintf("%v", currentVer)
	checkRespKV(t, 1, key, currVer, resp.Kvs...)
	d.SchemaSyncer().Close()
	resp, err = cli.Get(context.Background(), key)
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 0)
}

func isTimeoutError(err error) bool {
	return terror.ErrorEqual(err, context.DeadlineExceeded) ||
		status.Code(errors.Cause(err)) == codes.DeadlineExceeded ||
		terror.ErrorEqual(err, etcdserver.ErrTimeout)
}

func checkRespKV(t *testing.T, kvCount int, key, val string, kvs ...*mvccpb.KeyValue) {
	require.Len(t, kvs, kvCount)

	if kvCount == 0 {
		return
	}

	kv := kvs[0]
	require.Equal(t, key, string(kv.Key))
	require.Equal(t, val, string(kv.Value))
}
