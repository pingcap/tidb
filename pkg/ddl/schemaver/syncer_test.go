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

package schemaver_test

import (
	"context"
	"fmt"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/schemaver"
	util2 "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/server/v3/etcdserver"
	"go.etcd.io/etcd/tests/v3/integration"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const minInterval = 10 * time.Nanosecond // It's used to test timeout.

func TestSyncerSimple(t *testing.T) {
	variable.EnableMDL.Store(false)
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a colon which is not allowed on Windows")
	}
	integration.BeforeTestExternal(t)

	origin := schemaver.CheckVersFirstWaitTime
	schemaver.CheckVersFirstWaitTime = 0
	defer func() {
		schemaver.CheckVersFirstWaitTime = origin
	}()

	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(t)
	cli := cluster.RandClient()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	syncers := make([]schemaver.Syncer, 0, 2)
	for i := 0; i < 2; i++ {
		id := strconv.Itoa(i + 1)
		schemaVerSyncer := schemaver.NewEtcdSyncer(cli, id)
		require.NoError(t, schemaVerSyncer.Init(ctx))
		syncers = append(syncers, schemaVerSyncer)
	}
	defer func() {
		for _, syncer := range syncers {
			syncer.Close()
		}
	}()

	for i := range syncers {
		id := strconv.Itoa(i + 1)
		key := util2.DDLAllSchemaVersions + "/" + id
		resp, err := cli.Get(ctx, key)
		require.NoError(t, err)
		checkRespKV(t, 1, key, schemaver.InitialVersion, resp.Kvs...)
	}

	// for watchCh
	var wg util.WaitGroupWrapper
	currentVer := int64(123)
	var checkErr string
	wg.Run(func() {
		select {
		case resp := <-syncers[0].GlobalVersionCh():
			if len(resp.Events) < 1 {
				checkErr = "get chan events count less than 1"
				return
			}
			checkRespKV(t, 1, util2.DDLGlobalSchemaVersion, fmt.Sprintf("%v", currentVer), resp.Events[0].Kv)
		case <-time.After(3 * time.Second):
			checkErr = "get update version failed"
			return
		}
	})

	// for update latestSchemaVersion
	require.NoError(t, syncers[0].OwnerUpdateGlobalVersion(ctx, currentVer))

	wg.Wait()

	require.Equal(t, "", checkErr)

	// for CheckAllVersions
	childCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	require.Error(t, syncers[0].WaitVersionSynced(childCtx, 0, currentVer))
	cancel()

	// for UpdateSelfVersion
	require.NoError(t, syncers[0].UpdateSelfVersion(context.Background(), 0, currentVer))
	require.NoError(t, syncers[1].UpdateSelfVersion(context.Background(), 0, currentVer))

	childCtx, cancel = context.WithTimeout(ctx, minInterval)
	defer cancel()
	err := syncers[1].UpdateSelfVersion(childCtx, 0, currentVer)
	require.True(t, isTimeoutError(err))

	// for CheckAllVersions
	require.NoError(t, syncers[0].WaitVersionSynced(context.Background(), 0, currentVer-1))
	require.NoError(t, syncers[0].WaitVersionSynced(context.Background(), 0, currentVer))

	childCtx, cancel = context.WithTimeout(ctx, minInterval)
	defer cancel()
	err = syncers[0].WaitVersionSynced(childCtx, 0, currentVer)
	require.True(t, isTimeoutError(err))

	// for Close
	key := util2.DDLAllSchemaVersions + "/1"
	resp, err := cli.Get(context.Background(), key)
	require.NoError(t, err)

	currVer := fmt.Sprintf("%v", currentVer)
	checkRespKV(t, 1, key, currVer, resp.Kvs...)
	syncers[0].Close()
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

func TestPutKVToEtcdMono(t *testing.T) {
	integration.BeforeTestExternal(t)

	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(t)
	cli := cluster.RandClient()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := util2.PutKVToEtcdMono(ctx, cli, 3, "testKey", strconv.Itoa(1))
	require.NoError(t, err)

	err = util2.PutKVToEtcdMono(ctx, cli, 3, "testKey", strconv.Itoa(2))
	require.NoError(t, err)

	err = util2.PutKVToEtcdMono(ctx, cli, 3, "testKey", strconv.Itoa(3))
	require.NoError(t, err)

	eg := util.NewErrorGroupWithRecover()
	for i := 0; i < 30; i++ {
		eg.Go(func() error {
			err := util2.PutKVToEtcdMono(ctx, cli, 1, "testKey", strconv.Itoa(5))
			return err
		})
	}
	// PutKVToEtcdMono should be conflicted and get errors.
	require.Error(t, eg.Wait())

	eg = util.NewErrorGroupWithRecover()
	for i := 0; i < 30; i++ {
		eg.Go(func() error {
			err := util2.PutKVToEtcd(ctx, cli, 1, "testKey", strconv.Itoa(5))
			return err
		})
	}
	require.NoError(t, eg.Wait())

	err = util2.PutKVToEtcdMono(ctx, cli, 3, "testKey", strconv.Itoa(1))
	require.NoError(t, err)
}
