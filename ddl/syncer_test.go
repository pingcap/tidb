// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

/*
// TODO: Remove the package of integration.
// The tests are passed here. But import `integration` will introduce a lot of dependencies.
import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/integration"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/pingcap/tidb/store/localstore"
	"github.com/pingcap/tidb/store/localstore/goleveldb"
	goctx "golang.org/x/net/context"
)

func TestSyncerSimple(t *testing.T) {
	origin := checkVersFirstWaitTime
	checkVersFirstWaitTime = 0
	defer func() {
		checkVersFirstWaitTime = origin
	}()

	driver := localstore.Driver{Driver: goleveldb.MemoryDriver{}}
	store, err := driver.Open("memory://ddl_syncer_init")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer clus.Terminate(t)
	cli := clus.RandClient()
	ctx := goctx.Background()
	d := newDDL(ctx, cli, store, nil, nil, testLease)
	defer d.Stop()
	d1 := newDDL(ctx, cli, store, nil, nil, testLease)
	defer d1.Stop()

	// for init function
	if err = d.SchemaSyncer().Init(ctx); err != nil {
		t.Fatalf("schema version syncer init failed %v", err)
	}
	resp, err := cli.Get(ctx, ddlAllSchemaVersions, clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("client get version failed %v", err)
	}
	key := ddlAllSchemaVersions + "/" + d.uuid
	checkRespKV(t, 1, key, initialVersion, resp.Kvs...)
	resp, err = cli.Get(ctx, ddlGlobalSchemaVersion)
	if err != nil {
		t.Fatalf("client get version failed %v", err)
	}
	checkRespKV(t, 1, ddlGlobalSchemaVersion, initialVersion, resp.Kvs...)
	if err = d1.SchemaSyncer().Init(ctx); err != nil {
		t.Fatalf("schema version syncer init failed %v", err)
	}

	// for watchCh
	wg := sync.WaitGroup{}
	wg.Add(1)
	currentVer := int64(123)
	go func() {
		defer wg.Done()
		select {
		case resp := <-d.worker.GlobalVersionCh():
			if len(resp.Events) < 1 {
				t.Fatalf("get chan events count less than 1")
			}
			checkRespKV(t, 1, ddlGlobalSchemaVersion, fmt.Sprintf("%v", currentVer), resp.Events[0].Kv)
		case <-time.After(50 * time.Millisecond):
			t.Fatalf("get udpate version failed")
		}
	}()

	// for update latestSchemaVersion
	err = d.worker.UpdateGlobalVersion(ctx, currentVer)
	if err != nil {
		t.Fatalf("update latest schema version failed %v", err)
	}
	err = d1.worker.UpdateGlobalVersion(ctx, currentVer)
	if err != nil {
		t.Fatalf("update latest schema version failed %v", err)
	}

	wg.Wait()

	// for CheckAllVersions
	childCtx, cancel := goctx.WithTimeout(ctx, 20*time.Millisecond)
	err = d.worker.CheckAllVersions(childCtx, currentVer)
	if err == nil {
		t.Fatalf("check result not match")
	}
	cancel()

	// for UpdateSelfVersion
	childCtx, cancel = goctx.WithTimeout(ctx, 30*time.Millisecond)
	err = d.worker.UpdateSelfVersion(childCtx, currentVer)
	if err != nil {
		t.Fatalf("update self version failed %v", err)
	}
	cancel()
	childCtx, cancel = goctx.WithTimeout(ctx, 30*time.Millisecond)
	err = d1.worker.UpdateSelfVersion(childCtx, currentVer)
	if err != nil {
		t.Fatalf("update self version failed %v", err)
	}
	cancel()

	// for CheckAllVersions
	childCtx, cancel = goctx.WithTimeout(ctx, 30*time.Millisecond)
	err = d.worker.CheckAllVersions(childCtx, currentVer)
	if err != nil {
		t.Fatalf("check all version failed %v", err)
	}
	cancel()

	resp, err = cli.Get(goctx.Background(), key)
	if err != nil {
		t.Fatalf("get key %s failed %v", key, err)
	}
	checkRespKV(t, 1, key, "123", resp.Kvs...)
	d.worker.RemoveSelfVersionPath()
	resp, err = cli.Get(goctx.Background(), key)
	if err != nil {
		t.Fatalf("get key %s failed %v", key, err)
	}
	if len(resp.Kvs) != 0 {
		t.Fatalf("remove key %s failed %v", key, err)
	}
}

func checkRespKV(t *testing.T, kvCount int, key, val string,
	kvs ...*mvccpb.KeyValue) {
	if len(kvs) != kvCount {
		t.Fatalf("resp kvs %v length is != 1", kvs)
	}
	kv := kvs[0]
	if string(kv.Key) != key {
		t.Fatalf("key resp %s, exported %s", kv.Key, key)
	}
	if val != val {
		t.Fatalf("val resp %s, exported %s", kv.Value, val)
	}
}
*/
