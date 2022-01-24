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
// See the License for the specific language governing permissions and
// limitations under the License.

package owner_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"
	. "github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/owner"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/logutil"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/integration"
	goctx "golang.org/x/net/context"
)

const testLease = 5 * time.Millisecond

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

func TestSingle(t *testing.T) {
	store, err := mockstore.NewMockTikvStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer clus.Terminate(t)
	cli := clus.RandClient()
	ctx := goctx.Background()
	d := NewDDL(ctx, cli, store, nil, nil, testLease, nil)
	defer d.Stop()

	isOwner := checkOwner(d, true)
	if !isOwner {
		t.Fatalf("expect true, got isOwner:%v", isOwner)
	}

	// test for newSession failed
	ctx, cancel := goctx.WithCancel(ctx)
	cancel()
	manager := owner.NewOwnerManager(cli, "ddl", "ddl_id", DDLOwnerKey, nil)
	err = manager.CampaignOwner(ctx)
	if !terror.ErrorEqual(err, goctx.Canceled) &&
		!terror.ErrorEqual(err, goctx.DeadlineExceeded) {
		t.Fatalf("campaigned result don't match, err %v", err)
	}
	isOwner = checkOwner(d, true)
	if !isOwner {
		t.Fatalf("expect true, got isOwner:%v", isOwner)
	}
	// The test is used to exit campaign loop.
	d.OwnerManager().Cancel()
	isOwner = checkOwner(d, false)
	if isOwner {
		t.Fatalf("expect false, got isOwner:%v", isOwner)
	}
	time.Sleep(200 * time.Millisecond)
	ownerID, _ := manager.GetOwnerID(goctx.Background())
	// The error is ok to be not nil since we canceled the manager.
	if ownerID != "" {
		t.Fatalf("owner %s is not empty", ownerID)
	}
}

func TestCluster(t *testing.T) {
	tmpTTL := 3
	orignalTTL := owner.ManagerSessionTTL
	owner.ManagerSessionTTL = tmpTTL
	defer func() {
		owner.ManagerSessionTTL = orignalTTL
	}()
	store, err := mockstore.NewMockTikvStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 4})
	defer clus.Terminate(t)

	cli := clus.Client(0)
	d := NewDDL(goctx.Background(), cli, store, nil, nil, testLease, nil)
	isOwner := checkOwner(d, true)
	if !isOwner {
		t.Fatalf("expect true, got isOwner:%v", isOwner)
	}
	cli1 := clus.Client(1)
	d1 := NewDDL(goctx.Background(), cli1, store, nil, nil, testLease, nil)
	isOwner = checkOwner(d1, false)
	if isOwner {
		t.Fatalf("expect false, got isOwner:%v", isOwner)
	}

	// Delete the leader key, the d1 become the owner.
	cliRW := clus.Client(2)
	err = deleteLeader(cliRW, DDLOwnerKey)
	if err != nil {
		t.Fatal(err)
	}
	isOwner = checkOwner(d, false)
	if isOwner {
		t.Fatalf("expect false, got isOwner:%v", isOwner)
	}
	d.Stop()

	// d3 (not owner) stop
	cli3 := clus.Client(3)
	d3 := NewDDL(goctx.Background(), cli3, store, nil, nil, testLease, nil)
	defer d3.Stop()
	isOwner = checkOwner(d3, false)
	if isOwner {
		t.Fatalf("expect false, got isOwner:%v", isOwner)
	}
	d3.Stop()

	// Cancel the owner context, there is no owner.
	d1.Stop()
	time.Sleep(time.Duration(tmpTTL+1) * time.Second)
	session, err := concurrency.NewSession(cliRW)
	if err != nil {
		t.Fatalf("new session failed %v", err)
	}
	elec := concurrency.NewElection(session, DDLOwnerKey)
	logPrefix := fmt.Sprintf("[ddl] %s ownerManager %s", DDLOwnerKey, "useless id")
	logCtx := logutil.WithKeyValue(context.Background(), "owner info", logPrefix)
	_, err = owner.GetOwnerInfo(goctx.Background(), logCtx, elec, "useless id")
	if !terror.ErrorEqual(err, concurrency.ErrElectionNoLeader) {
		t.Fatalf("get owner info result don't match, err %v", err)
	}
}

func deleteLeader(cli *clientv3.Client, prefixKey string) error {
	session, err := concurrency.NewSession(cli)
	if err != nil {
		return errors.Trace(err)
	}
	defer session.Close()
	elec := concurrency.NewElection(session, prefixKey)
	resp, err := elec.Leader(goctx.Background())
	if err != nil {
		return errors.Trace(err)
	}
	_, err = cli.Delete(goctx.Background(), string(resp.Kvs[0].Key))
	return errors.Trace(err)
}
