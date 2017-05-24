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

// TODO: Remove the package of integration.
// The tests are passed here. But import `integration` will introduce a lot of dependencies.
/*
import (
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/coreos/etcd/integration"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/store/localstore"
	"github.com/pingcap/tidb/store/localstore/goleveldb"
	goctx "golang.org/x/net/context"
)

func checkOwners(d *ddl, val bool) (isOwner bool, isBgOwner bool) {
	// The longest to wait for 3 seconds to
	// make sure that campaigning owners is completed.
	for i := 0; i < 600; i++ {
		time.Sleep(5 * time.Millisecond)
		isOwner = d.worker.isOwner()
		if isOwner != val {
			continue
		}
		isBgOwner = d.worker.isBgOwner()
		if isBgOwner != val {
			continue
		}
		break
	}
	return
}

func TestSingle(t *testing.T) {
	ChangeOwnerInNewWay := true
	defer func() { ChangeOwnerInNewWay = false }()

	driver := localstore.Driver{Driver: goleveldb.MemoryDriver{}}
	store, err := driver.Open("memory://ddl_new_owner")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer clus.Terminate(t)
	cli := clus.RandClient()
	d := newDDL(goctx.Background(), cli, store, nil, nil, testLease)
	defer d.Stop()

	isOwner, isBgOwner := checkOwners(d, true)
	if !isOwner || !isBgOwner {
		t.Fatalf("expect true, got isOwner:%v, isBgOwner:%v", isOwner, isBgOwner)
	}
}

func TestCluster(t *testing.T) {
	ChangeOwnerInNewWay := true
	defer func() {
		ChangeOwnerInNewWay = false
	}()
	log.SetLevel(log.LOG_LEVEL_INFO)

	driver := localstore.Driver{Driver: goleveldb.MemoryDriver{}}
	store, err := driver.Open("memory://ddl_new_owner")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 3})
	defer clus.Terminate(t)

	cli := clus.Client(0)
	d := newDDL(goctx.Background(), cli, store, nil, nil, testLease)
	defer d.Stop()
	isOwner, isBgOwner := checkOwners(d, true)
	if !isOwner || !isBgOwner {
		t.Fatalf("expect true, got isOwner:%v, isBgOwner:%v", isOwner, isBgOwner)
	}
	cli1 := clus.Client(1)
	d1 := newDDL(goctx.Background(), cli1, store, nil, nil, testLease)
	defer d1.Stop()
	isOwner, isBgOwner = checkOwners(d1, false)
	if isOwner || isBgOwner {
		t.Fatalf("expect false, got isOwner:%v, isBgOwner:%v", isOwner, isBgOwner)
	}

	// Delete the leader key and make the current owner of worker wait for
	// another worker become the owner.
	tc := &testDDLCallback{}
	ch := make(chan struct{})
	tc.onWatched = func(ctx goctx.Context) {
		<-ch
		isOwner, isBgOwner = checkOwners(d1, true)
		if !isOwner || !isBgOwner {
			t.Logf("expect true, got isOwner:%v, isBgOwner:%v", isOwner, isBgOwner)
		}
	}
	d.setHook(tc)
	cli2 := clus.Client(2)
	err = deleteLeader(cli2, ddlOwnerKey)
	if err != nil {
		t.Fatal(err)
	}
	err = deleteLeader(cli2, bgOwnerKey)
	if err != nil {
		t.Fatal(err)
	}
	close(ch)
	isOwner, isBgOwner = checkOwners(d, false)
	if isOwner || isBgOwner {
		t.Fatalf("expect false, got isOwner:%v, isBgOwner:%v", isOwner, isBgOwner)
	}
}

func deleteLeader(cli *clientv3.Client, prefixKey string) error {
	session, err := concurrency.NewSession(cli)
	if err != nil {
		return err
	}
	defer session.Close()
	elec := concurrency.NewElection(session, prefixKey)
	resp, err := elec.Leader(goctx.Background())
	if err != nil {
		return err
	}
	_, err = cli.Delete(goctx.Background(), string(resp.Kvs[0].Key))
	return err
}
*/
