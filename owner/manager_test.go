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
// +build !windows

package owner_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"
	. "github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/owner"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/testleak"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/integration"
	goctx "golang.org/x/net/context"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = SerialSuites(&testSuite{})

type testSuite struct {
}

func (s *testSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
}

func (s *testSuite) TearDownSuite(c *C) {
	testleak.AfterTest(c)
}

const testLease = 5 * time.Millisecond

func checkOwner(d DDL, fbVal bool) (isOwner bool) {
	manager := d.OwnerManager()
	// The longest to wait for 3 seconds to
	// make sure that campaigning owners is completed.
	for i := 0; i < 600; i++ {
		time.Sleep(5 * time.Millisecond)
		isOwner = manager.IsOwner()
		if isOwner == fbVal {
			break
		}
	}
	return
}

// Ignore this test on the windows platform, because calling unix socket with address in
// host:port format fails on windows.
func (s *testSuite) TestSingle(c *C) {
	store, err := mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	defer store.Close()

	clus := integration.NewClusterV3(nil, &integration.ClusterConfig{Size: 1})
	defer clus.Terminate(nil)
	cli := clus.RandClient()
	ctx := goctx.Background()
	d := NewDDL(
		ctx,
		WithEtcdClient(cli),
		WithStore(store),
		WithLease(testLease),
	)
	defer d.Stop()

	isOwner := checkOwner(d, true)
	c.Assert(isOwner, IsTrue)

	// test for newSession failed
	ctx, cancel := goctx.WithCancel(ctx)
	cancel()
	manager := owner.NewOwnerManager(cli, "ddl", "ddl_id", DDLOwnerKey, nil)
	err = manager.CampaignOwner(ctx)
	c.Assert(terror.ErrorEqual(err, goctx.Canceled) || terror.ErrorEqual(err, goctx.DeadlineExceeded), IsTrue)
	isOwner = checkOwner(d, true)
	c.Assert(isOwner, IsTrue)
	// The test is used to exit campaign loop.
	d.OwnerManager().Cancel()
	isOwner = checkOwner(d, false)
	c.Assert(isOwner, IsFalse)
	time.Sleep(200 * time.Millisecond)
	ownerID, _ := manager.GetOwnerID(goctx.Background())
	// The error is ok to be not nil since we canceled the manager.
	c.Assert(ownerID, Equals, "")
}

func (s *testSuite) TestCluster(c *C) {
	tmpTTL := 3
	orignalTTL := owner.ManagerSessionTTL
	owner.ManagerSessionTTL = tmpTTL
	defer func() {
		owner.ManagerSessionTTL = orignalTTL
	}()
	store, err := mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	defer store.Close()
	clus := integration.NewClusterV3(nil, &integration.ClusterConfig{Size: 4})
	defer clus.Terminate(nil)

	cli := clus.Client(0)
	d := NewDDL(
		goctx.Background(),
		WithEtcdClient(cli),
		WithStore(store),
		WithLease(testLease),
	)
	isOwner := checkOwner(d, true)
	c.Assert(isOwner, IsTrue)
	cli1 := clus.Client(1)
	d1 := NewDDL(
		goctx.Background(),
		WithEtcdClient(cli1),
		WithStore(store),
		WithLease(testLease),
	)
	isOwner = checkOwner(d1, false)
	c.Assert(isOwner, IsFalse)

	// Delete the leader key, the d1 become the owner.
	cliRW := clus.Client(2)
	err = deleteLeader(cliRW, DDLOwnerKey)
	c.Assert(err, IsNil)
	isOwner = checkOwner(d, false)
	c.Assert(isOwner, IsFalse)
	d.Stop()

	// d3 (not owner) stop
	cli3 := clus.Client(3)
	d3 := NewDDL(
		goctx.Background(),
		WithEtcdClient(cli3),
		WithStore(store),
		WithLease(testLease),
	)
	defer d3.Stop()
	isOwner = checkOwner(d3, false)
	c.Assert(isOwner, IsFalse)
	d3.Stop()

	// Cancel the owner context, there is no owner.
	d1.Stop()
	time.Sleep(time.Duration(tmpTTL+1) * time.Second)
	session, err := concurrency.NewSession(cliRW)
	c.Assert(err, IsNil)
	elec := concurrency.NewElection(session, DDLOwnerKey)
	logPrefix := fmt.Sprintf("[ddl] %s ownerManager %s", DDLOwnerKey, "useless id")
	logCtx := logutil.WithKeyValue(context.Background(), "owner info", logPrefix)
	_, err = owner.GetOwnerInfo(goctx.Background(), logCtx, elec, "useless id")
	c.Assert(terror.ErrorEqual(err, concurrency.ErrElectionNoLeader), IsTrue)
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
