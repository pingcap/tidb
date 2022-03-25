// Copyright 2018 PingCAP, Inc.
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

package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/check"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
)

var (
	etcdMockCluster *integration.ClusterV3
	etcdCli         *Client
	ctx             context.Context
)

func TestClient(t *testing.T) {
	integration.BeforeTest(t)
	ctx, etcdCli, etcdMockCluster = testSetup(t)
	defer etcdMockCluster.Terminate(t)
	TestingT(t)
}

var _ = Suite(&testEtcdSuite{})

type testEtcdSuite struct{}

func (t *testEtcdSuite) TestCreate(c *C) {
	etcdClient := etcdMockCluster.RandClient()

	key := "binlogcreate/testkey"
	obj := "test"

	// verify that kv pair is empty before set
	getResp, err := etcdClient.KV.Get(ctx, key)
	c.Assert(err, IsNil)
	c.Assert(getResp.Kvs, HasLen, 0)

	_, err = etcdCli.Create(ctx, key, obj, nil)
	c.Assert(err, IsNil)

	getResp, err = etcdClient.KV.Get(ctx, key)
	c.Assert(err, IsNil)
	c.Assert(getResp.Kvs, HasLen, 1)
}

func (t *testEtcdSuite) TestCreateWithTTL(c *C) {
	key := "binlogttl/ttlkey"
	obj := "ttltest"

	lcr, err := etcdCli.client.Lease.Grant(ctx, 1)
	c.Assert(err, IsNil)
	opts := []clientv3.OpOption{clientv3.WithLease(lcr.ID)}

	_, err = etcdCli.Create(ctx, key, obj, opts)
	c.Assert(err, IsNil)

	time.Sleep(2 * time.Second)
	_, _, err = etcdCli.Get(ctx, key)
	c.Assert(errors.IsNotFound(err), IsTrue)
}

func (t *testEtcdSuite) TestCreateWithKeyExist(c *C) {
	obj := "existtest"
	key := "binlogexist/exist"

	etcdClient := etcdMockCluster.RandClient()
	_, err := etcdClient.KV.Put(ctx, key, obj, nil...)
	c.Assert(err, IsNil)

	_, err = etcdCli.Create(ctx, key, obj, nil)
	c.Assert(errors.IsAlreadyExists(err), IsTrue)
}

func (t *testEtcdSuite) TestUpdate(c *C) {
	obj1 := "updatetest"
	obj2 := "updatetest2"
	key := "binlogupdate/updatekey"

	lcr, err := etcdCli.client.Lease.Grant(ctx, 2)
	c.Assert(err, IsNil)

	opts := []clientv3.OpOption{clientv3.WithLease(lcr.ID)}
	revision0, err := etcdCli.Create(ctx, key, obj1, opts)
	c.Assert(err, IsNil)

	res, revision1, err := etcdCli.Get(ctx, key)
	c.Assert(err, IsNil)
	c.Assert(string(res), Equals, obj1)
	c.Assert(revision0, Equals, revision1)

	time.Sleep(time.Second)

	err = etcdCli.Update(ctx, key, obj2, 3)
	c.Assert(err, IsNil)

	time.Sleep(2 * time.Second)

	// the new revision should greater than the old
	res, revision2, err := etcdCli.Get(ctx, key)
	c.Assert(err, IsNil)
	c.Assert(string(res), Equals, obj2)
	c.Assert(revision2, check.Greater, revision1)

	time.Sleep(2 * time.Second)
	_, _, err = etcdCli.Get(ctx, key)
	c.Assert(errors.IsNotFound(err), IsTrue)
}

func (t *testEtcdSuite) TestUpdateOrCreate(c *C) {
	obj := "updatetest"
	key := "binlogupdatecreate/updatekey"
	err := etcdCli.UpdateOrCreate(ctx, key, obj, 3)
	c.Assert(err, IsNil)
}

func (t *testEtcdSuite) TestList(c *C) {
	key := "binloglist/testkey"

	k1 := key + "/level1"
	k2 := key + "/level2"
	k3 := key + "/level3"
	k11 := key + "/level1/level1"

	revision1, err := etcdCli.Create(ctx, k1, k1, nil)
	c.Assert(err, IsNil)

	revision2, err := etcdCli.Create(ctx, k2, k2, nil)
	c.Assert(err, IsNil)
	c.Assert(revision2 > revision1, IsTrue)

	revision3, err := etcdCli.Create(ctx, k3, k3, nil)
	c.Assert(err, IsNil)
	c.Assert(revision3 > revision2, IsTrue)

	revision4, err := etcdCli.Create(ctx, k11, k11, nil)
	c.Assert(err, IsNil)
	c.Assert(revision4 > revision3, IsTrue)

	root, revision5, err := etcdCli.List(ctx, key)
	c.Assert(err, IsNil)
	c.Assert(string(root.Childs["level1"].Value), Equals, k1)
	c.Assert(string(root.Childs["level1"].Childs["level1"].Value), Equals, k11)
	c.Assert(string(root.Childs["level2"].Value), Equals, k2)
	c.Assert(string(root.Childs["level3"].Value), Equals, k3)

	// the revision of list should equal to the latest update's revision
	_, revision6, err := etcdCli.Get(ctx, k11)
	c.Assert(err, IsNil)
	c.Assert(revision5, Equals, revision6)
}

func (t *testEtcdSuite) TestDelete(c *C) {
	key := "binlogdelete/testkey"
	keys := []string{key + "/level1", key + "/level2", key + "/level1" + "/level1"}
	for _, k := range keys {
		_, err := etcdCli.Create(ctx, k, k, nil)
		c.Assert(err, IsNil)
	}

	root, _, err := etcdCli.List(ctx, key)
	c.Assert(err, IsNil)
	c.Assert(root.Childs, HasLen, 2)

	err = etcdCli.Delete(ctx, keys[1], false)
	c.Assert(err, IsNil)

	root, _, err = etcdCli.List(ctx, key)
	c.Assert(err, IsNil)
	c.Assert(root.Childs, HasLen, 1)

	err = etcdCli.Delete(ctx, key, true)
	c.Assert(err, IsNil)

	root, _, err = etcdCli.List(ctx, key)
	c.Assert(err, IsNil)
	c.Assert(root.Childs, HasLen, 0)
}

func (t *testEtcdSuite) TestDoTxn(c *C) {
	// case1: create two keys in one transaction
	ops := []*Operation{
		{
			Tp:    CreateOp,
			Key:   "test1",
			Value: "1",
		}, {
			Tp:    CreateOp,
			Key:   "test2",
			Value: "2",
		},
	}
	revision, err := etcdCli.DoTxn(context.Background(), ops)
	c.Assert(err, IsNil)

	value1, revision1, err := etcdCli.Get(context.Background(), "test1")
	c.Assert(err, IsNil)
	c.Assert(string(value1), Equals, "1")
	c.Assert(revision1, Equals, revision)

	value2, revision2, err := etcdCli.Get(context.Background(), "test2")
	c.Assert(err, IsNil)
	c.Assert(string(value2), Equals, "2")
	c.Assert(revision2, Equals, revision)

	// case2: delete, update and create in one transaction
	ops = []*Operation{
		{
			Tp:  DeleteOp,
			Key: "test1",
		}, {
			Tp:    UpdateOp,
			Key:   "test2",
			Value: "22",
		}, {
			Tp:    CreateOp,
			Key:   "test3",
			Value: "3",
		},
	}

	revision, err = etcdCli.DoTxn(context.Background(), ops)
	c.Assert(err, IsNil)

	_, _, err = etcdCli.Get(context.Background(), "test1")
	c.Assert(err, ErrorMatches, ".* not found")

	value2, revision2, err = etcdCli.Get(context.Background(), "test2")
	c.Assert(err, IsNil)
	c.Assert(string(value2), Equals, "22")
	c.Assert(revision2, Equals, revision)

	value3, revision3, err := etcdCli.Get(context.Background(), "test3")
	c.Assert(err, IsNil)
	c.Assert(string(value3), Equals, "3")
	c.Assert(revision3, Equals, revision)

	// case3: create keys with TTL
	ops = []*Operation{
		{
			Tp:    CreateOp,
			Key:   "test4",
			Value: "4",
			TTL:   1,
		}, {
			Tp:    CreateOp,
			Key:   "test5",
			Value: "5",
		},
	}
	revision, err = etcdCli.DoTxn(context.Background(), ops)
	c.Assert(err, IsNil)

	value4, revision4, err := etcdCli.Get(context.Background(), "test4")
	c.Assert(err, IsNil)
	c.Assert(string(value4), Equals, "4")
	c.Assert(revision4, Equals, revision)

	value5, revision5, err := etcdCli.Get(context.Background(), "test5")
	c.Assert(err, IsNil)
	c.Assert(string(value5), Equals, "5")
	c.Assert(revision5, Equals, revision)

	// sleep 2 seconds and this key will be deleted
	time.Sleep(2 * time.Second)
	_, _, err = etcdCli.Get(context.Background(), "test4")
	c.Assert(err, ErrorMatches, ".* not found")

	// case4: do transaction failed because key is deleted, so can't update
	ops = []*Operation{
		{
			Tp:    CreateOp,
			Key:   "test4",
			Value: "4",
		}, {
			Tp:    UpdateOp, // key test1 is deleted, so will update failed
			Key:   "test1",
			Value: "11",
		},
	}

	_, err = etcdCli.DoTxn(context.Background(), ops)
	c.Assert(err, ErrorMatches, "do transaction failed.*")

	_, _, err = etcdCli.Get(context.Background(), "test4")
	c.Assert(err, ErrorMatches, ".* not found")

	// case5: do transaction failed because can't operate one key in one transaction
	ops = []*Operation{
		{
			Tp:    CreateOp,
			Key:   "test6",
			Value: "6",
		}, {
			Tp:    UpdateOp,
			Key:   "test6",
			Value: "66",
		},
	}

	_, err = etcdCli.DoTxn(context.Background(), ops)
	c.Assert(err, ErrorMatches, "etcdserver: duplicate key given in txn request")

	_, _, err = etcdCli.Get(context.Background(), "test6")
	c.Assert(err, ErrorMatches, ".* not found")

	// case6: do transaction failed because can't create an existing key
	ops = []*Operation{
		{
			Tp:    CreateOp,
			Key:   "test2", // already exist
			Value: "222",
		}, {
			Tp:    UpdateOp,
			Key:   "test5",
			Value: "555",
		},
	}

	_, err = etcdCli.DoTxn(context.Background(), ops)
	c.Assert(err, ErrorMatches, "do transaction failed.*")

	value2, _, err = etcdCli.Get(context.Background(), "test2")
	c.Assert(err, IsNil)
	c.Assert(string(value2), Equals, "22")

	value5, _, err = etcdCli.Get(context.Background(), "test5")
	c.Assert(err, IsNil)
	c.Assert(string(value5), Equals, "5")

	// case7: delete not exist key but will do transaction success
	ops = []*Operation{
		{
			Tp:  DeleteOp,
			Key: "test7", // not exist
		}, {
			Tp:    CreateOp,
			Key:   "test8",
			Value: "8",
		},
	}

	_, err = etcdCli.DoTxn(context.Background(), ops)
	c.Assert(err, IsNil)

	value8, _, err := etcdCli.Get(context.Background(), "test8")
	c.Assert(err, IsNil)
	c.Assert(string(value8), Equals, "8")

	// case8: do transaction failed because can't set TTL for delete operation
	ops = []*Operation{
		{
			Tp:  DeleteOp,
			Key: "test8",
			TTL: 1,
		},
	}

	_, err = etcdCli.DoTxn(context.Background(), ops)
	c.Assert(err, ErrorMatches, "unexpected TTL in delete operation")
}

func testSetup(t *testing.T) (context.Context, *Client, *integration.ClusterV3) {
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	etcd := NewClient(cluster.RandClient(), "binlog")
	return context.Background(), etcd, cluster
}
