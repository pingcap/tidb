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
// See the License for the specific language governing permissions and
// limitations under the License.

package etcd

import (
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/integration"
	. "github.com/pingcap/check"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

var (
	etcdMockCluster *integration.ClusterV3
	etcdCli         *Client
	ctx             context.Context
)

func TestClient(t *testing.T) {
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

	err = etcdCli.Create(ctx, key, obj, nil)
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
	opts := []clientv3.OpOption{clientv3.WithLease(clientv3.LeaseID(lcr.ID))}

	err = etcdCli.Create(ctx, key, obj, opts)
	c.Assert(err, IsNil)

	time.Sleep(2 * time.Second)
	_, err = etcdCli.Get(ctx, key)
	c.Assert(errors.IsNotFound(err), IsTrue)
}

func (t *testEtcdSuite) TestCreateWithKeyExist(c *C) {
	obj := "existtest"
	key := "binlogexist/exist"

	etcdClient := etcdMockCluster.RandClient()
	_, err := etcdClient.KV.Put(ctx, key, obj, nil...)
	c.Assert(err, IsNil)

	err = etcdCli.Create(ctx, key, obj, nil)
	c.Assert(errors.IsAlreadyExists(err), IsTrue)
}

func (t *testEtcdSuite) TestUpdate(c *C) {
	obj1 := "updatetest"
	obj2 := "updatetest2"
	key := "binlogupdate/updatekey"

	lcr, err := etcdCli.client.Lease.Grant(ctx, 2)
	c.Assert(err, IsNil)

	opts := []clientv3.OpOption{clientv3.WithLease(lcr.ID)}
	err = etcdCli.Create(ctx, key, obj1, opts)
	c.Assert(err, IsNil)

	time.Sleep(time.Second)

	err = etcdCli.Update(ctx, key, obj2, 3)
	c.Assert(err, IsNil)

	time.Sleep(2 * time.Second)

	res, err := etcdCli.Get(ctx, key)
	c.Assert(err, IsNil)
	c.Assert(string(res), Equals, obj2)

	time.Sleep(2 * time.Second)
	res, err = etcdCli.Get(ctx, key)
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

	err := etcdCli.Create(ctx, k1, k1, nil)
	c.Assert(err, IsNil)

	err = etcdCli.Create(ctx, k2, k2, nil)
	c.Assert(err, IsNil)

	err = etcdCli.Create(ctx, k3, k3, nil)
	c.Assert(err, IsNil)

	err = etcdCli.Create(ctx, k11, k11, nil)
	c.Assert(err, IsNil)

	root, err := etcdCli.List(ctx, key)
	c.Assert(err, IsNil)
	c.Assert(string(root.Childs["level1"].Value), Equals, k1)
	c.Assert(string(root.Childs["level1"].Childs["level1"].Value), Equals, k11)
	c.Assert(string(root.Childs["level2"].Value), Equals, k2)
	c.Assert(string(root.Childs["level3"].Value), Equals, k3)
}

func (t *testEtcdSuite) TestDelete(c *C) {
	key := "binlogdelete/testkey"
	keys := []string{key + "/level1", key + "/level2", key + "/level1" + "/level1"}
	for _, k := range keys {
		err := etcdCli.Create(ctx, k, k, nil)
		c.Assert(err, IsNil)
	}

	root, err := etcdCli.List(ctx, key)
	c.Assert(err, IsNil)
	c.Assert(root.Childs, HasLen, 2)

	err = etcdCli.Delete(ctx, keys[1], false)
	c.Assert(err, IsNil)

	root, err = etcdCli.List(ctx, key)
	c.Assert(err, IsNil)
	c.Assert(root.Childs, HasLen, 1)

	err = etcdCli.Delete(ctx, key, true)
	c.Assert(err, IsNil)

	root, err = etcdCli.List(ctx, key)
	c.Assert(err, IsNil)
	c.Assert(root.Childs, HasLen, 0)
}

func testSetup(t *testing.T) (context.Context, *Client, *integration.ClusterV3) {
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	etcd := NewClient(cluster.RandClient(), "binlog")
	return context.Background(), etcd, cluster
}
