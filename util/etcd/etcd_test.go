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

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
)

var (
	etcdMockCluster *integration.ClusterV3
	etcdCli         *Client
	ctx             context.Context
)

func TestCreate(t *testing.T) {
	integration.BeforeTestExternal(t)
	ctx, etcdCli, etcdMockCluster = testSetup(t)
	defer etcdMockCluster.Terminate(t)
	etcdClient := etcdMockCluster.RandClient()

	key := "binlogcreate/testkey"
	obj := "test"

	// verify that kv pair is empty before set
	getResp, err := etcdClient.KV.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, getResp.Kvs, 0)

	_, err = etcdCli.Create(ctx, key, obj, nil)
	require.NoError(t, err)

	getResp, err = etcdClient.KV.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, getResp.Kvs, 1)
}

func TestCreateWithTTL(t *testing.T) {
	integration.BeforeTestExternal(t)
	ctx, etcdCli, etcdMockCluster = testSetup(t)
	defer etcdMockCluster.Terminate(t)
	key := "binlogttl/ttlkey"
	obj := "ttltest"

	lcr, err := etcdCli.client.Lease.Grant(ctx, 1)
	require.NoError(t, err)
	opts := []clientv3.OpOption{clientv3.WithLease(lcr.ID)}

	_, err = etcdCli.Create(ctx, key, obj, opts)
	require.NoError(t, err)

	time.Sleep(2 * time.Second)
	_, _, err = etcdCli.Get(ctx, key)
	require.True(t, errors.IsNotFound(err))
}

func TestCreateWithKeyExist(t *testing.T) {
	integration.BeforeTestExternal(t)
	ctx, etcdCli, etcdMockCluster = testSetup(t)
	defer etcdMockCluster.Terminate(t)
	obj := "existtest"
	key := "binlogexist/exist"

	etcdClient := etcdMockCluster.RandClient()
	_, err := etcdClient.KV.Put(ctx, key, obj, nil...)
	require.NoError(t, err)

	_, err = etcdCli.Create(ctx, key, obj, nil)
	require.True(t, errors.IsAlreadyExists(err))
}

func TestUpdate(t *testing.T) {
	integration.BeforeTestExternal(t)
	ctx, etcdCli, etcdMockCluster = testSetup(t)
	defer etcdMockCluster.Terminate(t)
	obj1 := "updatetest"
	obj2 := "updatetest2"
	key := "binlogupdate/updatekey"

	lcr, err := etcdCli.client.Lease.Grant(ctx, 2)
	require.NoError(t, err)

	opts := []clientv3.OpOption{clientv3.WithLease(lcr.ID)}
	revision0, err := etcdCli.Create(ctx, key, obj1, opts)
	require.NoError(t, err)

	res, revision1, err := etcdCli.Get(ctx, key)
	require.NoError(t, err)
	require.Equal(t, obj1, string(res))
	require.Equal(t, revision1, revision0)

	time.Sleep(time.Second)

	err = etcdCli.Update(ctx, key, obj2, 3)
	require.NoError(t, err)

	time.Sleep(2 * time.Second)

	// the new revision should greater than the old
	res, revision2, err := etcdCli.Get(ctx, key)
	require.NoError(t, err)
	require.Equal(t, obj2, string(res))
	require.Greater(t, revision2, revision1)

	time.Sleep(2 * time.Second)
	_, _, err = etcdCli.Get(ctx, key)
	require.True(t, errors.IsNotFound(err))
}

func TestUpdateOrCreate(t *testing.T) {
	integration.BeforeTestExternal(t)
	ctx, etcdCli, etcdMockCluster = testSetup(t)
	defer etcdMockCluster.Terminate(t)
	obj := "updatetest"
	key := "binlogupdatecreate/updatekey"
	err := etcdCli.UpdateOrCreate(ctx, key, obj, 3)
	require.NoError(t, err)
}

func TestList(t *testing.T) {
	integration.BeforeTestExternal(t)
	ctx, etcdCli, etcdMockCluster = testSetup(t)
	defer etcdMockCluster.Terminate(t)
	key := "binloglist/testkey"

	k1 := key + "/level1"
	k2 := key + "/level2"
	k3 := key + "/level3"
	k11 := key + "/level1/level1"

	revision1, err := etcdCli.Create(ctx, k1, k1, nil)
	require.NoError(t, err)

	revision2, err := etcdCli.Create(ctx, k2, k2, nil)
	require.NoError(t, err)
	require.True(t, revision2 > revision1)

	revision3, err := etcdCli.Create(ctx, k3, k3, nil)
	require.NoError(t, err)
	require.True(t, revision3 > revision2)

	revision4, err := etcdCli.Create(ctx, k11, k11, nil)
	require.NoError(t, err)
	require.True(t, revision4 > revision3)

	root, revision5, err := etcdCli.List(ctx, key)
	require.NoError(t, err)
	require.Equal(t, k1, string(root.Childs["level1"].Value))
	require.Equal(t, k11, string(root.Childs["level1"].Childs["level1"].Value))
	require.Equal(t, k2, string(root.Childs["level2"].Value))
	require.Equal(t, k3, string(root.Childs["level3"].Value))

	// the revision of list should equal to the latest update's revision
	_, revision6, err := etcdCli.Get(ctx, k11)
	require.NoError(t, err)
	require.Equal(t, revision6, revision5)
}

func TestDelete(t *testing.T) {
	integration.BeforeTestExternal(t)
	ctx, etcdCli, etcdMockCluster = testSetup(t)
	defer etcdMockCluster.Terminate(t)
	key := "binlogdelete/testkey"
	keys := []string{key + "/level1", key + "/level2", key + "/level1" + "/level1"}
	for _, k := range keys {
		_, err := etcdCli.Create(ctx, k, k, nil)
		require.NoError(t, err)
	}

	root, _, err := etcdCli.List(ctx, key)
	require.NoError(t, err)
	require.Len(t, root.Childs, 2)

	err = etcdCli.Delete(ctx, keys[1], false)
	require.NoError(t, err)

	root, _, err = etcdCli.List(ctx, key)
	require.NoError(t, err)
	require.Len(t, root.Childs, 1)

	err = etcdCli.Delete(ctx, key, true)
	require.NoError(t, err)

	root, _, err = etcdCli.List(ctx, key)
	require.NoError(t, err)
	require.Len(t, root.Childs, 0)
}

func TestDoTxn(t *testing.T) {
	integration.BeforeTestExternal(t)
	ctx, etcdCli, etcdMockCluster = testSetup(t)
	defer etcdMockCluster.Terminate(t)
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
	require.NoError(t, err)

	value1, revision1, err := etcdCli.Get(context.Background(), "test1")
	require.NoError(t, err)
	require.Equal(t, "1", string(value1))
	require.Equal(t, revision, revision1)

	value2, revision2, err := etcdCli.Get(context.Background(), "test2")
	require.NoError(t, err)
	require.Equal(t, "2", string(value2))
	require.Equal(t, revision, revision2)

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
	require.NoError(t, err)

	_, _, err = etcdCli.Get(context.Background(), "test1")
	require.Regexp(t, ".* not found", err)

	value2, revision2, err = etcdCli.Get(context.Background(), "test2")
	require.NoError(t, err)
	require.Equal(t, "22", string(value2))
	require.Equal(t, revision, revision2)

	value3, revision3, err := etcdCli.Get(context.Background(), "test3")
	require.NoError(t, err)
	require.Equal(t, "3", string(value3))
	require.Equal(t, revision, revision3)

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
	require.NoError(t, err)

	value4, revision4, err := etcdCli.Get(context.Background(), "test4")
	require.NoError(t, err)
	require.Equal(t, "4", string(value4))
	require.Equal(t, revision, revision4)

	value5, revision5, err := etcdCli.Get(context.Background(), "test5")
	require.NoError(t, err)
	require.Equal(t, "5", string(value5))
	require.Equal(t, revision, revision5)

	// sleep 2 seconds and this key will be deleted
	time.Sleep(2 * time.Second)
	_, _, err = etcdCli.Get(context.Background(), "test4")
	require.Regexp(t, ".* not found", err)

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
	require.Regexp(t, "do transaction failed.*", err)

	_, _, err = etcdCli.Get(context.Background(), "test4")
	require.Regexp(t, ".* not found", err)

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
	require.Regexp(t, "etcdserver: duplicate key given in txn request", err)

	_, _, err = etcdCli.Get(context.Background(), "test6")
	require.Regexp(t, ".* not found", err)

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
	require.Regexp(t, "do transaction failed.*", err)

	value2, _, err = etcdCli.Get(context.Background(), "test2")
	require.NoError(t, err)
	require.Equal(t, "22", string(value2))

	value5, _, err = etcdCli.Get(context.Background(), "test5")
	require.NoError(t, err)
	require.Equal(t, "5", string(value5))

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
	require.NoError(t, err)

	value8, _, err := etcdCli.Get(context.Background(), "test8")
	require.NoError(t, err)
	require.Equal(t, "8", string(value8))

	// case8: do transaction failed because can't set TTL for delete operation
	ops = []*Operation{
		{
			Tp:  DeleteOp,
			Key: "test8",
			TTL: 1,
		},
	}

	_, err = etcdCli.DoTxn(context.Background(), ops)
	require.Regexp(t, "unexpected TTL in delete operation", err)
}

func testSetup(t *testing.T) (context.Context, *Client, *integration.ClusterV3) {
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	etcd := NewClient(cluster.RandClient(), "binlog")
	return context.Background(), etcd, cluster
}

func testSetupOriginal(t *testing.T) (context.Context, *clientv3.Client, *integration.ClusterV3) {
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	return context.Background(), cluster.RandClient(), cluster
}

func TestSetEtcdCliByNamespace(t *testing.T) {
	integration.BeforeTest(t)
	ctx, origEtcdCli, etcdMockCluster := testSetupOriginal(t)
	defer etcdMockCluster.Terminate(t)

	namespacePrefix := "testNamespace/"
	key := "testkey"
	obj := "test"

	unprefixedKV := origEtcdCli.KV
	cliNamespace := origEtcdCli
	SetEtcdCliByNamespace(cliNamespace, namespacePrefix)

	_, err := cliNamespace.Put(ctx, key, obj)
	require.NoError(t, err)

	// verify that kv pair is empty before set
	getResp, err := unprefixedKV.Get(ctx, namespacePrefix+key)
	require.NoError(t, err)
	require.Len(t, getResp.Kvs, 1)

}
