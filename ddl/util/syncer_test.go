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

package util_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"
	. "github.com/pingcap/tidb/ddl"
	. "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/owner"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/testleak"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/etcdserver"
	"go.etcd.io/etcd/integration"
	"go.etcd.io/etcd/mvcc/mvccpb"
	goctx "golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

const minInterval = 10 * time.Nanosecond // It's used to test timeout.

func (s *testSuite) TestSyncerSimple(c *C) {
	testLease := 5 * time.Millisecond
	origin := CheckVersFirstWaitTime
	CheckVersFirstWaitTime = 0
	defer func() {
		CheckVersFirstWaitTime = origin
	}()

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

	// for init function
	err = d.SchemaSyncer().Init(ctx)
	c.Assert(err, IsNil)
	resp, err := cli.Get(ctx, DDLAllSchemaVersions, clientv3.WithPrefix())
	c.Assert(err, IsNil)
	key := DDLAllSchemaVersions + "/" + d.OwnerManager().ID()
	checkRespKV(c, 1, key, InitialVersion, resp.Kvs...)
	// for MustGetGlobalVersion function
	globalVer, err := d.SchemaSyncer().MustGetGlobalVersion(ctx)
	c.Assert(err, IsNil)
	c.Assert(InitialVersion, Equals, fmt.Sprintf("%d", globalVer))
	childCtx, _ := goctx.WithTimeout(ctx, minInterval)
	_, err = d.SchemaSyncer().MustGetGlobalVersion(childCtx)
	c.Assert(isTimeoutError(err), IsTrue)

	d1 := NewDDL(
		ctx,
		WithEtcdClient(cli),
		WithStore(store),
		WithLease(testLease),
	)
	defer d1.Stop()
	err = d1.SchemaSyncer().Init(ctx)
	c.Assert(err, IsNil)

	// for watchCh
	wg := sync.WaitGroup{}
	wg.Add(1)
	currentVer := int64(123)
	go func() {
		defer wg.Done()
		select {
		case resp := <-d.SchemaSyncer().GlobalVersionCh():
			c.Assert(len(resp.Events), GreaterEqual, 1)
			checkRespKV(c, 1, DDLGlobalSchemaVersion, fmt.Sprintf("%v", currentVer), resp.Events[0].Kv)
		case <-time.After(3 * time.Second):
			c.Fail()
		}
	}()

	// for update latestSchemaVersion
	err = d.SchemaSyncer().OwnerUpdateGlobalVersion(ctx, currentVer)
	c.Assert(err, IsNil)

	wg.Wait()

	// for CheckAllVersions
	childCtx, cancel := goctx.WithTimeout(ctx, 200*time.Millisecond)
	err = d.SchemaSyncer().OwnerCheckAllVersions(childCtx, currentVer)
	c.Assert(err, NotNil)
	cancel()

	// for UpdateSelfVersion
	err = d.SchemaSyncer().UpdateSelfVersion(context.Background(), currentVer)
	c.Assert(err, IsNil)
	err = d1.SchemaSyncer().UpdateSelfVersion(context.Background(), currentVer)
	c.Assert(err, IsNil)
	childCtx, _ = goctx.WithTimeout(ctx, minInterval)
	err = d1.SchemaSyncer().UpdateSelfVersion(childCtx, currentVer)
	c.Assert(isTimeoutError(err), IsTrue)

	// for CheckAllVersions
	err = d.SchemaSyncer().OwnerCheckAllVersions(context.Background(), currentVer-1)
	c.Assert(err, IsNil)
	err = d.SchemaSyncer().OwnerCheckAllVersions(context.Background(), currentVer)
	c.Assert(err, IsNil)
	childCtx, _ = goctx.WithTimeout(ctx, minInterval)
	err = d.SchemaSyncer().OwnerCheckAllVersions(childCtx, currentVer)
	c.Assert(isTimeoutError(err), IsTrue)

	// for StartCleanWork
	go d.SchemaSyncer().StartCleanWork()
	ttl := 10
	// Make sure NeededCleanTTL > ttl, then we definitely clean the ttl.
	NeededCleanTTL = int64(11)
	ttlKey := "session_ttl_key"
	ttlVal := "session_ttl_val"
	session, err := owner.NewSession(ctx, "", cli, owner.NewSessionDefaultRetryCnt, ttl)
	c.Assert(err, IsNil)
	err = PutKVToEtcd(context.Background(), cli, 5, ttlKey, ttlVal, clientv3.WithLease(session.Lease()))
	c.Assert(err, IsNil)
	// Make sure the ttlKey is exist in etcd.
	resp, err = cli.Get(ctx, ttlKey)
	c.Assert(err, IsNil)
	checkRespKV(c, 1, ttlKey, ttlVal, resp.Kvs...)
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
	c.Assert(notifiedCnt, Equals, 3)
	// Make sure the ttlKey is removed in etcd.
	resp, err = cli.Get(ctx, ttlKey)
	c.Assert(err, IsNil)
	checkRespKV(c, 0, ttlKey, "", resp.Kvs...)

	// for RemoveSelfVersionPath
	resp, err = cli.Get(goctx.Background(), key)
	c.Assert(err, IsNil)
	currVer := fmt.Sprintf("%v", currentVer)
	checkRespKV(c, 1, key, currVer, resp.Kvs...)
	d.SchemaSyncer().RemoveSelfVersionPath()
	resp, err = cli.Get(goctx.Background(), key)
	c.Assert(err, IsNil)
	c.Assert(len(resp.Kvs), Equals, 0)
}

func isTimeoutError(err error) bool {
	if terror.ErrorEqual(err, goctx.DeadlineExceeded) || status.Code(errors.Cause(err)) == codes.DeadlineExceeded ||
		terror.ErrorEqual(err, etcdserver.ErrTimeout) {
		return true
	}
	return false
}

func checkRespKV(c *C, kvCount int, key, val string, kvs ...*mvccpb.KeyValue) {
	c.Assert(len(kvs), Equals, kvCount)
	if kvCount == 0 {
		return
	}

	kv := kvs[0]
	c.Assert(string(kv.Key), Equals, key)
	c.Assert(string(kv.Value), Equals, val)
}
