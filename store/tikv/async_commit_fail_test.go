// Copyright 2020 PingCAP, Inc.
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

package tikv

import (
	"bytes"
	"context"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/mockstore/cluster"
	"github.com/pingcap/tidb/store/mockstore/unistore"
)

type testAsyncCommitFailSuite struct {
	OneByOneSuite
	cluster cluster.Cluster
	store   *tikvStore
}

var _ = SerialSuites(&testAsyncCommitFailSuite{})

func (s *testAsyncCommitFailSuite) SetUpTest(c *C) {
	client, pdClient, cluster, err := unistore.New("")
	c.Assert(err, IsNil)
	unistore.BootstrapWithSingleStore(cluster)
	s.cluster = cluster
	store, err := NewTestTiKVStore(client, pdClient, nil, nil, 0)
	c.Assert(err, IsNil)

	s.store = store.(*tikvStore)
}

// TestFailCommitPrimaryRpcErrors tests rpc errors are handled properly when
// committing primary region task.
func (s *testAsyncCommitFailSuite) TestFailAsyncCommitPrewriteRpcErrors(c *C) {
	defer config.RestoreFunc()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.EnableAsyncCommit = true
	})
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/noRetryOnRpcError", "return(true)"), IsNil)
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/mockstore/unistore/rpcPrewriteTimeout", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/mockstore/unistore/rpcPrewriteTimeout"), IsNil)
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/noRetryOnRpcError"), IsNil)
	}()
	// The rpc error will be wrapped to ErrResultUndetermined.
	t1, err := s.store.Begin()
	c.Assert(err, IsNil)
	err = t1.Set([]byte("a"), []byte("a1"))
	c.Assert(err, IsNil)
	ctx := context.WithValue(context.Background(), sessionctx.ConnID, uint64(1))
	err = t1.Commit(ctx)
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, terror.ErrResultUndetermined), IsTrue, Commentf("%s", errors.ErrorStack(err)))

	// We don't need to call "Rollback" after "Commit" fails.
	err = t1.Rollback()
	c.Assert(err, Equals, kv.ErrInvalidTxn)

	// Create a new transaction to check. The previous transaction should actually commit.
	t2, err := s.store.Begin()
	c.Assert(err, IsNil)
	res, err := t2.Get(context.Background(), []byte("a"))
	c.Assert(err, IsNil)
	c.Assert(bytes.Equal(res, []byte("a1")), IsTrue)
}
