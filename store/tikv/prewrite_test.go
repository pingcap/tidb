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
	. "github.com/pingcap/check"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/store/mockstore/unistore"
)

type testPrewriteSuite struct {
	store *KVStore
}

var _ = Suite(&testPrewriteSuite{})

func (s *testPrewriteSuite) SetUpTest(c *C) {
	client, pdClient, cluster, err := unistore.New("")
	c.Assert(err, IsNil)
	unistore.BootstrapWithSingleStore(cluster)
	store, err := NewTestTiKVStore(client, pdClient, nil, nil, 0)
	c.Assert(err, IsNil)
	s.store = store.(*KVStore)
}

func (s *testPrewriteSuite) TestSetMinCommitTSInAsyncCommit(c *C) {
	t, err := s.store.Begin()
	c.Assert(err, IsNil)
	txn := t.(*tikvTxn)
	err = txn.Set([]byte("k"), []byte("v"))
	c.Assert(err, IsNil)
	committer, err := newTwoPhaseCommitterWithInit(txn, 1)
	c.Assert(err, IsNil)
	committer.useAsyncCommit = 1

	buildRequest := func() *pb.PrewriteRequest {
		batch := batchMutations{mutations: committer.mutations}
		req := committer.buildPrewriteRequest(batch, 1)
		return req.Req.(*pb.PrewriteRequest)
	}

	// no forUpdateTS
	req := buildRequest()
	c.Assert(req.MinCommitTs, Equals, txn.startTS+1)

	// forUpdateTS is set
	committer.forUpdateTS = txn.startTS + (5 << 18)
	req = buildRequest()
	c.Assert(req.MinCommitTs, Equals, committer.forUpdateTS+1)

	// minCommitTS is set
	committer.minCommitTS = txn.startTS + (10 << 18)
	req = buildRequest()
	c.Assert(req.MinCommitTs, Equals, committer.minCommitTS)

}
