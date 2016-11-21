// Copyright 2016 PingCAP, Inc.
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
	"math/rand"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/store/tikv/mock-tikv"
	"golang.org/x/net/context"
)

type testCommitterSuite struct {
	cluster *mocktikv.Cluster
	store   *tikvStore
}

var _ = Suite(&testCommitterSuite{})

func (s *testCommitterSuite) SetUpTest(c *C) {
	s.cluster = mocktikv.NewCluster()
	mocktikv.BootstrapWithMultiRegions(s.cluster, []byte("a"), []byte("b"), []byte("c"))
	mvccStore := mocktikv.NewMvccStore()
	client := mocktikv.NewRPCClient(s.cluster, mvccStore)
	store, err := newTikvStore("mock-tikv-store", mocktikv.NewPDClient(s.cluster), client, false)
	c.Assert(err, IsNil)
	s.store = store
}

func (s *testCommitterSuite) begin(c *C) *tikvTxn {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	return txn.(*tikvTxn)
}

func (s *testCommitterSuite) checkValues(c *C, m map[string]string) {
	txn := s.begin(c)
	for k, v := range m {
		val, err := txn.Get([]byte(k))
		c.Assert(err, IsNil)
		c.Assert(string(val), Equals, v)
	}
}

func (s *testCommitterSuite) mustCommit(c *C, m map[string]string) {
	txn := s.begin(c)
	for k, v := range m {
		err := txn.Set([]byte(k), []byte(v))
		c.Assert(err, IsNil)
	}
	err := txn.Commit()
	c.Assert(err, IsNil)

	s.checkValues(c, m)
}

func randKV(keyLen, valLen int) (string, string) {
	const letters = "abc"
	k, v := make([]byte, keyLen), make([]byte, valLen)
	for i := range k {
		k[i] = letters[rand.Intn(len(letters))]
	}
	for i := range v {
		v[i] = letters[rand.Intn(len(letters))]
	}
	return string(k), string(v)
}

func (s *testCommitterSuite) TestCommitMultipleRegions(c *C) {
	m := make(map[string]string)
	for i := 0; i < 100; i++ {
		k, v := randKV(10, 10)
		m[k] = v
	}
	s.mustCommit(c, m)

	// Test big values.
	m = make(map[string]string)
	for i := 0; i < 50; i++ {
		k, v := randKV(11, txnCommitBatchSize/7)
		m[k] = v
	}
	s.mustCommit(c, m)
}

func (s *testCommitterSuite) TestCommitRollback(c *C) {
	s.mustCommit(c, map[string]string{
		"a": "a",
		"b": "b",
		"c": "c",
	})

	txn := s.begin(c)
	txn.Set([]byte("a"), []byte("a1"))
	txn.Set([]byte("b"), []byte("b1"))
	txn.Set([]byte("c"), []byte("c1"))

	s.mustCommit(c, map[string]string{
		"c": "c2",
	})

	err := txn.Commit()
	c.Assert(err, NotNil)

	s.checkValues(c, map[string]string{
		"a": "a",
		"b": "b",
		"c": "c2",
	})
}

func (s *testCommitterSuite) TestPrewriteRollback(c *C) {
	s.mustCommit(c, map[string]string{
		"a": "a0",
		"b": "b0",
	})

	ctx := context.Background()
	txn1 := s.begin(c)
	err := txn1.Set([]byte("a"), []byte("a1"))
	c.Assert(err, IsNil)
	err = txn1.Set([]byte("b"), []byte("b1"))
	c.Assert(err, IsNil)
	committer, err := newTwoPhaseCommitter(txn1)
	c.Assert(err, IsNil)
	err = committer.prewriteKeys(NewBackoffer(prewriteMaxBackoff, ctx), committer.keys)
	c.Assert(err, IsNil)

	txn2 := s.begin(c)
	v, err := txn2.Get([]byte("a"))
	c.Assert(err, IsNil)
	c.Assert(v, BytesEquals, []byte("a0"))

	err = committer.prewriteKeys(NewBackoffer(prewriteMaxBackoff, ctx), committer.keys)
	if err != nil {
		// Retry.
		txn1 = s.begin(c)
		err = txn1.Set([]byte("a"), []byte("a1"))
		c.Assert(err, IsNil)
		err = txn1.Set([]byte("b"), []byte("b1"))
		c.Assert(err, IsNil)
		committer, err = newTwoPhaseCommitter(txn1)
		c.Assert(err, IsNil)
		err = committer.prewriteKeys(NewBackoffer(prewriteMaxBackoff, ctx), committer.keys)
		c.Assert(err, IsNil)
	}
	committer.commitTS, err = s.store.oracle.GetTimestamp()
	c.Assert(err, IsNil)
	err = committer.commitKeys(NewBackoffer(commitMaxBackoff, ctx), [][]byte{[]byte("a")})
	c.Assert(err, IsNil)

	txn3 := s.begin(c)
	v, err = txn3.Get([]byte("b"))
	c.Assert(err, IsNil)
	c.Assert(v, BytesEquals, []byte("b1"))
}

func (s *testCommitterSuite) TestContextCancel(c *C) {
	txn1 := s.begin(c)
	err := txn1.Set([]byte("a"), []byte("a1"))
	c.Assert(err, IsNil)
	err = txn1.Set([]byte("b"), []byte("b1"))
	c.Assert(err, IsNil)
	committer, err := newTwoPhaseCommitter(txn1)
	c.Assert(err, IsNil)

	bo := NewBackoffer(prewriteMaxBackoff, context.Background())
	cancel := bo.WithCancel()
	cancel() // cancel the context
	err = committer.prewriteKeys(bo, committer.keys)
	c.Assert(errors.Cause(err), Equals, context.Canceled)
}
