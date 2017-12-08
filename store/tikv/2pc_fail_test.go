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

package tikv

import (
	gofail "github.com/coreos/gofail/runtime"
	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/terror"
	goctx "golang.org/x/net/context"
)

// TestFailCommitPrimaryRpcErrors tests rpc errors are handled properly when
// committing primary region task.
func (s *testCommitterSuite) TestFailCommitPrimaryRpcErrors(c *C) {
	gofail.Enable("github.com/pingcap/tidb/store/tikv/mocktikv/rpcCommitResult", `return("timeout")`)
	defer gofail.Disable("github.com/pingcap/tidb/store/tikv/mocktikv/rpcCommitResult")
	// The rpc error will be wrapped to ErrResultUndetermined.
	t1 := s.begin(c)
	err := t1.Set([]byte("a"), []byte("a1"))
	c.Assert(err, IsNil)
	err = t1.Commit(goctx.Background())
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, terror.ErrResultUndetermined), IsTrue, Commentf("%s", errors.ErrorStack(err)))
}

// TestFailCommitPrimaryRegionError tests RegionError is handled properly when
// committing primary region task.
func (s *testCommitterSuite) TestFailCommitPrimaryRegionError(c *C) {
	gofail.Enable("github.com/pingcap/tidb/store/tikv/mocktikv/rpcCommitResult", `return("notLeader")`)
	defer gofail.Disable("github.com/pingcap/tidb/store/tikv/mocktikv/rpcCommitResult")
	// Ensure it returns the original error without wrapped to ErrResultUndetermined
	// if it exceeds max retry timeout on RegionError.
	t2 := s.begin(c)
	err := t2.Set([]byte("b"), []byte("b1"))
	c.Assert(err, IsNil)
	err = t2.Commit(goctx.Background())
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorNotEqual(err, terror.ErrResultUndetermined), IsTrue)
}

// TestFailCommitPrimaryRPCErrorThenRegionError tests the case when commit first
// receive a rpc timeout, then region errors afterwrards.
func (s *testCommitterSuite) TestFailCommitPrimaryRPCErrorThenRegionError(c *C) {
	gofail.Enable("github.com/pingcap/tidb/store/tikv/mocktikv/rpcCommitResult", `1*return("timeout")->return("notLeader")`)
	defer gofail.Disable("github.com/pingcap/tidb/store/tikv/mocktikv/rpcCommitResult")
	// The region error will be wrapped to ErrResultUndetermined.
	t1 := s.begin(c)
	err := t1.Set([]byte("a"), []byte("a1"))
	c.Assert(err, IsNil)
	err = t1.Commit(goctx.Background())
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, terror.ErrResultUndetermined), IsTrue, Commentf("%s", errors.ErrorStack(err)))
}

// TestFailCommitPrimaryKeyError tests KeyError is handled properly when
// committing primary region task.
func (s *testCommitterSuite) TestFailCommitPrimaryKeyError(c *C) {
	gofail.Enable("github.com/pingcap/tidb/store/tikv/mocktikv/rpcCommitResult", `return("keyError")`)
	defer gofail.Disable("github.com/pingcap/tidb/store/tikv/mocktikv/rpcCommitResult")
	// Ensure it returns the original error without wrapped to ErrResultUndetermined
	// if it meets KeyError.
	t3 := s.begin(c)
	err := t3.Set([]byte("c"), []byte("c1"))
	c.Assert(err, IsNil)
	err = t3.Commit(goctx.Background())
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorNotEqual(err, terror.ErrResultUndetermined), IsTrue)
}

func (s *testCommitterSuite) TestFailCommitTimeout(c *C) {
	gofail.Enable("github.com/pingcap/tidb/store/tikv/mocktikv/rpcCommitTimeout", `return(true)`)
	defer gofail.Disable("github.com/pingcap/tidb/store/tikv/mocktikv/rpcCommitTimeout")
	txn := s.begin(c)
	err := txn.Set([]byte("a"), []byte("a1"))
	c.Assert(err, IsNil)
	err = txn.Set([]byte("b"), []byte("b1"))
	c.Assert(err, IsNil)
	err = txn.Set([]byte("c"), []byte("c1"))
	c.Assert(err, IsNil)
	err = txn.Commit(goctx.Background())
	c.Assert(err, NotNil)

	txn2 := s.begin(c)
	value, err := txn2.Get([]byte("a"))
	c.Assert(err, IsNil)
	c.Assert(len(value), Greater, 0)
	_, err = txn2.Get([]byte("b"))
	c.Assert(err, IsNil)
	c.Assert(len(value), Greater, 0)
}
