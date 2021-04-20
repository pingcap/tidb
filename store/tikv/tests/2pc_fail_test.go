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

package tikv_test

import (
	"context"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/kv"
)

// TestFailCommitPrimaryRpcErrors tests rpc errors are handled properly when
// committing primary region task.
func (s *testCommitterSuite) TestFailCommitPrimaryRpcErrors(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/mockstore/mocktikv/rpcCommitResult", `return("timeout")`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/mockstore/mocktikv/rpcCommitResult"), IsNil)
	}()
	// The rpc error will be wrapped to ErrResultUndetermined.
	t1 := s.begin(c)
	err := t1.Set([]byte("a"), []byte("a1"))
	c.Assert(err, IsNil)
	err = t1.Commit(context.Background())
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, terror.ErrResultUndetermined), IsTrue, Commentf("%s", errors.ErrorStack(err)))

	// We don't need to call "Rollback" after "Commit" fails.
	err = t1.Rollback()
	c.Assert(err, Equals, kv.ErrInvalidTxn)
}

// TestFailCommitPrimaryRegionError tests RegionError is handled properly when
// committing primary region task.
func (s *testCommitterSuite) TestFailCommitPrimaryRegionError(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/mockstore/mocktikv/rpcCommitResult", `return("notLeader")`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/mockstore/mocktikv/rpcCommitResult"), IsNil)
	}()
	// Ensure it returns the original error without wrapped to ErrResultUndetermined
	// if it exceeds max retry timeout on RegionError.
	t2 := s.begin(c)
	err := t2.Set([]byte("b"), []byte("b1"))
	c.Assert(err, IsNil)
	err = t2.Commit(context.Background())
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorNotEqual(err, terror.ErrResultUndetermined), IsTrue)
}

// TestFailCommitPrimaryRPCErrorThenRegionError tests the case when commit first
// receive a rpc timeout, then region errors afterwrards.
func (s *testCommitterSuite) TestFailCommitPrimaryRPCErrorThenRegionError(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/mockstore/mocktikv/rpcCommitResult", `1*return("timeout")->return("notLeader")`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/mockstore/mocktikv/rpcCommitResult"), IsNil)
	}()
	// The region error will be wrapped to ErrResultUndetermined.
	t1 := s.begin(c)
	err := t1.Set([]byte("a"), []byte("a1"))
	c.Assert(err, IsNil)
	err = t1.Commit(context.Background())
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, terror.ErrResultUndetermined), IsTrue, Commentf("%s", errors.ErrorStack(err)))
}

// TestFailCommitPrimaryKeyError tests KeyError is handled properly when
// committing primary region task.
func (s *testCommitterSuite) TestFailCommitPrimaryKeyError(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/mockstore/mocktikv/rpcCommitResult", `return("keyError")`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/mockstore/mocktikv/rpcCommitResult"), IsNil)
	}()
	// Ensure it returns the original error without wrapped to ErrResultUndetermined
	// if it meets KeyError.
	t3 := s.begin(c)
	err := t3.Set([]byte("c"), []byte("c1"))
	c.Assert(err, IsNil)
	err = t3.Commit(context.Background())
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorNotEqual(err, terror.ErrResultUndetermined), IsTrue)
}

func (s *testCommitterSuite) TestFailCommitTimeout(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/mockstore/mocktikv/rpcCommitTimeout", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/mockstore/mocktikv/rpcCommitTimeout"), IsNil)
	}()
	txn := s.begin(c)
	err := txn.Set([]byte("a"), []byte("a1"))
	c.Assert(err, IsNil)
	err = txn.Set([]byte("b"), []byte("b1"))
	c.Assert(err, IsNil)
	err = txn.Set([]byte("c"), []byte("c1"))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, NotNil)

	txn2 := s.begin(c)
	value, err := txn2.Get(context.TODO(), []byte("a"))
	c.Assert(err, IsNil)
	c.Assert(len(value), Greater, 0)
	_, err = txn2.Get(context.TODO(), []byte("b"))
	c.Assert(err, IsNil)
	c.Assert(len(value), Greater, 0)
}
