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

package ddl_test

import (
	gofail "github.com/etcd-io/gofail/runtime"
	. "github.com/pingcap/check"
	"golang.org/x/net/context"
)

// TestInitializeOffsetAndState tests the case that the column's offset and state don't be initialized in the file of ddl_api.go when
// doing the operation of 'modify column'.
func (s *testStateChangeSuite) TestInitializeOffsetAndState(c *C) {
	_, err := s.se.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "create table t(a int, b int, c int)")
	c.Assert(err, IsNil)
	defer s.se.Execute(context.Background(), "drop table t")

	gofail.Enable("github.com/pingcap/tidb/ddl/uninitializedOffsetAndState", `return(true)`)
	_, err = s.se.Execute(context.Background(), "ALTER TABLE t MODIFY COLUMN b int FIRST;")
	c.Assert(err, IsNil)
	gofail.Disable("github.com/pingcap/tidb/ddl/uninitializedOffsetAndState")
}
