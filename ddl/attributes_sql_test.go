// Copyright 2021 PingCAP, Inc.
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
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testDBSuite8) TestAlterTableAttributes(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	defer tk.MustExec("drop table if exists t1")

	tk.MustExec(`create table t1 (c int);`)

	// normal cases
	_, err := tk.Exec(`alter table t1 attributes="nomerge";`)
	c.Assert(err, IsNil)
	_, err = tk.Exec(`alter table t1 attributes="nomerge,somethingelse";`)
	c.Assert(err, IsNil)

	// space cases
	_, err = tk.Exec(`alter table t1 attributes=" nomerge ";`)
	c.Assert(err, IsNil)
	_, err = tk.Exec(`alter table t1 attributes=" nomerge , somethingelse ";`)
	c.Assert(err, IsNil)

	// without equal
	_, err = tk.Exec(`alter table t1 attributes " nomerge ";`)
	c.Assert(err, IsNil)
	_, err = tk.Exec(`alter table t1 attributes " nomerge , somethingelse ";`)
	c.Assert(err, IsNil)
}
