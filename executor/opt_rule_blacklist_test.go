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

package executor_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSuite2) TestReloadOptRuleBlacklist(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database opt_rule_blacklist")
	tk.MustExec("use opt_rule_blacklist")
	tk.MustExec("create table t (a int)")

	// test disable logical rule.
	tk.MustQuery("desc select * from t where a < 1").Check(testkit.Rows(
		"TableReader_7 3323.33 root data:Selection_6",
		"└─Selection_6 3323.33 cop lt(opt_rule_blacklist.t.a, 1)",
		"  └─TableScan_5 10000.00 cop table:t, range:[-inf,+inf], keep order:false, stats:pseudo"))

	tk.MustExec("insert into mysql.opt_rule_blacklist values('predicate_push_down', 'logical_rule')")
	tk.MustExec("admin reload opt_rule_blacklist")
	tk.MustQuery("desc select * from t where a < 1").Check(testkit.Rows(
		"Selection_5 8000.00 root lt(opt_rule_blacklist.t.a, 1)",
		"└─TableReader_7 10000.00 root data:TableScan_6",
		"  └─TableScan_6 10000.00 cop table:t, range:[-inf,+inf], keep order:false, stats:pseudo"))

	tk.MustExec("delete from mysql.opt_rule_blacklist where name='predicate_push_down'")
	tk.MustExec("delete from mysql.expr_pushdown_blacklist where name='lt'")
	tk.MustExec("admin reload opt_rule_blacklist")
}
