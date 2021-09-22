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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSuite5) TestShowPlacement(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("drop placement policy if exists p1")

	tk.MustExec("create placement policy pa1 " +
		"PRIMARY_REGION=\"cn-east-1\" " +
		"REGIONS=\"cn-east-1,cn-east-2\"" +
		"SCHEDULE=\"EVEN\"")
	defer tk.MustExec("drop placement policy pa1")

	tk.MustExec("create placement policy pa2 " +
		"LEADER_CONSTRAINTS=\"[+region=us-east-1]\" " +
		"FOLLOWER_CONSTRAINTS=\"[+region=us-east-2]\" " +
		"FOLLOWERS=3")
	defer tk.MustExec("drop placement policy pa2")

	tk.MustExec("create placement policy pb1 " +
		"VOTER_CONSTRAINTS=\"[+region=bj]\" " +
		"LEARNER_CONSTRAINTS=\"[+region=sh]\" " +
		"CONSTRAINTS=\"[+disk=ssd]\"" +
		"VOTERS=5 " +
		"LEARNERS=3")
	defer tk.MustExec("drop placement policy pb1")

	tk.MustQuery("show placement").Check(testkit.Rows(
		"POLICY pa1 PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east-2\" SCHEDULE=\"EVEN\" SCHEDULED",
		"POLICY pa2 LEADER_CONSTRAINTS=\"[+region=us-east-1]\" FOLLOWERS=3 FOLLOWER_CONSTRAINTS=\"[+region=us-east-2]\" SCHEDULED",
		"POLICY pb1 CONSTRAINTS=\"[+disk=ssd]\" VOTERS=5 VOTER_CONSTRAINTS=\"[+region=bj]\" LEARNERS=3 LEARNER_CONSTRAINTS=\"[+region=sh]\" SCHEDULED",
	))

	tk.MustQuery("show placement like 'POLICY%'").Check(testkit.Rows(
		"POLICY pa1 PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east-2\" SCHEDULE=\"EVEN\" SCHEDULED",
		"POLICY pa2 LEADER_CONSTRAINTS=\"[+region=us-east-1]\" FOLLOWERS=3 FOLLOWER_CONSTRAINTS=\"[+region=us-east-2]\" SCHEDULED",
		"POLICY pb1 CONSTRAINTS=\"[+disk=ssd]\" VOTERS=5 VOTER_CONSTRAINTS=\"[+region=bj]\" LEARNERS=3 LEARNER_CONSTRAINTS=\"[+region=sh]\" SCHEDULED",
	))

	tk.MustQuery("show placement like 'POLICY pa%'").Check(testkit.Rows(
		"POLICY pa1 PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east-2\" SCHEDULE=\"EVEN\" SCHEDULED",
		"POLICY pa2 LEADER_CONSTRAINTS=\"[+region=us-east-1]\" FOLLOWERS=3 FOLLOWER_CONSTRAINTS=\"[+region=us-east-2]\" SCHEDULED",
	))

	tk.MustQuery("show placement where Target='POLICY pb1'").Check(testkit.Rows(
		"POLICY pb1 CONSTRAINTS=\"[+disk=ssd]\" VOTERS=5 VOTER_CONSTRAINTS=\"[+region=bj]\" LEARNERS=3 LEARNER_CONSTRAINTS=\"[+region=sh]\" SCHEDULED",
	))
}
