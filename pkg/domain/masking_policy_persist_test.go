// Copyright 2026 PingCAP, Inc.
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

package domain_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestMaskingPolicyPersistAfterRestart(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	// Create table and masking policy
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int primary key auto_increment, c char(120))")
	tk.MustExec("create masking policy simple_mask on t(c) as c enable")

	// Verify policy shows in show create table
	tk.MustQuery("show create table t").Check(testkit.Rows(
		"t CREATE TABLE `t` (\n" +
			"  `id` int NOT NULL AUTO_INCREMENT,\n" +
			"  `c` char(120) DEFAULT NULL /* MASKING POLICY `simple_mask` ENABLED */,\n" +
			"  PRIMARY KEY (`id`)\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	// Verify policy is in system table
	tk.MustQuery("select policy_name, db_name, table_name, column_name, status from mysql.tidb_masking_policy where policy_name = 'simple_mask'").
		Check(testkit.Rows("simple_mask test t c ENABLED"))

	// Simulate restart by creating a new test kit
	// This should trigger a fresh info schema load
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")

	// BUG: Policy disappears after restart!
	// This test should FAIL until the bug is fixed
	tk2.MustQuery("show create table t").Check(testkit.Rows(
		"t CREATE TABLE `t` (\n" +
			"  `id` int NOT NULL AUTO_INCREMENT,\n" +
			"  `c` char(120) DEFAULT NULL /* MASKING POLICY `simple_mask` ENABLED */,\n" +
			"  PRIMARY KEY (`id`)\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	// Verify policy is still in system table
	tk2.MustQuery("select policy_name, db_name, table_name, column_name, status from mysql.tidb_masking_policy where policy_name = 'simple_mask'").
		Check(testkit.Rows("simple_mask test t c ENABLED"))
}
