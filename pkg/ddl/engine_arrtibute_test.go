// Copyright 2025 PingCAP, Inc.
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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package ddl_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/testkit"
)

func TestEngineAttribute(t *testing.T) {
	if !kerneltype.IsNextGen() {
		t.Skip("skip test not in nextgen")
	}
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	// Engine attribute in table.
	tk.MustExec("CREATE TABLE test.t2 (id INT PRIMARY KEY, value VARCHAR(16383));")
	tk.MustQuery("SELECT TIDB_STORAGE_CLASS FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 't2'").
		Check(testkit.Rows(""))
	tk.MustQuery("SHOW CREATE TABLE test.t2;").
		CheckNotContain("ENGINE_ATTRIBUTE")

	tk.MustExec("CREATE TABLE test.t3 (id INT PRIMARY KEY, value VARCHAR(16383)) ENGINE_ATTRIBUTE = '{\"storage_class\": \"IA\"}';")
	tk.MustQuery("SELECT TIDB_STORAGE_CLASS FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 't3'").
		Check(testkit.Rows("IA"))
	tk.MustQuery("SHOW CREATE TABLE test.t3;").
		CheckContain("ENGINE_ATTRIBUTE='{\"storage_class\": \"IA\"}'")

	tk.MustExec("ALTER TABLE test.t3 ENGINE_ATTRIBUTE = '{\"storage_class\": \"STANDARD\"}';")
	tk.MustQuery("SELECT TIDB_STORAGE_CLASS FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 't3'").
		Check(testkit.Rows("STANDARD"))
	tk.MustQuery("SHOW CREATE TABLE test.t3;").
		CheckContain("ENGINE_ATTRIBUTE='{\"storage_class\": \"STANDARD\"}'")

	// Engine attribute in partition.
	tk.MustExec("CREATE TABLE test.t5 (id INT PRIMARY KEY, value VARCHAR(16383)) " +
		"ENGINE_ATTRIBUTE = '{\"storage_class\": {\"tier\":\"IA\",\"names_in\":[\"p0\", \"p1\"]}}' " +
		"PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (100));")
	tk.MustQuery("SELECT TIDB_STORAGE_CLASS FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_NAME = 't5' AND PARTITION_NAME = 'p0'").
		Check(testkit.Rows("IA"))
	tk.MustQuery("SHOW CREATE TABLE test.t5;").
		CheckContain("ENGINE_ATTRIBUTE='{\"storage_class\": {\"tier\":\"IA\",\"names_in\":[\"p0\", \"p1\"]}}'")

	tk.MustExec("ALTER TABLE test.t5 ADD PARTITION (PARTITION p1 VALUES LESS THAN (200));")
	tk.MustQuery("SELECT TIDB_STORAGE_CLASS FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_NAME = 't5' AND PARTITION_NAME = 'p1'").
		Check(testkit.Rows("IA"))

	tk.MustExec("ALTER TABLE test.t5 ADD PARTITION (PARTITION p2 VALUES LESS THAN (300));")
	tk.MustQuery("SELECT TIDB_STORAGE_CLASS FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_NAME = 't5' AND PARTITION_NAME = 'p2'").
		Check(testkit.Rows("STANDARD"))

	tk.MustExec("ALTER TABLE test.t5 ENGINE_ATTRIBUTE = '{\"storage_class\": {\"tier\":\"IA\",\"names_in\":[\"p2\"]}}';")
	tk.MustQuery("SELECT PARTITION_NAME, TIDB_STORAGE_CLASS FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_NAME = 't5'").
		Check(testkit.Rows("p0 STANDARD", "p1 STANDARD", "p2 IA"))
	tk.MustQuery("SHOW CREATE TABLE test.t5;").
		CheckContain("ENGINE_ATTRIBUTE='{\"storage_class\": {\"tier\":\"IA\",\"names_in\":[\"p2\"]}}'")
}

func TestEngineAttributeNotIsNextGen(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("skip test in nextgen")
	}
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	// Engine attribute in table.
	tk.MustExec("CREATE TABLE test.t2 (id INT PRIMARY KEY, value VARCHAR(16383));")
	tk.MustQuery("SELECT TIDB_STORAGE_CLASS FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 't2'").
		Check(testkit.Rows("<nil>"))

	tk.MustExecToErr("CREATE TABLE test.t3 (id INT PRIMARY KEY, value VARCHAR(16383)) ENGINE_ATTRIBUTE = '{\"storage_class\": \"IA\"}';")
	tk.MustQuery("SELECT TIDB_STORAGE_CLASS FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 't3'").
		Check(testkit.Rows())

	tk.MustExecToErr("ALTER TABLE test.t3 ENGINE_ATTRIBUTE = '{\"storage_class\": \"STANDARD\"}';")
	tk.MustQuery("SELECT TIDB_STORAGE_CLASS FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 't3'").
		Check(testkit.Rows())

	// Engine attribute in partition.
	tk.MustExecToErr("CREATE TABLE test.t5 (id INT PRIMARY KEY, value VARCHAR(16383)) " +
		"ENGINE_ATTRIBUTE = '{\"storage_class\": {\"tier\":\"IA\",\"names_in\":[\"p0\", \"p1\"]}}' " +
		"PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (100));")
	tk.MustQuery("SELECT TIDB_STORAGE_CLASS FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_NAME = 't5' AND PARTITION_NAME = 'p0'").
		Check(testkit.Rows())

	tk.MustExecToErr("ALTER TABLE test.t5 ADD PARTITION (PARTITION p1 VALUES LESS THAN (200));")
	tk.MustQuery("SELECT TIDB_STORAGE_CLASS FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_NAME = 't5' AND PARTITION_NAME = 'p1'").
		Check(testkit.Rows())

	tk.MustExecToErr("ALTER TABLE test.t5 ADD PARTITION (PARTITION p2 VALUES LESS THAN (300));")
	tk.MustQuery("SELECT TIDB_STORAGE_CLASS FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_NAME = 't5' AND PARTITION_NAME = 'p2'").
		Check(testkit.Rows())

	tk.MustExecToErr("ALTER TABLE test.t5 ENGINE_ATTRIBUTE = '{\"storage_class\": {\"tier\":\"IA\",\"names_in\":[\"p2\"]}}';")
	tk.MustQuery("SELECT PARTITION_NAME, TIDB_STORAGE_CLASS FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_NAME = 't5'").
		Check(testkit.Rows())
}
