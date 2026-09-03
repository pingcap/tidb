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

package index

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

// Benchmarks for index pruning on a multi-tenant schema where every secondary
// index leads with the clustered-key prefix column (54 indexes, mirroring the
// workload that motivated the clustered-prefix discount in
// PruneIndexesByWhereAndOrder). The NoPrune variants run with
// tidb_opt_index_prune_threshold=-1 to measure what stage-1 pruning saves
// overall.

const sharedPrefixBenchDDL = `CREATE TABLE obj (
  id binary(16) NOT NULL,
  workspace_id varchar(255) NOT NULL,
  sequential_id bigint DEFAULT NULL,
  label varchar(255) DEFAULT NULL,
  obj_type_id binary(16) NOT NULL,
  schema_id binary(16) NOT NULL,
  external_id text DEFAULT NULL,
  hash_key varchar(255) DEFAULT NULL,
  monolith_id bigint DEFAULT NULL,
  created_on timestamp DEFAULT CURRENT_TIMESTAMP,
  updated_on timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  group_values_keys varchar(500) DEFAULT NULL,
  numeric_value_1 decimal(65,30) DEFAULT NULL,
  numeric_value_2 decimal(65,30) DEFAULT NULL,
  numeric_value_3 decimal(65,30) DEFAULT NULL,
  numeric_value_4 decimal(65,30) DEFAULT NULL,
  numeric_value_5 decimal(65,30) DEFAULT NULL,
  numeric_value_6 decimal(65,30) DEFAULT NULL,
  numeric_value_7 decimal(65,30) DEFAULT NULL,
  numeric_value_8 decimal(65,30) DEFAULT NULL,
  numeric_value_9 decimal(65,30) DEFAULT NULL,
  text_value_1 text, text_value_2 text, text_value_3 text, text_value_4 text,
  text_value_5 text, text_value_6 text, text_value_7 text, text_value_8 text,
  text_value_9 text, text_value_10 text, text_value_11 text, text_value_12 text,
  text_value_13 text, text_value_14 text, text_value_15 text, text_value_16 text,
  text_value_17 text, text_value_18 text, text_value_19 text, text_value_20 text,
  text_value_21 text, text_value_22 text, text_value_23 text, text_value_24 text,
  text_value_25 text, text_value_26 text, text_value_27 text, text_value_28 text,
  text_value_29 text,
  unique_numeric_value_1 decimal(65,17) DEFAULT NULL,
  unique_numeric_value_2 decimal(65,17) DEFAULT NULL,
  unique_text_value_1 text DEFAULT NULL,
  unique_text_value_2 text DEFAULT NULL,
  unique_value_ota_1 binary(16) DEFAULT NULL,
  unique_value_ota_2 binary(16) DEFAULT NULL,
  unique_value_ota_3 binary(16) DEFAULT NULL,
  unique_value_ota_4 binary(16) DEFAULT NULL,
  unique_value_1 text DEFAULT NULL,
  unique_value_2 text DEFAULT NULL,
  unique_value_3 text DEFAULT NULL,
  unique_value_4 text DEFAULT NULL,
  PRIMARY KEY (workspace_id, id) /*T![clustered_index] CLUSTERED */,
  UNIQUE KEY ix_external_id_unique (workspace_id, external_id(500)),
  KEY ix_obj_label (workspace_id, label),
  KEY ix_obj_type_id (workspace_id, obj_type_id),
  UNIQUE KEY unique_constraint_numeric_value_1 (workspace_id, obj_type_id, unique_numeric_value_1),
  UNIQUE KEY unique_constraint_numeric_value_2 (workspace_id, obj_type_id, unique_numeric_value_2),
  UNIQUE KEY unique_constraint_text_value_1 (workspace_id, obj_type_id, unique_text_value_1(500)),
  UNIQUE KEY unique_constraint_text_value_2 (workspace_id, obj_type_id, unique_text_value_2(500)),
  UNIQUE KEY unique_index_value_1 (workspace_id, unique_value_ota_1, unique_value_1(500)),
  UNIQUE KEY unique_index_value_2 (workspace_id, unique_value_ota_2, unique_value_2(500)),
  UNIQUE KEY unique_index_value_3 (workspace_id, unique_value_ota_3, unique_value_3(500)),
  UNIQUE KEY unique_index_value_4 (workspace_id, unique_value_ota_4, unique_value_4(500)),
  KEY ix_external_id (workspace_id, external_id(500)),
  KEY ix_obj__hash_key (hash_key),
  KEY ix_obj__monolith_id (monolith_id),
  KEY ix_obj_created_on (workspace_id, created_on),
  KEY ix_obj_group_values_keys (workspace_id(36), group_values_keys),
  KEY ix_obj_numeric_value_1 (workspace_id, numeric_value_1),
  KEY ix_obj_numeric_value_2 (workspace_id, numeric_value_2),
  KEY ix_obj_numeric_value_3 (workspace_id, numeric_value_3),
  KEY ix_obj_numeric_value_4 (workspace_id, numeric_value_4),
  KEY ix_obj_numeric_value_5 (workspace_id, numeric_value_5),
  KEY ix_obj_numeric_value_6 (workspace_id, numeric_value_6),
  KEY ix_obj_numeric_value_7 (workspace_id, numeric_value_7),
  KEY ix_obj_numeric_value_8 (workspace_id, numeric_value_8),
  KEY ix_obj_numeric_value_9 (workspace_id, numeric_value_9),
  KEY ix_obj_text_value_1 (workspace_id, text_value_1(500)),
  KEY ix_obj_text_value_2 (workspace_id, text_value_2(500)),
  KEY ix_obj_text_value_3 (workspace_id, text_value_3(500)),
  KEY ix_obj_text_value_4 (workspace_id, text_value_4(500)),
  KEY ix_obj_text_value_5 (workspace_id, text_value_5(500)),
  KEY ix_obj_text_value_6 (workspace_id, text_value_6(500)),
  KEY ix_obj_text_value_7 (workspace_id, text_value_7(500)),
  KEY ix_obj_text_value_8 (workspace_id, text_value_8(500)),
  KEY ix_obj_text_value_9 (workspace_id, text_value_9(500)),
  KEY ix_obj_text_value_10 (workspace_id, text_value_10(500)),
  KEY ix_obj_text_value_11 (workspace_id, text_value_11(500)),
  KEY ix_obj_text_value_12 (workspace_id, text_value_12(500)),
  KEY ix_obj_text_value_13 (workspace_id, text_value_13(500)),
  KEY ix_obj_text_value_14 (workspace_id, text_value_14(500)),
  KEY ix_obj_text_value_15 (workspace_id, text_value_15(500)),
  KEY ix_obj_text_value_16 (workspace_id, text_value_16(500)),
  KEY ix_obj_text_value_17 (workspace_id, text_value_17(500)),
  KEY ix_obj_text_value_18 (workspace_id, text_value_18(500)),
  KEY ix_obj_text_value_19 (workspace_id, text_value_19(500)),
  KEY ix_obj_text_value_20 (workspace_id, text_value_20(500)),
  KEY ix_obj_text_value_21 (workspace_id, text_value_21(500)),
  KEY ix_obj_text_value_22 (workspace_id, text_value_22(500)),
  KEY ix_obj_text_value_23 (workspace_id, text_value_23(500)),
  KEY ix_obj_text_value_24 (workspace_id, text_value_24(500)),
  KEY ix_obj_text_value_25 (workspace_id, text_value_25(500)),
  KEY ix_obj_text_value_26 (workspace_id, text_value_26(500)),
  KEY ix_obj_text_value_27 (workspace_id, text_value_27(500)),
  KEY ix_obj_text_value_28 (workspace_id, text_value_28(500)),
  KEY ix_obj_text_value_29 (workspace_id, text_value_29(500)),
  KEY ix_obj_unique_numeric_value_1 (workspace_id, unique_numeric_value_1),
  KEY ix_obj_unique_numeric_value_2 (workspace_id, unique_numeric_value_2),
  KEY ix_obj_unique_text_value_1 (workspace_id, unique_text_value_1(500)),
  KEY ix_obj_unique_text_value_2 (workspace_id, unique_text_value_2(500)),
  KEY ix_obj_updated_on (workspace_id, updated_on),
  KEY ix_schema_id (workspace_id, schema_id),
  UNIQUE KEY ix_sequential_id_unique (workspace_id, sequential_id),
  KEY ix_obj_label_objtypeid (workspace_id, label, obj_type_id, sequential_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`

const sharedPrefixBenchFullQuery = `explain SELECT o.sequential_id, o.label
	FROM obj o
	WHERE o.workspace_id = 'aa01e3d3-0423-4614-8004-206989601265'
	AND o.obj_type_id IN (
		UUID_TO_BIN('21bcdd9b-5bed-4ead-9a2c-778a6cf60d0b'),
		UUID_TO_BIN('3307bd5f-0564-4aea-807f-10b71c936cb8'),
		UUID_TO_BIN('770e0734-c440-47b0-90de-6abd76ec9fe2'),
		UUID_TO_BIN('9639c0b6-eb74-4d4d-96d3-ee562099d1f0'),
		UUID_TO_BIN('b95ee1c2-f117-4f9c-8dd7-0473a70d3237'))
	AND o.numeric_value_1 = 0
	AND o.numeric_value_3 = 15
	ORDER BY o.label ASC LIMIT 1000 OFFSET 0`

const sharedPrefixBenchOrderOnlyQuery = `explain SELECT o.sequential_id, o.label
	FROM obj o
	WHERE o.workspace_id = 'aa01e3d3-0423-4614-8004-206989601265'
	ORDER BY o.label ASC LIMIT 1000`

func setupSharedPrefixBench(b *testing.B, threshold int) *testkit.TestKit {
	store := testkit.CreateMockStore(b)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_collect_execution_info=0")
	tk.MustExec(sharedPrefixBenchDDL)
	typeUUIDs := []string{
		"21bcdd9b-5bed-4ead-9a2c-778a6cf60d0b",
		"3307bd5f-0564-4aea-807f-10b71c936cb8",
		"770e0734-c440-47b0-90de-6abd76ec9fe2",
		"9639c0b6-eb74-4d4d-96d3-ee562099d1f0",
		"b95ee1c2-f117-4f9c-8dd7-0473a70d3237",
		"c95ee1c2-f117-4f9c-8dd7-0473a70d3238",
	}
	tenants := []string{
		"aa01e3d3-0423-4614-8004-206989601265",
		"bb02e3d3-0423-4614-8004-206989601266",
		"cc03e3d3-0423-4614-8004-206989601267",
	}
	var sb strings.Builder
	sb.WriteString("insert into obj (id, workspace_id, sequential_id, label, obj_type_id, schema_id, numeric_value_1, numeric_value_3) values ")
	rows := 0
	for i := range 900 {
		ws := tenants[i%len(tenants)]
		if rows > 0 {
			sb.WriteString(",")
		}
		fmt.Fprintf(&sb, "(UUID_TO_BIN(UUID()), '%s', %d, 'label-%d', UUID_TO_BIN('%s'), UUID_TO_BIN(UUID()), %d, %d)",
			ws, i, i%50, typeUUIDs[i%len(typeUUIDs)], i%3, 15+(i%4))
		rows++
	}
	tk.MustExec(sb.String())
	tk.MustExec("analyze table obj all columns")
	tk.MustExec(fmt.Sprintf("set @@tidb_opt_index_prune_threshold=%d", threshold))
	return tk
}

func benchSharedPrefixPlanning(b *testing.B, threshold int, query string) {
	tk := setupSharedPrefixBench(b, threshold)
	tk.MustQuery(query) // warm up
	b.ResetTimer()
	for range b.N {
		tk.MustQuery(query)
	}
}

func BenchmarkIndexPruneSharedPrefixFullQuery(b *testing.B) {
	benchSharedPrefixPlanning(b, 20, sharedPrefixBenchFullQuery)
}

func BenchmarkIndexPruneSharedPrefixFullQueryNoPrune(b *testing.B) {
	benchSharedPrefixPlanning(b, -1, sharedPrefixBenchFullQuery)
}

func BenchmarkIndexPruneSharedPrefixOrderOnly(b *testing.B) {
	benchSharedPrefixPlanning(b, 20, sharedPrefixBenchOrderOnlyQuery)
}

func BenchmarkIndexPruneSharedPrefixOrderOnlyNoPrune(b *testing.B) {
	benchSharedPrefixPlanning(b, -1, sharedPrefixBenchOrderOnlyQuery)
}
