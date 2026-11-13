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

package issuetest_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestFloatCastDateGrouping(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("DROP TABLE IF EXISTS t")
	tk.MustExec(`CREATE TABLE t (
		c0 DECIMAL(10, 0) NOT NULL,
		c1 FLOAT UNSIGNED ZEROFILL NOT NULL,
		PRIMARY KEY (c0, c1)
	)`)
	tk.MustExec(`INSERT INTO t VALUES
		(-2068985011, 0.75245386),
		(-668435082, 0.19411194),
		(-500731198, 0.39079505),
		(0, 0),
		(0, 0.9938275),
		(12196703, 970789000),
		(919009011, 0.28699672),
		(1069380201, 0.2576304)`)

	tk.MustQuery("SELECT CAST(c1 AS DATE) IS NULL FROM t ORDER BY c0, c1").Check(
		testkit.Rows("1", "1", "1", "1", "1", "1", "1", "1"),
	)
	tk.MustQuery(`SELECT c0 FROM t
		GROUP BY c0, CAST(c1 AS DATE), c0 OR ''
		HAVING NOT c0`).Check(testkit.Rows("0"))
}
