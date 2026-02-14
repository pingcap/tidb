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

package schema

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestSchemaCannotFindColumnRegression(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec(`CREATE TABLE t1 (
  id BIGINT NOT NULL,
  k0 BIGINT NOT NULL,
  d0 DATE NOT NULL,
  d1 FLOAT NOT NULL,
  PRIMARY KEY (id)
)`)
		tk.MustExec(`CREATE TABLE t3 (
  id BIGINT NOT NULL,
  k2 VARCHAR(64) NOT NULL,
  k0 BIGINT NOT NULL,
  d0 DOUBLE NOT NULL,
  d1 DATE NOT NULL,
  PRIMARY KEY (id)
)`)
		tk.MustExec("INSERT INTO t1 (id, k0, d0, d1) VALUES (10, 93, '2024-04-14', 14.19)")
		tk.MustExec("INSERT INTO t3 (id, k2, k0, d0, d1) VALUES (10, 's356', 749, 89.26, '2024-04-29')")
		tk.MustQuery(`SELECT
  id AS t0_id
FROM t1
JOIN t3 USING (id)
WHERE (
  (
    (t3.d1 = '2024-04-29')
    AND (t3.id = 10)
  )
  AND (t1.k0 = 93)
)
AND (
  t3.k0 = ALL (
    SELECT t3.k0 AS c0
    FROM t3
    WHERE t3.k0 = 749
  )
)`).Check(testkit.Rows("10"))
	})
}
