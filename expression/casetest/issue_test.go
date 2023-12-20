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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package casetest

import (
	"testing"

	"github.com/pingcap/tidb/testkit"
)

func TestInvalidEnumName(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t01")
	tk.MustExec(`
	CREATE TABLE t01 (
		a timestamp DEFAULT '2024-10-02 01:54:55',
		b int(11) NOT NULL DEFAULT '2023959529',
		c varchar(122) DEFAULT '36h0hvfpylz0f0iv9h0ownfcg3rehi4',
		d enum('l7i9','3sdz3','83','4','92p','4g','8y5rn','7gp','7','1','e') NOT NULL DEFAULT '4',
		PRIMARY KEY (b, d) /*T![clustered_index] CLUSTERED */
	      ) ENGINE=InnoDB DEFAULT CHARSET=gbk COLLATE=gbk_chinese_ci COMMENT='7ad99128'
	      PARTITION BY HASH (b) PARTITIONS 9;`)
	tk.MustExec("insert ignore into t01 values ('2023-01-01 20:01:02', 123, 'abcd', '');")
	tk.MustQuery("select `t01`.`d` as r0 from `t01` where `t01`.`a` in ( '2010-05-25') or not( `t01`.`d` > '1' ) ;").Check(testkit.Rows(""))
}
