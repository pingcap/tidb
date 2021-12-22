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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression_test

import (
	"testing"

	"github.com/pingcap/tidb/testkit"
)

func TestFoldIfNull(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a bigint, b bigint);`)
	tk.MustExec(`insert into t values(1, 1);`)
	tk.MustQuery(`desc select ifnull("aaaa", a) from t;`).Check(testkit.Rows(
		`Projection_3 10000.00 root  aaaa->Column#4`,
		`└─TableReader_5 10000.00 root  data:TableFullScan_4`,
		`  └─TableFullScan_4 10000.00 cop[tikv] table:t keep order:false, stats:pseudo`,
	))
	tk.MustQuery(`show warnings;`).Check(testkit.Rows())
	tk.MustQuery(`select ifnull("aaaa", a) from t;`).Check(testkit.Rows("aaaa"))
	tk.MustQuery(`show warnings;`).Check(testkit.Rows())
}
