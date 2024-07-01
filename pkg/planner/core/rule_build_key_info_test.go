// Copyright 2024 PingCAP, Inc.
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

package core_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestNotColumnTypeInBuildKeyInfo(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE t0 (c1 double);")
	tk.MustExec("CREATE TABLE t2 (c11 int);")
	tk.MustQuery(`select 1
from
  (select false as c1) as subq_0
where (case when subq_0.c1 >= (
            select
                1 as c0
              from
                t2 as ref_4
        ) then 1 else 0 end
      ) = nullif(1 << 1, (
                select
                    0 as c0
                  from
                    t0 as ref_8
                  where exists (
                        select
                            1 as c11
                          from
                            t0 as ref_13
                          where ref_8.c1 < ref_13.c1
                        )
                 ));
	`)
}
