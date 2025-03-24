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

package bindinfo_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestShowPlanForSQL(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec(`create table t (a int, b int, c int, key(a))`)
	tk.MustQuery(`show plan for "select a from t where b=1"`).Check(testkit.Rows())
	tk.MustExec(`create global binding using select a from t where b=1`)
	rs := tk.MustQuery(`show global bindings`).Rows()

	fmt.Println("============================================")
	for _, r := range rs {
		fmt.Println(r)
	}
	fmt.Println("============================================")

	tk.MustQuery(`show plan for "select a from t where b=1"`).Check(testkit.Rows())
}
