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

package instanceplancache

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestInstancePlanCacheDMLBasic(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set global tidb_enable_instance_plan_cache=1`)

	tk.MustExec(`create table t1 (a int, b int, c int, key(a), key(b, c))`)
	tk.MustExec(`create table t2 (a int, b int, c int, key(a), key(b, c))`)

	randInsert := func() (prep, set, exec, insert string) {
		a := rand.Intn(100)
		b := rand.Intn(100)
		c := rand.Intn(100)
		prep = `prepare stmt from 'insert into t1 values (?, ?, ?)'`
		set = fmt.Sprintf("set @a = %d, @b = %d, @c = %d", a, b, c)
		exec = `execute stmt using @a, @b, @c`
		insert = fmt.Sprintf("insert into t2 values (%d, %d, %d)", a, b, c)
		return
	}
	randDelete := func() (prep, set, exec, delete string) {
		a := rand.Intn(100)
		b := rand.Intn(100)
		c := rand.Intn(100)
		prep = `prepare stmt from 'delete from t1 where a < ? and b < ? or c < ?'`
		set = fmt.Sprintf("set @a = %d, @b = %d, @c = %d", a, b, c)
		exec = `execute stmt using @a, @b, @c`
		delete = fmt.Sprintf("delete from t2 where a < %d and b < %d or c < %d", a, b, c)
		return
	}
	randUpdate := func() (prep, set, exec, update string) {
		a := rand.Intn(100)
		b := rand.Intn(100)
		c := rand.Intn(100)
		prep = `prepare stmt from 'update t1 set a = ? where b = ? or c = ?'`
		set = fmt.Sprintf("set @a = %d, @b = %d, @c = %d", a, b, c)
		exec = `execute stmt using @a, @b, @c`
		update = fmt.Sprintf("update t2 set a = %d where b = %d or c = %d", a, b, c)
		return
	}

	checkResult := func() {
		tk.MustQuery("select * from t1").Sort().Check(
			tk.MustQuery("select * from t2").Sort().Rows())
	}

	for i := 0; i < 50; i++ {
		prep, set, exec, insert := randInsert()
		tk.MustExec(prep)
		tk.MustExec(set)
		tk.MustExec(exec)
		tk.MustExec(insert)
		checkResult()
	}

	for i := 0; i < 100; i++ {
		var prep, set, exec, dml string
		switch rand.Intn(3) {
		case 0:
			prep, set, exec, dml = randInsert()
		case 1:
			prep, set, exec, dml = randDelete()
		case 2:
			prep, set, exec, dml = randUpdate()
		}
		tk.MustExec(prep)
		tk.MustExec(set)
		tk.MustExec(exec)
		tk.MustExec(dml)
		checkResult()
	}
}
