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
	"sync"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestBuiltinInIntSig(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set global tidb_enable_instance_plan_cache=1`)
	tk.MustExec("create table t1 (a int)")
	tk.MustExec("create table t2 (a int, key(a))")
	tk.MustExec("create table t3 (a int, primary key(a))")
	tk.MustExec("create table t4 (a int, unique key(a))")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t1 values (%d)", i))
		tk.MustExec(fmt.Sprintf("insert into t2 values (%d)", i))
		tk.MustExec(fmt.Sprintf("insert into t3 values (%d)", i))
		tk.MustExec(fmt.Sprintf("insert into t4 values (%d)", i))
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			for k := 0; k < 100; k++ {
				v1, v2 := rand.Intn(50), 50+rand.Intn(50)
				tName := fmt.Sprintf("t%d", rand.Intn(4)+1)
				tk.MustExec(fmt.Sprintf("prepare st from 'select a from %v where a in (?, ?)'", tName))
				tk.MustExec(fmt.Sprintf("set @p1=%v, @p2=%v", v1, v2))
				v1Str, v2Str := fmt.Sprintf("%v", v1), fmt.Sprintf("%v", v2)
				if v1Str > v2Str {
					v1Str, v2Str = v2Str, v1Str
				}
				tk.MustQuery("execute st using @p1, @p2").Sort().Check(testkit.Rows(v1Str, v2Str))
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func TestBuiltinInStringSig(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set global tidb_enable_instance_plan_cache=1`)
	tk.MustExec("create table t1 (a varchar(20))")
	tk.MustExec("create table t2 (a varchar(20), key(a))")
	tk.MustExec("create table t3 (a varchar(20), primary key(a))")
	tk.MustExec("create table t4 (a varchar(20), unique key(a))")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t1 values ('%d')", i))
		tk.MustExec(fmt.Sprintf("insert into t2 values ('%d')", i))
		tk.MustExec(fmt.Sprintf("insert into t3 values ('%d')", i))
		tk.MustExec(fmt.Sprintf("insert into t4 values ('%d')", i))
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			for k := 0; k < 100; k++ {
				v1, v2 := rand.Intn(50), 50+rand.Intn(50)
				tName := fmt.Sprintf("t%d", rand.Intn(4)+1)
				tk.MustExec(fmt.Sprintf("prepare st from 'select a from %v where a in (?, ?)'", tName))
				tk.MustExec(fmt.Sprintf("set @p1='%v', @p2='%v'", v1, v2))
				v1Str, v2Str := fmt.Sprintf("%v", v1), fmt.Sprintf("%v", v2)
				if v1Str > v2Str {
					v1Str, v2Str = v2Str, v1Str
				}
				tk.MustQuery("execute st using @p1, @p2").Sort().Check(testkit.Rows(v1Str, v2Str))
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func TestBuiltinInRealSig(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set global tidb_enable_instance_plan_cache=1`)
	tk.MustExec("create table t1 (a real)")
	tk.MustExec("create table t2 (a real, key(a))")
	tk.MustExec("create table t3 (a real, primary key(a))")
	tk.MustExec("create table t4 (a real, unique key(a))")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t1 values ('%d.1')", i))
		tk.MustExec(fmt.Sprintf("insert into t2 values ('%d.1')", i))
		tk.MustExec(fmt.Sprintf("insert into t3 values ('%d.1')", i))
		tk.MustExec(fmt.Sprintf("insert into t4 values ('%d.1')", i))
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			for k := 0; k < 100; k++ {
				v1, v2 := fmt.Sprintf("%v.1", rand.Intn(50)), fmt.Sprintf("%v.1", 50+rand.Intn(50))
				tName := fmt.Sprintf("t%d", rand.Intn(4)+1)
				tk.MustExec(fmt.Sprintf("prepare st from 'select a from %v where a in (?, ?)'", tName))
				tk.MustExec(fmt.Sprintf("set @p1='%v', @p2='%v'", v1, v2))
				v1Str, v2Str := fmt.Sprintf("%v", v1), fmt.Sprintf("%v", v2)
				if v1Str > v2Str {
					v1Str, v2Str = v2Str, v1Str
				}
				tk.MustQuery("execute st using @p1, @p2").Sort().Check(testkit.Rows(v1Str, v2Str))
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func TestBuiltinInDecimalSig(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set global tidb_enable_instance_plan_cache=1`)
	tk.MustExec("create table t1 (a decimal(10, 2))")
	tk.MustExec("create table t2 (a decimal(10, 2), key(a))")
	tk.MustExec("create table t3 (a decimal(10, 2), primary key(a))")
	tk.MustExec("create table t4 (a decimal(10, 2), unique key(a))")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t1 values ('%d.10')", i))
		tk.MustExec(fmt.Sprintf("insert into t2 values ('%d.10')", i))
		tk.MustExec(fmt.Sprintf("insert into t3 values ('%d.10')", i))
		tk.MustExec(fmt.Sprintf("insert into t4 values ('%d.10')", i))
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			for k := 0; k < 100; k++ {
				v1, v2 := fmt.Sprintf("%v.10", rand.Intn(50)), fmt.Sprintf("%v.10", 50+rand.Intn(50))
				tName := fmt.Sprintf("t%d", rand.Intn(4)+1)
				tk.MustExec(fmt.Sprintf("prepare st from 'select a from %v where a in (?, ?)'", tName))
				tk.MustExec(fmt.Sprintf("set @p1='%v', @p2='%v'", v1, v2))
				v1Str, v2Str := fmt.Sprintf("%v", v1), fmt.Sprintf("%v", v2)
				if v1Str > v2Str {
					v1Str, v2Str = v2Str, v1Str
				}
				tk.MustQuery("execute st using @p1, @p2").Sort().Check(testkit.Rows(v1Str, v2Str))
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func TestBuiltinInTimeSig(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set global tidb_enable_instance_plan_cache=1`)
	tk.MustExec("create table t1 (a datetime)")
	tk.MustExec("create table t2 (a datetime, key(a))")
	tk.MustExec("create table t3 (a datetime, primary key(a))")
	tk.MustExec("create table t4 (a datetime, unique key(a))")
	for i := 0; i < 40; i++ {
		tk.MustExec(fmt.Sprintf("insert into t1 values ('2000-01-01 00:%v:00')", 10+i))
		tk.MustExec(fmt.Sprintf("insert into t2 values ('2000-01-01 00:%v:00')", 10+i))
		tk.MustExec(fmt.Sprintf("insert into t3 values ('2000-01-01 00:%v:00')", 10+i))
		tk.MustExec(fmt.Sprintf("insert into t4 values ('2000-01-01 00:%v:00')", 10+i))
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			for k := 0; k < 100; k++ {
				v1 := fmt.Sprintf("2000-01-01 00:%v:00", 10+rand.Intn(20))
				v2 := fmt.Sprintf("2000-01-01 00:%v:00", 30+rand.Intn(20))
				tName := fmt.Sprintf("t%d", rand.Intn(4)+1)
				tk.MustExec(fmt.Sprintf("prepare st from 'select a from %v where a in (?, ?)'", tName))
				tk.MustExec(fmt.Sprintf("set @p1='%v', @p2='%v'", v1, v2))
				v1Str, v2Str := fmt.Sprintf("%v", v1), fmt.Sprintf("%v", v2)
				if v1Str > v2Str {
					v1Str, v2Str = v2Str, v1Str
				}
				tk.MustQuery("execute st using @p1, @p2").Sort().Check(testkit.Rows(v1Str, v2Str))
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func TestBuiltinRealIsTrueFalse(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set global tidb_enable_instance_plan_cache=1`)
	tk.MustExec("create table t (a real)")
	tk.MustExec("insert into t values (1.1)")
	tk.MustExec("insert into t values (2.2)")

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			for k := 0; k < 100; k++ {
				vs := []string{"1.1", "2.2"}
				if rand.Intn(2) < 1 { // is true
					tk.MustExec("prepare st from 'select a from t where (a-?) is true'")
					vIdx := rand.Intn(2)
					tk.MustExec(fmt.Sprintf("set @p1=%v", vs[vIdx]))
					tk.MustQuery("execute st using @p1").Check(testkit.Rows(vs[1-vIdx]))
				} else { // is false
					tk.MustExec("prepare st from 'select a from t where (a-?) is false'")
					vIdx := rand.Intn(2)
					tk.MustExec(fmt.Sprintf("set @p1=%v", vs[vIdx]))
					tk.MustQuery("execute st using @p1").Check(testkit.Rows(vs[vIdx]))
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func TestBuiltinDecimalIsTrueFalse(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set global tidb_enable_instance_plan_cache=1`)
	tk.MustExec("create table t (a decimal(10, 2))")
	tk.MustExec("insert into t values (1.10)")
	tk.MustExec("insert into t values (2.20)")

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			for k := 0; k < 100; k++ {
				vs := []string{"1.10", "2.20"}
				if rand.Intn(2) < 1 { // is true
					tk.MustExec("prepare st from 'select a from t where (a-?) is true'")
					vIdx := rand.Intn(2)
					tk.MustExec(fmt.Sprintf("set @p1=%v", vs[vIdx]))
					tk.MustQuery("execute st using @p1").Check(testkit.Rows(vs[1-vIdx]))
				} else { // is false
					tk.MustExec("prepare st from 'select a from t where (a-?) is false'")
					vIdx := rand.Intn(2)
					tk.MustExec(fmt.Sprintf("set @p1=%v", vs[vIdx]))
					tk.MustQuery("execute st using @p1").Check(testkit.Rows(vs[vIdx]))
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func TestBuiltinIntIsTrueFalse(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set global tidb_enable_instance_plan_cache=1`)
	tk.MustExec("create table t (a int)")
	tk.MustExec("insert into t values (1)")
	tk.MustExec("insert into t values (2)")

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			for k := 0; k < 100; k++ {
				vs := []string{"1", "2"}
				if rand.Intn(2) < 1 { // is true
					tk.MustExec("prepare st from 'select a from t where (a-?) is true'")
					vIdx := rand.Intn(2)
					tk.MustExec(fmt.Sprintf("set @p1=%v", vs[vIdx]))
					tk.MustQuery("execute st using @p1").Check(testkit.Rows(vs[1-vIdx]))
				} else { // is false
					tk.MustExec("prepare st from 'select a from t where (a-?) is false'")
					vIdx := rand.Intn(2)
					tk.MustExec(fmt.Sprintf("set @p1=%v", vs[vIdx]))
					tk.MustQuery("execute st using @p1").Check(testkit.Rows(vs[vIdx]))
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
}
