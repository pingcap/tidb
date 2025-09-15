// Copyright 2023 PingCAP, Inc.
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

package tests

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/testkit"
)

func TestBindUsageInfo(t *testing.T) {
	bindinfo.WriteIntervalAfterNoReadBinding = 10 * time.Microsecond
	bindinfo.MinCheckIntervalForUpdateBindingUsageInfo = 1
	bindinfo.MaxCheckIntervalForUpdateBindingUsageInfo = 2
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, c int, d int, e int, key(a), key(b), key(c), key(d), key(e))`)
	tk.MustExec(`create database test1`)
	tk.MustExec(`use test1`)
	tk.MustExec(`create table t (a int, b int, c int, d int, e int, key(a), key(b), key(c), key(d), key(e))`)
	tk.MustExec(`create database test2`)
	tk.MustExec(`use test2`)
	tk.MustExec(`create table t (a int, b int, c int, d int, e int, key(a), key(b), key(c), key(d), key(e))`)

	tk.MustExec(`admin reload bindings`)

}
