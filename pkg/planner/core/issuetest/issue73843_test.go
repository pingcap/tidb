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

package issuetest

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestIssue73843(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists binary_agg")
	tk.MustExec("create table binary_agg (id int primary key, a int not null, key ia(a, id))")
	tk.MustExec("insert into binary_agg (id, a) values (1, 1), (2, 1), (3, 2)")

	tk.MustQuery("select b'101010' = x'00002A', hex(b'101010'), hex(x'00002A')").
		Check(testkit.Rows("0 2A 00002A"))

	tk.MustQuery("select hex(min(b'101010')), hex(max(b'101010')) from binary_agg").
		Check(testkit.Rows("2A 2A"))
	tk.MustQuery("select hex(min(x'00002A')), hex(max(x'00002A')) from binary_agg").
		Check(testkit.Rows("00002A 00002A"))

	tk.MustQuery("select hex(min(b'101010')) as short_min, hex(min(x'00002A')) as long_min, " +
		"hex(max(b'101010')) as short_max, hex(max(x'00002A')) as long_max from binary_agg").
		Check(testkit.Rows("2A 00002A 2A 00002A"))

	tk.MustQuery("select hex(min(x'00002A')) as long_min, hex(min(b'101010')) as short_min, " +
		"hex(max(x'00002A')) as long_max, hex(max(b'101010')) as short_max from binary_agg").
		Check(testkit.Rows("00002A 2A 00002A 2A"))

	tk.MustQuery("select hex(min(cast(b'101010' as binary))), hex(min(cast(x'00002A' as binary))) from binary_agg").
		Check(testkit.Rows("2A 00002A"))

	tk.MustQuery("select id, hex(min(b'101010')), hex(min(x'00002A')) " +
		"from (select * from binary_agg order by id limit 3) as t group by id order by id").
		Check(testkit.Rows("1 2A 00002A", "2 2A 00002A", "3 2A 00002A"))
}
