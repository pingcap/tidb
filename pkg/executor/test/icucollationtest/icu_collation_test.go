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

package icucollationtest

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

// TestICULocaleCollationSQL exercises the ICU-derived locale collations end-to-end through real
// SQL: CREATE TABLE ... COLLATE, ORDER BY (via an index on the collated column), case-insensitive
// equality, and numeric ordering.
func TestICULocaleCollationSQL(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// German: ä collates near a; case-insensitive; ordered through the secondary index on name.
	tk.MustExec("create table tg (id int primary key, name varchar(64) " +
		"character set utf8mb4 collate utf8mb4_de_0900_as_ci_kn, key(name))")
	tk.MustExec("insert into tg values (1,'z'),(2,'ä'),(3,'a'),(4,'b')")
	tk.MustQuery("select name from tg order by name").Check(testkit.Rows("a", "ä", "b", "z"))
	// Case-insensitive equality (predicate stays in TiDB via the pushdown gate): 'A' matches 'a'.
	tk.MustQuery("select id from tg where name = 'A'").Check(testkit.Rows("3"))
	tk.MustQuery("select id from tg where name = 'B'").Check(testkit.Rows("4"))

	// Danish: the "aa" contraction sorts after z.
	tk.MustExec("create table td (id int primary key, name varchar(64) " +
		"character set utf8mb4 collate utf8mb4_da_0900_as_ci_kn, key(name))")
	tk.MustExec("insert into td values (1,'Zealand'),(2,'Aarhus'),(3,'Bornholm')")
	tk.MustQuery("select name from td order by name").Check(testkit.Rows("Bornholm", "Zealand", "Aarhus"))

	// Numeric ordering: digit runs compare by value.
	tk.MustExec("create table tn (v varchar(32) " +
		"character set utf8mb4 collate utf8mb4_fr_0900_as_ci_kn)")
	tk.MustExec("insert into tn values ('item-2'),('item-10'),('item-9')")
	tk.MustQuery("select v from tn order by v").Check(testkit.Rows("item-2", "item-9", "item-10"))

	// Accent-sensitivity: é distinct from e (not folded at level 2).
	tk.MustQuery("select v from (select 'e' v union all select 'é' union all select 'f') t order by v " +
		"collate utf8mb4_fr_0900_as_ci_kn").Check(testkit.Rows("e", "é", "f"))
}
