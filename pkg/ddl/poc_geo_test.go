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

package ddl_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

// TestPOCGeoCreateTable is a throwaway POC check that a table with geometry
// columns (and a per-column SRID) can be created and inspected.
func TestPOCGeoCreateTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	_, err := tk.Exec("CREATE TABLE locs (id int primary key, g GEOMETRY, p POINT NOT NULL SRID 4326, poly POLYGON)")
	if err != nil {
		t.Fatalf("create table error: %v", err)
	}
	res := tk.MustQuery("SHOW CREATE TABLE locs").Rows()
	t.Logf("SHOW CREATE TABLE:\n%s", res[0][1])

	// Raw EWKB-ish bytes round-trip as a binary string for now (no value codec yet).
	tk.MustExec("CREATE TABLE pts (id int primary key, p POINT SRID 0)")
	tk.MustExec("INSERT INTO pts VALUES (1, x'00000000010100000000000000000000000000000000000000')")
	tk.MustQuery("SELECT id, hex(p) FROM pts").Check(
		testkit.Rows("1 00000000010100000000000000000000000000000000000000"))
}
