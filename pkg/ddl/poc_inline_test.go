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
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

// TestPOCInlineSpatialIndex verifies inline `SPATIAL INDEX` in CREATE TABLE and
// that SHOW CREATE TABLE renders the SPATIAL KEY form that round-trips.
func TestPOCInlineSpatialIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	_, err := tk.Exec("CREATE TABLE locs (id int primary key, p POINT NOT NULL SRID 0, SPATIAL INDEX idx_p (p))")
	if err != nil {
		t.Fatalf("inline spatial index create error: %v", err)
	}
	tk.MustExec("INSERT INTO locs VALUES (1, ST_GeomFromText('POINT(1 1)',0)), (2, ST_GeomFromText('POINT(5 5)',0))")
	tk.MustExec("ADMIN CHECK TABLE locs")
	tk.MustExec("ADMIN CHECK INDEX locs idx_p")
	// SHOW CREATE TABLE should render the spatial index in its re-importable form.
	res := tk.MustQuery("SHOW CREATE TABLE locs").Rows()
	create := res[0][1].(string)
	t.Logf("SHOW CREATE TABLE:\n%s", create)
	if !strings.Contains(create, "SPATIAL KEY `idx_p` (`p`)") {
		t.Fatalf("expected SPATIAL KEY rendering, got:\n%s", create)
	}
	// Round-trip: the SHOW CREATE output re-imports into a fresh table.
	create2 := strings.Replace(create, "TABLE `locs`", "TABLE `locs2`", 1)
	tk.MustExec(create2)
	tk.MustExec("ADMIN CHECK INDEX locs2 idx_p")
	// Query equivalence with the index present.
	tk.MustQuery("SELECT id FROM locs WHERE ST_Distance(p, ST_GeomFromText('POINT(5 5)',0)) <= 1 ORDER BY id").Check(testkit.Rows("2"))
}
