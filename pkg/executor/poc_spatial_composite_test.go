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

package executor_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// TestPOCSpatialComposite covers a composite spatial index (tenant_id, position):
// a multi-tenant table where a query carries the tenant key. The index serves
// `tenant_id = X AND <within radius>` by combining the prefix equality with the
// covering-cell ranges, so only one tenant's points are scanned.
func TestPOCSpatialComposite(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE locs (id int primary key, tenant_id int NOT NULL, p POINT NOT NULL SRID 0)")
	tk.MustExec("CREATE SPATIAL INDEX tp ON locs (tenant_id, p) COMMENT 'spatial:10,0,0,1024,1024'")

	// Two tenants, each with a grid of points in the same coordinate space.
	var b strings.Builder
	b.WriteString("INSERT INTO locs VALUES ")
	id := 0
	for tenant := 1; tenant <= 2; tenant++ {
		for x := 0; x < 40; x++ {
			for y := 0; y < 40; y++ {
				if id > 0 {
					b.WriteString(",")
				}
				fmt.Fprintf(&b, "(%d,%d, ST_GeomFromText('POINT(%d %d)',0))", id, tenant, x*10, y*10)
				id++
			}
		}
	}
	tk.MustExec(b.String())
	tk.MustExec("ADMIN CHECK TABLE locs")

	const query = "SELECT id FROM locs %s WHERE tenant_id = 2 AND " +
		"ST_Distance(p, ST_GeomFromText('POINT(200 200)',0)) <= 35 ORDER BY id"
	want := tk.MustQuery(fmt.Sprintf(query, "")).Rows()
	require.NotEmpty(t, want)

	// Forcing the composite index returns the same rows.
	forced := fmt.Sprintf(query, "FORCE INDEX (tp)")
	tk.MustQuery(forced).Check(want)

	// The plan uses an IndexRangeScan on tp whose range is bounded by tenant_id=2.
	var planText strings.Builder
	for _, r := range tk.MustQuery("EXPLAIN " + forced).Rows() {
		for _, c := range r {
			planText.WriteString(fmt.Sprintf("%v ", c))
		}
		planText.WriteString("\n")
	}
	plan := planText.String()
	t.Logf("EXPLAIN:\n%s", plan)
	require.Contains(t, plan, "tp(tenant_id", "expected composite index tp on tenant_id first")
	require.Contains(t, plan, "IndexRangeScan", "expected an index range scan")
}
