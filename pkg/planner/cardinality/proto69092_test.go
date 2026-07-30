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

package cardinality_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// TestProto69092ExpBackoffSchedule pins the per-level exponential-backoff ratio.
func TestProto69092ExpBackoffSchedule(t *testing.T) {
	r := 0.01
	require.InDelta(t, 0.01, cardinality.OrderingRatioForProbeLevel(r, 0), 1e-9)  // not under a probe
	require.InDelta(t, 0.01, cardinality.OrderingRatioForProbeLevel(r, 1), 1e-9)  // outermost
	require.InDelta(t, 0.10, cardinality.OrderingRatioForProbeLevel(r, 2), 1e-9)   // r^(1/2)
	require.InDelta(t, 0.3162, cardinality.OrderingRatioForProbeLevel(r, 3), 1e-3) // r^(1/4)
	require.InDelta(t, 0.5623, cardinality.OrderingRatioForProbeLevel(r, 4), 1e-3) // r^(1/8)
}

// TestProto69092InnerProbeTrace traces the inner-probe penalty across nesting depth.
// Driver d joins big1 (level-1 inner) which joins big2 (level-2 inner); both inner
// scans access by an indexed join key and carry a residual filter (the surplus the
// ratio works on). We compare plan cost with the ratio disabled vs enabled.
func TestProto69092InnerProbeTrace(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists d, big1, big2")
	// d: small driver.
	tk.MustExec("create table d(id int primary key, fk int)")
	// big1/big2: join key jk (indexed, non-unique → fanout), filter column f.
	tk.MustExec("create table big1(id int primary key, jk int, f int, g int, index ijk(jk))")
	tk.MustExec("create table big2(id int primary key, jk int, f int, index ijk(jk))")

	tk.MustExec("insert into d values (1,1),(2,2),(3,3),(4,4),(5,5)")
	tk.MustExec("insert into big1(id,jk,f,g) values (1,1,1,1),(2,1,2,1),(3,2,1,2),(4,2,2,2),(5,3,1,3),(6,3,2,3),(7,4,1,4),(8,4,2,4),(9,5,1,5),(10,5,2,5)")
	for i := 0; i < 6; i++ {
		tk.MustExec("insert into big1(id,jk,f,g) select id+(select max(id) from big1),jk,f,g from big1")
	}
	tk.MustExec("insert into big2(id,jk,f) select id,jk,f from big1")
	tk.MustExec("analyze table d, big1, big2")
	require.NoError(t, dom.StatsHandle().Update(context.Background(), dom.InfoSchema()))

	dump := func(tag, q string) [][]any {
		rows := tk.MustQuery(q).Rows()
		t.Logf("==== plan (%s) ====", tag)
		out := make([][]any, len(rows))
		for i, row := range rows {
			out[i] = row
			t.Logf("%-40s estRows=%-12s estCost=%-14s task=%-8s access=%v",
				fmt.Sprint(row[0]), fmt.Sprint(row[1]), fmt.Sprint(row[2]), fmt.Sprint(row[3]), row[5])
		}
		return out
	}
	// For big2, return (matching estRows from its IndexLookUp, access estRows from its
	// IndexRangeScan). The IndexLookUp appears just above its IndexRangeScan child in the
	// EXPLAIN tree, so the most-recent IndexLookUp before big2's IndexRangeScan is big2's.
	big2LookupAndAccess := func(rows [][]any) (lookup, access float64) {
		lookup, access = -1, -1
		lastLookup := -1.0
		for _, row := range rows {
			op := fmt.Sprint(row[0])
			estRows, _ := strconv.ParseFloat(fmt.Sprint(row[1]), 64)
			if strings.Contains(op, "IndexLookUp") {
				lastLookup = estRows
			}
			if strings.Contains(op, "IndexRangeScan") && strings.Contains(fmt.Sprint(row[5]), "big2.jk") {
				lookup, access = lastLookup, estRows
			}
		}
		return lookup, access
	}
	impliedRatio := func(base, on, access float64) float64 {
		if access <= base {
			return 0
		}
		return (on - base) / (access - base)
	}

	// Query A: plain nested join. big2 is the index-join inner at probe level 1 (r_eff=0.01).
	qA := "explain format=verbose select /*+ inl_join(big1), inl_join(big2) */ d.id " +
		"from d join big1 on d.fk = big1.jk join big2 on big1.g = big2.jk " +
		"where big1.f = 1 and big2.f = 1"
	// Query B: EXISTS body is a join, kept correlated (NO_DECORRELATE) so it becomes an
	// Apply whose probe is the index join. big2 is then the index-join inner UNDER the
	// apply → probe level 2 (r_eff=sqrt(0.01)=0.1), i.e. the two-EXISTS-style nesting.
	qB := "explain format=verbose select d.id from d where exists (" +
		"select /*+ NO_DECORRELATE(), INL_JOIN(big2) */ 1 from big1 join big2 on big1.g = big2.jk " +
		"where big1.jk = d.fk and big1.f = 1 and big2.f = 1)"

	tk.MustExec("set @@session.tidb_opt_ordering_index_selectivity_ratio = 0")
	baseA, accessA := big2LookupAndAccess(dump("A plain-join, ratio=0", qA))
	baseB, accessB := big2LookupAndAccess(dump("B exists/apply, ratio=0", qB))
	tk.MustExec("set @@session.tidb_opt_ordering_index_selectivity_ratio = 0.01")
	onA, _ := big2LookupAndAccess(dump("A plain-join, ratio=0.01 (big2 @ level 1)", qA))
	onB, _ := big2LookupAndAccess(dump("B exists/apply, ratio=0.01 (big2 @ level 2)", qB))

	rA := impliedRatio(baseA, onA, accessA)
	rB := impliedRatio(baseB, onB, accessB)
	t.Logf("big2 inner-probe penalty (base ratio r=0.01):")
	t.Logf("  level 1 (plain join):  lookup %.2f->%.2f  access=%.0f  implied r_eff=%.4f (expect 0.0100)", baseA, onA, accessA, rA)
	t.Logf("  level 2 (under apply): lookup %.2f->%.2f  access=%.0f  implied r_eff=%.4f (expect 0.1000)", baseB, onB, accessB, rB)
	require.InDelta(t, 0.01, rA, 1e-4, "level-1 effective ratio should equal the base ratio")
	require.InDelta(t, 0.10, rB, 1e-3, "level-2 effective ratio should be sqrt(base) = exp-backoff muting")
}
