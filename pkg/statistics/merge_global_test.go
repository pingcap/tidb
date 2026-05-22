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

package statistics

import (
	"bytes"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/stretchr/testify/require"
)

// This file is the single home for unit-test coverage of
// MergePartTopNAndHistToGlobal. Cases are described declaratively as
// (per-partition TopN entries + buckets) plus optional pinned
// expectations. Structural invariants run on every case.
//
// The merge call site in runMergeCase is a single line, so the same
// fixture can be retargeted to a different merge implementation
// (e.g. a two-step variant that builds the global TopN and histogram
// separately) by replacing that call and routing the outputs into
// (gTopN, gHist, err). The fixture types and assertions are
// algorithm-agnostic.

// --------------------------------------------------------------------
// Fixture types
// --------------------------------------------------------------------

// bucketSpec describes a single source bucket in a partition. mass is
// the per-bucket count (NOT cumulative); buildInputs converts to the
// cumulative form Histogram expects.
type bucketSpec struct {
	lo, up datum
	mass   int64
	repeat int64
}

// topnSpec describes a single TopN entry by encoded value.
type topnSpec struct {
	val   datum
	count uint64
}

// partSpec is one partition's worth of input.
type partSpec struct {
	topN []topnSpec
	hist []bucketSpec
}

// datum lets a case express either an int or a string value without
// the boilerplate of types.Datum construction. Cases pick one form per
// case (mixing within a case is not supported because the field type
// is single).
type datum struct {
	i int64
	s string
	// kind: 0 = int, 1 = string. Default zero-value is int.
	kind uint8
}

func di(v int64) datum  { return datum{i: v, kind: 0} }
func ds(v string) datum { return datum{s: v, kind: 1} }

func (d datum) toDatum() types.Datum {
	switch d.kind {
	case 1:
		return types.NewStringDatum(d.s)
	default:
		return types.NewIntDatum(d.i)
	}
}

// mergeCase is a single declarative test input + expectation set.
type mergeCase struct {
	name string

	// Inputs.
	parts      []partSpec
	numTopN    uint32
	expBuckets int64
	isIndex    bool
	colTp      byte // mysql.TypeLong (default) or mysql.TypeVarchar

	// Pre-arm killer signal before calling the merge.
	preKill bool

	// Optional pinned expectations.
	wantTopN    []topnSpec   // post-merge global TopN, sorted by encoded
	wantBuckets []bucketSpec // post-merge global buckets; if empty, only invariants
	wantErrSub  string       // substring of expected error; empty means no error expected

	// Optional case-specific extra assertion (e.g. equi-depth quality).
	// Runs after invariants and pinned expectations.
	extra func(t *testing.T, hist *Histogram, topN *TopN)
}

// --------------------------------------------------------------------
// Fixture builder
// --------------------------------------------------------------------

// newCaseFieldType returns the field type to use for the partition
// histograms. Index histograms always use TypeBlob (their bucket
// bounds are stored as the encoded byte key); column histograms use
// the case's colTp (default TypeLong).
func newCaseFieldType(tpKind byte, isIndex bool) *types.FieldType {
	if isIndex {
		return types.NewFieldType(mysql.TypeBlob)
	}
	if tpKind == 0 {
		tpKind = mysql.TypeLong
	}
	return types.NewFieldType(tpKind)
}

func encodeDatum(t *testing.T, sc *stmtctx.StatementContext, d datum) []byte {
	enc, err := codec.EncodeKey(sc.TimeZone(), nil, d.toDatum())
	require.NoError(t, err)
	return enc
}

// boundDatum returns the datum to append as a histogram bucket bound.
// For index histograms the bound must be Bytes-kind carrying the
// encoded key (that's what the merge's per-bucket compares against
// the index TopN's Encoded form). For column histograms the typed
// datum is used directly.
func boundDatum(t *testing.T, sc *stmtctx.StatementContext, d datum, isIndex bool) types.Datum {
	base := d.toDatum()
	if !isIndex {
		return base
	}
	enc, err := codec.EncodeKey(sc.TimeZone(), nil, base)
	require.NoError(t, err)
	var out types.Datum
	out.SetBytes(enc)
	return out
}

// buildInputs turns the declarative parts into the slices the merge
// expects. Bucket counts are converted from per-bucket mass to
// cumulative-within-partition. For tc.isIndex=true bucket bounds are
// converted to Bytes-kind datums carrying the encoded key.
func buildInputs(t *testing.T, sc *stmtctx.StatementContext, tc mergeCase) ([]*TopN, []*Histogram) {
	tp := newCaseFieldType(tc.colTp, tc.isIndex)
	topNs := make([]*TopN, len(tc.parts))
	hists := make([]*Histogram, len(tc.parts))
	for p, ps := range tc.parts {
		tn := NewTopN(len(ps.topN))
		for _, e := range ps.topN {
			tn.AppendTopN(encodeDatum(t, sc, e.val), e.count)
		}
		topNs[p] = tn

		h := NewHistogram(1, 0, 0, 0, tp, chunk.InitialCapacity, 0)
		var cum int64
		for _, b := range ps.hist {
			cum += b.mass
			lo := boundDatum(t, sc, b.lo, tc.isIndex)
			up := boundDatum(t, sc, b.up, tc.isIndex)
			h.AppendBucket(&lo, &up, cum, b.repeat)
		}
		hists[p] = h
	}
	return topNs, hists
}

// --------------------------------------------------------------------
// Driver
// --------------------------------------------------------------------

func runMergeCase(t *testing.T, tc mergeCase) {
	sc := stmtctx.NewStmtCtxWithTimeZone(time.UTC)
	killer := sqlkiller.SQLKiller{}
	if tc.preKill {
		killer.SendKillSignal(sqlkiller.QueryInterrupted)
	}

	topNs, hists := buildInputs(t, sc, tc)

	// Single call site, retargeting to a different merge
	// implementation only edits this line.
	gTopN, gHist, err := MergePartTopNAndHistToGlobal(
		sc, &killer, topNs, hists, tc.numTopN, tc.expBuckets, tc.isIndex,
	)

	if tc.wantErrSub != "" {
		require.Error(t, err)
		require.Contains(t, err.Error(), tc.wantErrSub)
		return
	}
	require.NoError(t, err)
	require.NotNil(t, gHist, "global histogram must be non-nil on success")

	assertInvariants(t, sc, tc, gHist, gTopN)

	if tc.wantTopN != nil {
		assertTopN(t, sc, tc.wantTopN, gTopN)
	}
	if tc.wantBuckets != nil {
		assertBuckets(t, tc.wantBuckets, gHist, tc.isIndex)
	}
	if tc.extra != nil {
		tc.extra(t, gHist, gTopN)
	}
}

// --------------------------------------------------------------------
// Invariants, run on every successful case
// --------------------------------------------------------------------

func assertInvariants(t *testing.T, sc *stmtctx.StatementContext, tc mergeCase, hist *Histogram, topN *TopN) {
	// Inv 1: bucket bounds not inverted; Inv 8: every bucket has
	// strictly positive mass (no ghost bucket from a no-mass merged
	// group).
	masses := bucketMasses(hist)
	for i := 0; i < hist.Len(); i++ {
		require.LessOrEqualf(t, compareDatums(t, sc, hist.GetLower(i), hist.GetUpper(i)), 0,
			"bucket %d has inverted bounds", i)
		require.Greaterf(t, masses[i], int64(0),
			"bucket %d has zero mass (ghost bucket); masses=%v", i, masses)
	}
	// Inv 2 + 3: cumulative non-decreasing, no overlap with previous.
	for i := 1; i < hist.Len(); i++ {
		require.GreaterOrEqualf(t, hist.Buckets[i].Count, hist.Buckets[i-1].Count,
			"bucket %d cumulative regressed", i)
		require.GreaterOrEqualf(t,
			compareDatums(t, sc, hist.GetLower(i), hist.GetUpper(i-1)), 0,
			"bucket %d lower below previous upper (overlap)", i)
	}
	// Inv 4: total rows preserved across histogram + global TopN.
	wantTotal := totalInputRows(tc)
	got := int64(0)
	if hist.Len() > 0 {
		got = hist.Buckets[hist.Len()-1].Count
	}
	if topN != nil {
		for _, m := range topN.TopN {
			got += int64(m.Count)
		}
	}
	require.Equalf(t, wantTotal, got, "total rows must be preserved (input=%d, output=%d)", wantTotal, got)
	// Inv 5: bucket count and TopN size respect their caps.
	require.LessOrEqualf(t, int64(hist.Len()), tc.expBuckets,
		"bucket count %d exceeds expBuckets=%d", hist.Len(), tc.expBuckets)
	if topN != nil {
		require.LessOrEqualf(t, uint32(len(topN.TopN)), tc.numTopN,
			"global TopN size %d exceeds numTopN=%d", len(topN.TopN), tc.numTopN)
		// Inv 6: TopN sorted by encoded bytes.
		for i := 1; i < len(topN.TopN); i++ {
			require.LessOrEqualf(t,
				bytes.Compare(topN.TopN[i-1].Encoded, topN.TopN[i].Encoded), 0,
				"global TopN must be sorted by encoded; entries %d and %d out of order", i-1, i)
		}
		// Inv 7: bucket upper matching a global TopN value must have
		// Repeat = 0, the TopN counter owns those rows. For index
		// histograms the upper is already the encoded bytes (Bytes-
		// kind datum), so use those directly; for column histograms
		// re-encode the typed upper.
		topNSet := make(map[string]struct{}, len(topN.TopN))
		for _, m := range topN.TopN {
			topNSet[string(m.Encoded)] = struct{}{}
		}
		for i := 0; i < hist.Len(); i++ {
			upper := hist.GetUpper(i)
			var enc []byte
			if tc.isIndex {
				enc = upper.GetBytes()
			} else {
				var err error
				enc, err = codec.EncodeKey(sc.TimeZone(), nil, *upper)
				require.NoError(t, err)
			}
			if _, hit := topNSet[string(enc)]; hit {
				require.Equalf(t, int64(0), hist.Buckets[i].Repeat,
					"bucket %d upper matches a global TopN value; Repeat must be 0, got %d",
					i, hist.Buckets[i].Repeat)
			}
		}
	}
}

func compareDatums(t *testing.T, sc *stmtctx.StatementContext, a, b *types.Datum) int {
	c, err := a.Compare(sc.TypeCtx(), b, collate.GetBinaryCollator())
	require.NoError(t, err)
	return c
}

func totalInputRows(tc mergeCase) int64 {
	var sum int64
	for _, p := range tc.parts {
		for _, e := range p.topN {
			sum += int64(e.count)
		}
		for _, b := range p.hist {
			sum += b.mass
		}
	}
	return sum
}

// --------------------------------------------------------------------
// Pin-to-expectation assertions
// --------------------------------------------------------------------

func assertTopN(t *testing.T, sc *stmtctx.StatementContext, want []topnSpec, got *TopN) {
	require.NotNil(t, got, "expected non-nil global TopN")
	require.Equalf(t, len(want), len(got.TopN), "global TopN size mismatch; got=%v", got.TopN)
	for i, w := range want {
		enc := encodeDatum(t, sc, w.val)
		require.Equalf(t, enc, got.TopN[i].Encoded,
			"global TopN entry %d encoded mismatch", i)
		require.Equalf(t, w.count, got.TopN[i].Count,
			"global TopN entry %d count mismatch", i)
	}
}

// assertBuckets pins lower / upper / cumulative count from want.
// Repeat is intentionally not checked here, Inv 7 covers the
// dangerous case (TopN-matched upper must have Repeat=0); fine-grained
// per-bucket Repeat values are hard to predict from outside the merge
// and the integration tests assert them where it matters.
//
// For index histograms (isIndex=true) only bucket count and cumulative
// count are pinned, bucket bounds are stored as encoded bytes which
// don't round-trip into the case's typed value.
func assertBuckets(t *testing.T, want []bucketSpec, hist *Histogram, isIndex bool) {
	require.Equalf(t, len(want), hist.Len(), "global bucket count mismatch")
	var cum int64
	for i, w := range want {
		cum += w.mass
		if !isIndex {
			wantLo, wantUp := w.lo.toDatum(), w.up.toDatum()
			require.Equalf(t, wantLo.GetValue(), hist.GetLower(i).GetValue(),
				"bucket %d lower mismatch", i)
			require.Equalf(t, wantUp.GetValue(), hist.GetUpper(i).GetValue(),
				"bucket %d upper mismatch", i)
		}
		require.Equalf(t, cum, hist.Buckets[i].Count,
			"bucket %d cumulative count mismatch", i)
	}
}

// --------------------------------------------------------------------
// Mass helpers (shared with histogram_fuzz_test.go callers if needed)
// --------------------------------------------------------------------

func bucketMasses(h *Histogram) []int64 {
	out := make([]int64, h.Len())
	prev := int64(0)
	for i := 0; i < h.Len(); i++ {
		out[i] = h.Buckets[i].Count - prev
		prev = h.Buckets[i].Count
	}
	return out
}

func maxOverMin(masses []int64) float64 {
	if len(masses) == 0 {
		return 0
	}
	lo, hi := masses[0], masses[0]
	for _, m := range masses[1:] {
		if m < lo {
			lo = m
		}
		if m > hi {
			hi = m
		}
	}
	if lo <= 0 {
		return 0
	}
	return float64(hi) / float64(lo)
}

// --------------------------------------------------------------------
// Driver entry, cases live in merge_global_cases_test.go
// --------------------------------------------------------------------

func TestMergePartTopNAndHistToGlobal(t *testing.T) {
	for _, tc := range mergeCases() {
		t.Run(tc.name, func(t *testing.T) { runMergeCase(t, tc) })
	}
}
