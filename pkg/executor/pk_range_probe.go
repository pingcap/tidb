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

package executor

import (
	"bytes"
	"fmt"
	"time"

	"github.com/pingcap/tidb/pkg/distsql"
	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/ranger"
	rangerctx "github.com/pingcap/tidb/pkg/util/ranger/context"
)

// pkRangeProber bounds the key ranges of a table scan with the handles that
// secondary indexes hold for the scan's equality conditions. See
// physicalop.PKRangeProbe. It reads the indexes at the snapshot the scan
// reads, so the bounds cover every row the scan can return.
type pkRangeProber struct {
	probes   []*physicalop.PKRangeProbe
	snapshot kv.Snapshot
	tableID  int64

	evaluated bool
	// empty is set when no row can satisfy the equality conditions.
	empty bool
	// lo and hi bound the record keys to read to [lo, hi). Both are nil when
	// no probe narrowed the scan.
	lo, hi kv.Key
	stats  pkRangeProbeRuntimeStats
}

// evaluate probes the indexes once; later calls, such as re-opens under an
// Apply, reuse the result.
func (p *pkRangeProber) evaluate(rctx *rangerctx.RangerContext, dctx *distsqlctx.DistSQLContext) error {
	if p.evaluated {
		return nil
	}
	p.evaluated = true
	start := time.Now()
	defer func() { p.stats.probeTime = time.Since(start) }()
	prefix := tablecodec.GenTableRecordPrefix(p.tableID)
	for _, probe := range p.probes {
		minHandle, maxHandle, exact, err := p.probeIndex(rctx, dctx, probe)
		if err != nil {
			return err
		}
		if !exact {
			continue
		}
		if minHandle == nil {
			p.empty = true
			return nil
		}
		lo := tablecodec.EncodeRecordKey(prefix, minHandle)
		hi := tablecodec.EncodeRecordKey(prefix, maxHandle).Next()
		if p.lo == nil || lo.Cmp(p.lo) > 0 {
			p.lo = lo
		}
		if p.hi == nil || hi.Cmp(p.hi) < 0 {
			p.hi = hi
		}
		if p.lo.Cmp(p.hi) >= 0 {
			p.empty = true
			return nil
		}
	}
	return nil
}

// probeIndex returns the smallest and the largest handle that probe.Index
// holds for the values probe.EqConds select, or nil handles when it holds
// none. exact is false when the conditions don't map to a single index key
// prefix, in which case the entries under the range aren't ordered by handle
// and the probe can't bound the scan.
func (p *pkRangeProber) probeIndex(rctx *rangerctx.RangerContext, dctx *distsqlctx.DistSQLContext,
	probe *physicalop.PKRangeProbe) (minHandle, maxHandle kv.Handle, exact bool, err error) {
	res, err := ranger.DetachCondAndBuildRangeForIndex(rctx, probe.EqConds, probe.IdxCols, probe.IdxColLens, 0)
	if err != nil {
		return nil, nil, false, err
	}
	if len(res.Ranges) == 0 {
		// The conditions can't be true, e.g. `c = NULL`.
		return nil, nil, true, nil
	}
	if len(res.Ranges) != 1 || len(res.Ranges[0].LowVal) != len(probe.IdxCols) ||
		!res.Ranges[0].IsPointNonNullable(rctx.TypeCtx) {
		return nil, nil, false, nil
	}
	keyRanges, err := distsql.IndexRangesToKVRanges(dctx, p.tableID, probe.Index.ID, res.Ranges)
	if err != nil {
		return nil, nil, false, err
	}
	kvRanges := keyRanges.FirstPartitionRange()
	if len(kvRanges) != 1 {
		return nil, nil, false, nil
	}
	start, end := kvRanges[0].StartKey, kvRanges[0].EndKey
	colsLen := len(probe.Index.Columns)

	iter, err := p.snapshot.Iter(start, end)
	if err != nil {
		return nil, nil, false, err
	}
	defer iter.Close()
	if !iter.Valid() || bytes.Compare(iter.Key(), end) >= 0 {
		return nil, nil, true, nil
	}
	minHandle, err = tablecodec.DecodeIndexHandle(iter.Key(), iter.Value(), colsLen)
	if err != nil {
		return nil, nil, false, err
	}

	rIter, err := p.snapshot.IterReverse(end, start)
	if err != nil {
		return nil, nil, false, err
	}
	defer rIter.Close()
	if !rIter.Valid() {
		// Both reads use one snapshot, so this shouldn't happen. Don't narrow.
		return nil, nil, false, nil
	}
	maxHandle, err = tablecodec.DecodeIndexHandle(rIter.Key(), rIter.Value(), colsLen)
	if err != nil {
		return nil, nil, false, err
	}
	return minHandle, maxHandle, true, nil
}

// intersect returns the parts of ranges within [p.lo, p.hi).
func (p *pkRangeProber) intersect(ranges []kv.KeyRange) []kv.KeyRange {
	result := make([]kv.KeyRange, 0, len(ranges))
	for _, r := range ranges {
		start, end := r.StartKey, r.EndKey
		if start.Cmp(p.lo) < 0 {
			start = p.lo
		}
		if len(end) == 0 || end.Cmp(p.hi) > 0 {
			end = p.hi
		}
		if start.Cmp(end) < 0 {
			result = append(result, kv.KeyRange{StartKey: start, EndKey: end})
		}
	}
	return result
}

func (p *pkRangeProber) result() string {
	switch {
	case p.empty:
		return "empty"
	case p.lo != nil:
		return "narrowed"
	default:
		return "not narrowed"
	}
}

// pkRangeProbeRuntimeStats reports what the PK range probes of a table
// reader found, for EXPLAIN ANALYZE.
type pkRangeProbeRuntimeStats struct {
	result    string
	probeTime time.Duration
}

// String implements the execdetails.RuntimeStats interface.
func (s *pkRangeProbeRuntimeStats) String() string {
	return fmt.Sprintf("pk_range_probe:{result:%s, time:%s}", s.result, execdetails.FormatDuration(s.probeTime))
}

// Merge implements the execdetails.RuntimeStats interface. The probes run
// once per executor, so there is nothing to add up.
func (*pkRangeProbeRuntimeStats) Merge(execdetails.RuntimeStats) {}

// Clone implements the execdetails.RuntimeStats interface.
func (s *pkRangeProbeRuntimeStats) Clone() execdetails.RuntimeStats {
	cloned := *s
	return &cloned
}

// Tp implements the execdetails.RuntimeStats interface.
func (*pkRangeProbeRuntimeStats) Tp() int {
	return execdetails.TpPKRangeProbeRuntimeStats
}
