// Copyright 2018 PingCAP, Inc.
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

	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
)

// InsertRuntimeStat record the stat about insert and check
type InsertRuntimeStat struct {
	*execdetails.BasicRuntimeStats
	*txnsnapshot.SnapshotRuntimeStats
	*autoid.AllocatorRuntimeStats
	CheckInsertTime time.Duration
	Prefetch        time.Duration
	FKCheckTime     time.Duration
}

func (e *InsertRuntimeStat) String() string {
	buf := bytes.NewBuffer(make([]byte, 0, 32))
	var allocatorStatsStr string
	if e.AllocatorRuntimeStats != nil {
		allocatorStatsStr = e.AllocatorRuntimeStats.String()
	}
	if e.CheckInsertTime == 0 {
		// For replace statement.
		if allocatorStatsStr != "" {
			buf.WriteString(allocatorStatsStr)
		}
		if e.Prefetch > 0 && e.SnapshotRuntimeStats != nil {
			if buf.Len() > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString("prefetch: ")
			buf.WriteString(execdetails.FormatDuration(e.Prefetch))
			buf.WriteString(", rpc: {")
			buf.WriteString(e.SnapshotRuntimeStats.String())
			buf.WriteString("}")
		}
		return buf.String()
	}
	if allocatorStatsStr != "" {
		buf.WriteString("prepare: {total: ")
		buf.WriteString(execdetails.FormatDuration(time.Duration(e.BasicRuntimeStats.GetTime()) - e.CheckInsertTime))
		buf.WriteString(", ")
		buf.WriteString(allocatorStatsStr)
		buf.WriteString("}, ")
	} else {
		buf.WriteString("prepare: ")
		buf.WriteString(execdetails.FormatDuration(time.Duration(e.BasicRuntimeStats.GetTime()) - e.CheckInsertTime))
		buf.WriteString(", ")
	}
	if e.Prefetch > 0 {
		fmt.Fprintf(
			buf, "check_insert: {total_time: %v, mem_insert_time: %v, prefetch: %v",
			execdetails.FormatDuration(e.CheckInsertTime),
			execdetails.FormatDuration(e.CheckInsertTime-e.Prefetch),
			execdetails.FormatDuration(e.Prefetch),
		)
		if e.FKCheckTime > 0 {
			fmt.Fprintf(buf, ", fk_check: %v", execdetails.FormatDuration(e.FKCheckTime))
		}
		if e.SnapshotRuntimeStats != nil {
			if rpc := e.SnapshotRuntimeStats.String(); len(rpc) > 0 {
				fmt.Fprintf(buf, ", rpc:{%s}", rpc)
			}
		}
		buf.WriteString("}")
	} else {
		fmt.Fprintf(buf, "insert:%v", execdetails.FormatDuration(e.CheckInsertTime))
		if e.SnapshotRuntimeStats != nil {
			if rpc := e.SnapshotRuntimeStats.String(); len(rpc) > 0 {
				fmt.Fprintf(buf, ", rpc:{%s}", rpc)
			}
		}
	}
	return buf.String()
}

// Clone implements the RuntimeStats interface.
func (e *InsertRuntimeStat) Clone() execdetails.RuntimeStats {
	newRs := &InsertRuntimeStat{
		CheckInsertTime: e.CheckInsertTime,
		Prefetch:        e.Prefetch,
		FKCheckTime:     e.FKCheckTime,
	}
	if e.SnapshotRuntimeStats != nil {
		snapshotStats := e.SnapshotRuntimeStats.Clone()
		newRs.SnapshotRuntimeStats = snapshotStats
	}
	// BasicRuntimeStats is unique for all executor instances mapping to the same plan id
	newRs.BasicRuntimeStats = e.BasicRuntimeStats
	if e.AllocatorRuntimeStats != nil {
		newRs.AllocatorRuntimeStats = e.AllocatorRuntimeStats.Clone()
	}
	return newRs
}

// Merge implements the RuntimeStats interface.
func (e *InsertRuntimeStat) Merge(other execdetails.RuntimeStats) {
	tmp, ok := other.(*InsertRuntimeStat)
	if !ok {
		return
	}
	if tmp.SnapshotRuntimeStats != nil {
		if e.SnapshotRuntimeStats == nil {
			snapshotStats := tmp.SnapshotRuntimeStats.Clone()
			e.SnapshotRuntimeStats = snapshotStats
		} else {
			e.SnapshotRuntimeStats.Merge(tmp.SnapshotRuntimeStats)
		}
	}
	if tmp.BasicRuntimeStats != nil && e.BasicRuntimeStats == nil {
		e.BasicRuntimeStats = tmp.BasicRuntimeStats
	}
	if tmp.AllocatorRuntimeStats != nil {
		if e.AllocatorRuntimeStats == nil {
			e.AllocatorRuntimeStats = tmp.AllocatorRuntimeStats.Clone()
		} else {
			e.AllocatorRuntimeStats.Merge(tmp.AllocatorRuntimeStats)
		}
	}
	e.Prefetch += tmp.Prefetch
	e.FKCheckTime += tmp.FKCheckTime
	e.CheckInsertTime += tmp.CheckInsertTime
}

// Tp implements the RuntimeStats interface.
func (*InsertRuntimeStat) Tp() int {
	return execdetails.TpInsertRuntimeStat
}
