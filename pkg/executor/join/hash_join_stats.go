// Copyright 2025 PingCAP, Inc.
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

package join

import (
	"bytes"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/execdetails"
)

func writeSpilledPartitionNumStatsToString(buf *bytes.Buffer, partitionNum int, spilledPartitionNumPerRound []int) {
	buf.WriteString("[")
	fmt.Fprintf(buf, "%d/%d", spilledPartitionNumPerRound[0], partitionNum)
	for i := 1; i < len(spilledPartitionNumPerRound); i++ {
		fmt.Fprintf(buf, " %d/%d", spilledPartitionNumPerRound[i], spilledPartitionNumPerRound[i-1]*partitionNum)
	}
	buf.WriteString("]")
}

func writeBytesStatsToString(buf *bytes.Buffer, convertedBytes []int64) {
	buf.WriteString("[")
	for i, byte := range convertedBytes {
		if i == 0 {
			fmt.Fprintf(buf, "%.2f", util.ByteToGiB(float64(byte)))
		} else {
			fmt.Fprintf(buf, " %.2f", util.ByteToGiB(float64(byte)))
		}
	}
	buf.WriteString("]")
}

type hashJoinRuntimeStats struct {
	fetchAndBuildHashTable time.Duration
	hashStat               hashStatistic
	fetchAndProbe          int64
	probe                  int64
	concurrent             int
	maxFetchAndProbe       int64
}

// Tp implements the RuntimeStats interface.
func (*hashJoinRuntimeStats) Tp() int {
	return execdetails.TpHashJoinRuntimeStats
}

func (e *hashJoinRuntimeStats) String() string {
	buf := bytes.NewBuffer(make([]byte, 0, 128))
	if e.fetchAndBuildHashTable > 0 {
		buf.WriteString("build_hash_table:{total:")
		buf.WriteString(execdetails.FormatDuration(e.fetchAndBuildHashTable))
		buf.WriteString(", fetch:")
		buf.WriteString(execdetails.FormatDuration(e.fetchAndBuildHashTable - e.hashStat.buildTableElapse))
		buf.WriteString(", build:")
		buf.WriteString(execdetails.FormatDuration(e.hashStat.buildTableElapse))
		buf.WriteString("}")
	}
	if e.probe > 0 {
		buf.WriteString(", probe:{concurrency:")
		buf.WriteString(strconv.Itoa(e.concurrent))
		buf.WriteString(", total:")
		buf.WriteString(execdetails.FormatDuration(time.Duration(e.fetchAndProbe)))
		buf.WriteString(", max:")
		buf.WriteString(execdetails.FormatDuration(time.Duration(atomic.LoadInt64(&e.maxFetchAndProbe))))
		buf.WriteString(", probe:")
		buf.WriteString(execdetails.FormatDuration(time.Duration(e.probe)))
		// fetch time is the time wait fetch result from its child executor,
		// wait time is the time wait its parent executor to fetch the joined result
		buf.WriteString(", fetch and wait:")
		buf.WriteString(execdetails.FormatDuration(time.Duration(e.fetchAndProbe - e.probe)))
		if e.hashStat.probeCollision > 0 {
			buf.WriteString(", probe_collision:")
			buf.WriteString(strconv.FormatInt(e.hashStat.probeCollision, 10))
		}
		buf.WriteString("}")
	}
	return buf.String()
}

func (e *hashJoinRuntimeStats) Clone() execdetails.RuntimeStats {
	return &hashJoinRuntimeStats{
		fetchAndBuildHashTable: e.fetchAndBuildHashTable,
		hashStat:               e.hashStat,
		fetchAndProbe:          e.fetchAndProbe,
		probe:                  e.probe,
		concurrent:             e.concurrent,
		maxFetchAndProbe:       e.maxFetchAndProbe,
	}
}

func (e *hashJoinRuntimeStats) Merge(rs execdetails.RuntimeStats) {
	tmp, ok := rs.(*hashJoinRuntimeStats)
	if !ok {
		return
	}
	e.fetchAndBuildHashTable += tmp.fetchAndBuildHashTable
	e.hashStat.buildTableElapse += tmp.hashStat.buildTableElapse
	e.hashStat.probeCollision += tmp.hashStat.probeCollision
	e.fetchAndProbe += tmp.fetchAndProbe
	e.probe += tmp.probe
	if e.maxFetchAndProbe < tmp.maxFetchAndProbe {
		e.maxFetchAndProbe = tmp.maxFetchAndProbe
	}
}

type hashStatistic struct {
	// NOTE: probeCollision may be accessed from multiple goroutines concurrently.
	probeCollision   int64
	buildTableElapse time.Duration
}

func (s *hashStatistic) String() string {
	return fmt.Sprintf("probe_collision:%v, build:%v", s.probeCollision, execdetails.FormatDuration(s.buildTableElapse))
}

type spillStats struct {
	round                            int
	totalSpillBytesPerRound          []int64
	spilledPartitionNumPerRound      []int
	spillBuildRowTableBytesPerRound  []int64
	spillBuildHashTableBytesPerRound []int64

	partitionNum int
}

type hashJoinRuntimeStatsV2 struct {
	concurrent      int
	buildConcurrent int
	probeCollision  int64

	fetchAndBuildHashTable int64

	partitionData       int64
	buildHashTable      int64
	probe               int64
	fetchAndProbe       int64
	workerFetchAndProbe int64

	maxPartitionData       int64
	maxBuildHashTable      int64
	maxProbe               int64
	maxWorkerFetchAndProbe int64

	maxPartitionDataForCurrentRound       int64
	maxBuildHashTableForCurrentRound      int64
	maxProbeForCurrentRound               int64
	maxWorkerFetchAndProbeForCurrentRound int64

	spill spillStats

	isHashJoinGA bool
}

func setMaxValue(addr *int64, currentValue int64) {
	for {
		value := atomic.LoadInt64(addr)
		if currentValue <= value {
			return
		}
		if atomic.CompareAndSwapInt64(addr, value, currentValue) {
			return
		}
	}
}

func (e *hashJoinRuntimeStatsV2) reset() {
	e.probeCollision = 0
	e.fetchAndBuildHashTable = 0
	e.partitionData = 0
	e.buildHashTable = 0
	e.probe = 0
	e.fetchAndProbe = 0
	e.workerFetchAndProbe = 0
	e.maxPartitionData = 0
	e.maxBuildHashTable = 0
	e.maxProbe = 0
	e.maxWorkerFetchAndProbe = 0
	e.maxPartitionDataForCurrentRound = 0
	e.maxBuildHashTableForCurrentRound = 0
	e.maxProbeForCurrentRound = 0
	e.maxWorkerFetchAndProbeForCurrentRound = 0
}

func (e *hashJoinRuntimeStatsV2) resetCurrentRound() {
	e.maxPartitionData += e.maxPartitionDataForCurrentRound
	e.maxBuildHashTable += e.maxBuildHashTableForCurrentRound
	e.maxProbe += e.maxProbeForCurrentRound
	e.maxWorkerFetchAndProbe += e.maxWorkerFetchAndProbeForCurrentRound
	e.maxPartitionDataForCurrentRound = 0
	e.maxBuildHashTableForCurrentRound = 0
	e.maxProbeForCurrentRound = 0
	e.maxWorkerFetchAndProbeForCurrentRound = 0
}

// Tp implements the RuntimeStats interface.
func (*hashJoinRuntimeStatsV2) Tp() int {
	return execdetails.TpHashJoinRuntimeStatsV2
}

func (e *hashJoinRuntimeStatsV2) String() string {
	buf := bytes.NewBuffer(make([]byte, 0, 128))
	if e.fetchAndBuildHashTable > 0 {
		if e.isHashJoinGA {
			buf.WriteString("build_hash_table:{concurrency:")
			buf.WriteString(strconv.Itoa(e.buildConcurrent))
			buf.WriteString(", time:")
			buf.WriteString(execdetails.FormatDuration(time.Duration(e.fetchAndBuildHashTable)))
			buf.WriteString(", fetch:")
			buf.WriteString(execdetails.FormatDuration(time.Duration(e.fetchAndBuildHashTable - e.maxBuildHashTable - e.maxPartitionData)))
			buf.WriteString(", max_partition:")
			buf.WriteString(execdetails.FormatDuration(time.Duration(e.maxPartitionData)))
			buf.WriteString(", total_partition:")
			buf.WriteString(execdetails.FormatDuration(time.Duration(e.partitionData)))
			buf.WriteString(", max_build:")
			buf.WriteString(execdetails.FormatDuration(time.Duration(e.maxBuildHashTable)))
			buf.WriteString(", total_build:")
			buf.WriteString(execdetails.FormatDuration(time.Duration(e.buildHashTable)))
			buf.WriteString("}")
		} else {
			buf.WriteString("build_hash_table:{total:")
			buf.WriteString(execdetails.FormatDuration(time.Duration(e.fetchAndBuildHashTable)))
			buf.WriteString(", fetch:")
			buf.WriteString(execdetails.FormatDuration(time.Duration(e.fetchAndBuildHashTable - e.maxBuildHashTable - e.maxPartitionData)))
			buf.WriteString(", build:")
			buf.WriteString(execdetails.FormatDuration(time.Duration(e.maxBuildHashTable + e.maxPartitionData)))
			buf.WriteString("}")
		}
	}

	if e.probe > 0 {
		buf.WriteString(", probe:{concurrency:")
		buf.WriteString(strconv.Itoa(e.concurrent))
		if e.isHashJoinGA {
			buf.WriteString(", time:")
			buf.WriteString(execdetails.FormatDuration(time.Duration(e.fetchAndProbe)))
			buf.WriteString(", fetch_and_wait:")
			buf.WriteString(execdetails.FormatDuration(time.Duration(e.fetchAndProbe - e.maxProbe)))
			buf.WriteString(", max_worker_time:")
			buf.WriteString(execdetails.FormatDuration(time.Duration(e.maxWorkerFetchAndProbe)))
			buf.WriteString(", total_worker_time:")
			buf.WriteString(execdetails.FormatDuration(time.Duration(e.workerFetchAndProbe)))
			buf.WriteString(", max_probe:")
			buf.WriteString(execdetails.FormatDuration(time.Duration(e.maxProbe)))
			buf.WriteString(", total_probe:")
			buf.WriteString(execdetails.FormatDuration(time.Duration(e.probe)))
		} else {
			buf.WriteString(", total:")
			buf.WriteString(execdetails.FormatDuration(time.Duration(e.fetchAndProbe)))
			buf.WriteString(", max:")
			buf.WriteString(execdetails.FormatDuration(time.Duration(atomic.LoadInt64(&e.maxWorkerFetchAndProbe))))
			buf.WriteString(", probe:")
			buf.WriteString(execdetails.FormatDuration(time.Duration(e.maxProbe)))
			buf.WriteString(", fetch_and_wait:")
			buf.WriteString(execdetails.FormatDuration(time.Duration(e.fetchAndProbe - e.maxProbe)))
		}

		if e.probeCollision > 0 {
			buf.WriteString(", probe_collision:")
			buf.WriteString(strconv.FormatInt(e.probeCollision, 10))
		}
		buf.WriteString("}")
	}
	if e.spill.round > 0 {
		buf.WriteString(", spill:{round:")
		buf.WriteString(strconv.Itoa(e.spill.round))
		buf.WriteString(", spilled_partition_num_per_round:")
		writeSpilledPartitionNumStatsToString(buf, e.spill.partitionNum, e.spill.spilledPartitionNumPerRound)
		buf.WriteString(", total_spill_GiB_per_round:")
		writeBytesStatsToString(buf, e.spill.totalSpillBytesPerRound)
		buf.WriteString(", build_spill_row_table_GiB_per_round:")
		writeBytesStatsToString(buf, e.spill.spillBuildRowTableBytesPerRound)
		buf.WriteString(", build_spill_hash_table_per_round:")
		writeBytesStatsToString(buf, e.spill.spillBuildHashTableBytesPerRound)
		buf.WriteString("}")
	}
	return buf.String()
}

func (e *hashJoinRuntimeStatsV2) Clone() execdetails.RuntimeStats {
	return &hashJoinRuntimeStatsV2{
		concurrent:                            e.concurrent,
		buildConcurrent:                       e.buildConcurrent,
		probeCollision:                        e.probeCollision,
		fetchAndBuildHashTable:                e.fetchAndBuildHashTable,
		partitionData:                         e.partitionData,
		buildHashTable:                        e.buildHashTable,
		probe:                                 e.probe,
		fetchAndProbe:                         e.fetchAndProbe,
		maxPartitionData:                      e.maxPartitionData,
		maxBuildHashTable:                     e.maxBuildHashTable,
		maxProbe:                              e.maxProbe,
		maxWorkerFetchAndProbe:                e.maxWorkerFetchAndProbe,
		maxPartitionDataForCurrentRound:       e.maxPartitionDataForCurrentRound,
		maxBuildHashTableForCurrentRound:      e.maxBuildHashTableForCurrentRound,
		maxProbeForCurrentRound:               e.maxProbeForCurrentRound,
		maxWorkerFetchAndProbeForCurrentRound: e.maxWorkerFetchAndProbeForCurrentRound,
	}
}

func (e *hashJoinRuntimeStatsV2) Merge(rs execdetails.RuntimeStats) {
	tmp, ok := rs.(*hashJoinRuntimeStatsV2)
	if !ok {
		return
	}
	e.fetchAndBuildHashTable += tmp.fetchAndBuildHashTable
	e.buildHashTable += tmp.buildHashTable
	if e.maxBuildHashTable < tmp.maxBuildHashTable {
		e.maxBuildHashTable = tmp.maxBuildHashTable
	}
	e.partitionData += tmp.partitionData
	if e.maxPartitionData < tmp.maxPartitionData {
		e.maxPartitionData = tmp.maxPartitionData
	}
	e.probeCollision += tmp.probeCollision
	e.fetchAndProbe += tmp.fetchAndProbe
	e.probe += tmp.probe
	if e.maxWorkerFetchAndProbe < tmp.maxWorkerFetchAndProbe {
		e.maxWorkerFetchAndProbe = tmp.maxWorkerFetchAndProbe
	}
}
