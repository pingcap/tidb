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
// See the License for the specific language governing permissions and
// limitations under the License.

package execdetails

import (
	"bytes"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

// ExecDetails contains execution detail information.
type ExecDetails struct {
	ProcessTime   time.Duration
	WaitTime      time.Duration
	BackoffTime   time.Duration
	RequestCount  int
	TotalKeys     int64
	ProcessedKeys int64
}

// String implements the fmt.Stringer interface.
func (d ExecDetails) String() string {
	parts := make([]string, 0, 6)
	if d.ProcessTime > 0 {
		parts = append(parts, fmt.Sprintf("process_time:%v", d.ProcessTime))
	}
	if d.WaitTime > 0 {
		parts = append(parts, fmt.Sprintf("wait_time:%v", d.WaitTime))
	}
	if d.BackoffTime > 0 {
		parts = append(parts, fmt.Sprintf("backoff_time:%v", d.BackoffTime))
	}
	if d.RequestCount > 0 {
		parts = append(parts, fmt.Sprintf("request_count:%d", d.RequestCount))
	}
	if d.TotalKeys > 0 {
		parts = append(parts, fmt.Sprintf("total_keys:%d", d.TotalKeys))
	}
	if d.ProcessedKeys > 0 {
		parts = append(parts, fmt.Sprintf("processed_keys:%d", d.ProcessedKeys))
	}
	return strings.Join(parts, " ")
}

// RuntimeStats collects executors's execution info.
type RuntimeStats map[string]*RuntimeStat

// RuntimeStat collects one executor's execution info.
type RuntimeStat struct {
	// executor's Next() called times.
	loop int32
	// executor consume time.
	consume int64
	// executor return row count.
	rows int64
}

// NewRuntimeStats creates new executor collector.
func NewRuntimeStats() RuntimeStats {
	return RuntimeStats(make(map[string]*RuntimeStat))
}

// GetRuntimeStat gets execStat for a executor.
func (e RuntimeStats) GetRuntimeStat(planID string) *RuntimeStat {
	if e == nil {
		return nil
	}
	runtimecStat, exists := e[planID]
	if !exists {
		runtimecStat = &RuntimeStat{}
		e[planID] = runtimecStat
	}
	return runtimecStat
}

func (e RuntimeStats) String() string {
	var buff bytes.Buffer
	buff.WriteString("(")
	for planID, stat := range e {
		buff.WriteString(planID + ":" + stat.String() + ",")
	}
	buff.WriteString(")")
	return buff.String()
}

// Record records executor's execution.
func (e *RuntimeStat) Record(d time.Duration, rowNum int) {
	atomic.AddInt32(&e.loop, 1)
	atomic.AddInt64(&e.consume, int64(d))
	atomic.AddInt64(&e.rows, int64(rowNum))
}

func (e *RuntimeStat) String() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("time:%f, loops:%d, rows:%d", time.Duration(e.consume).Seconds()*1e3, e.loop, e.rows)
}
