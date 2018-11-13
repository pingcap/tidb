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
	"fmt"
	"strings"
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
