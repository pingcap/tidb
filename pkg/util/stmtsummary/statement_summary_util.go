// Copyright 2019 PingCAP, Inc.
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

package stmtsummary

import (
	"bytes"
	"cmp"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/tikv/client-go/v2/util"
)

// Truncate SQL to maxSQLLength.
func formatSQL(sql string) string {
	maxSQLLength := StmtSummaryByDigestMap.maxSQLLength()
	length := len(sql)
	if length > maxSQLLength {
		var result strings.Builder
		result.WriteString(sql[:maxSQLLength])
		fmt.Fprintf(&result, "(len:%d)", length)
		return result.String()
	}
	return strings.Clone(sql)
}

// Format the backoffType map to a string or nil.
func formatBackoffTypes(backoffMap map[string]int) any {
	type backoffStat struct {
		backoffType string
		count       int
	}

	size := len(backoffMap)
	if size == 0 {
		return nil
	}

	backoffArray := make([]backoffStat, 0, len(backoffMap))
	for backoffType, count := range backoffMap {
		backoffArray = append(backoffArray, backoffStat{backoffType, count})
	}
	slices.SortFunc(backoffArray, func(i, j backoffStat) int {
		return cmp.Compare(j.count, i.count)
	})

	var buffer bytes.Buffer
	for index, stat := range backoffArray {
		if _, err := fmt.Fprintf(&buffer, "%v:%d", stat.backoffType, stat.count); err != nil {
			return "FORMAT ERROR"
		}
		if index < len(backoffArray)-1 {
			buffer.WriteString(",")
		}
	}
	return buffer.String()
}

func avgInt(sum int64, count int64) int64 {
	if count > 0 {
		return sum / count
	}
	return 0
}

func avgFloat(sum int64, count int64) float64 {
	if count > 0 {
		return float64(sum) / float64(count)
	}
	return 0
}

func avgSumFloat(sum float64, count int64) float64 {
	if count > 0 {
		return sum / float64(count)
	}
	return 0
}

func convertEmptyToNil(str string) any {
	if str == "" {
		return nil
	}
	return str
}

// StmtRUSummary is the request-units summary for each type of statements.
type StmtRUSummary struct {
	SumRRU            float64       `json:"sum_rru"`
	SumWRU            float64       `json:"sum_wru"`
	SumRUWaitDuration time.Duration `json:"sum_ru_wait_duration"`
	MaxRRU            float64       `json:"max_rru"`
	MaxWRU            float64       `json:"max_wru"`
	MaxRUWaitDuration time.Duration `json:"max_ru_wait_duration"`
}

// Add add a new sample value to the ru summary record.
func (s *StmtRUSummary) Add(info *util.RUDetails) {
	if info != nil {
		rru := info.RRU()
		s.SumRRU += rru
		if s.MaxRRU < rru {
			s.MaxRRU = rru
		}
		wru := info.WRU()
		s.SumWRU += wru
		if s.MaxWRU < wru {
			s.MaxWRU = wru
		}
		ruWaitDur := info.RUWaitDuration()
		s.SumRUWaitDuration += ruWaitDur
		if s.MaxRUWaitDuration < ruWaitDur {
			s.MaxRUWaitDuration = ruWaitDur
		}
	}
}

// Merge merges the value of 2 ru summary records.
func (s *StmtRUSummary) Merge(other *StmtRUSummary) {
	s.SumRRU += other.SumRRU
	s.SumWRU += other.SumWRU
	s.SumRUWaitDuration += other.SumRUWaitDuration
	if s.MaxRRU < other.MaxRRU {
		s.MaxRRU = other.MaxRRU
	}
	if s.MaxWRU < other.MaxWRU {
		s.MaxWRU = other.MaxWRU
	}
	if s.MaxRUWaitDuration < other.MaxRUWaitDuration {
		s.MaxRUWaitDuration = other.MaxRUWaitDuration
	}
}

// StmtNetworkTrafficSummary is the network traffic summary for each type of statements.
type StmtNetworkTrafficSummary struct {
	UnpackedBytesSentTiKVTotal            int64 `json:"unpacked_bytes_send_tikv_total"`
	UnpackedBytesReceivedTiKVTotal        int64 `json:"unpacked_bytes_received_tikv_total"`
	UnpackedBytesSentTiKVCrossZone        int64 `json:"unpacked_bytes_send_tikv_cross_zone"`
	UnpackedBytesReceivedTiKVCrossZone    int64 `json:"unpacked_bytes_received_tikv_cross_zone"`
	UnpackedBytesSentTiFlashTotal         int64 `json:"unpacked_bytes_send_tiflash_total"`
	UnpackedBytesReceivedTiFlashTotal     int64 `json:"unpacked_bytes_received_tiflash_total"`
	UnpackedBytesSentTiFlashCrossZone     int64 `json:"unpacked_bytes_send_tiflash_cross_zone"`
	UnpackedBytesReceivedTiFlashCrossZone int64 `json:"unpacked_bytes_received_tiflash_cross_zone"`
}

// Merge merges the value of 2 network traffic summary records.
func (s *StmtNetworkTrafficSummary) Merge(other *StmtNetworkTrafficSummary) {
	if other == nil {
		return
	}
	s.UnpackedBytesSentTiKVTotal += other.UnpackedBytesSentTiKVTotal
	s.UnpackedBytesReceivedTiKVTotal += other.UnpackedBytesReceivedTiKVTotal
	s.UnpackedBytesSentTiKVCrossZone += other.UnpackedBytesSentTiKVCrossZone
	s.UnpackedBytesReceivedTiKVCrossZone += other.UnpackedBytesReceivedTiKVCrossZone
	s.UnpackedBytesSentTiFlashTotal += other.UnpackedBytesSentTiFlashTotal
	s.UnpackedBytesReceivedTiFlashTotal += other.UnpackedBytesReceivedTiFlashTotal
	s.UnpackedBytesSentTiFlashCrossZone += other.UnpackedBytesSentTiFlashCrossZone
	s.UnpackedBytesReceivedTiFlashCrossZone += other.UnpackedBytesReceivedTiFlashCrossZone
}

// Add add a new sample value to the ru summary record.
func (s *StmtNetworkTrafficSummary) Add(info *util.ExecDetails) {
	if info != nil {
		s.UnpackedBytesSentTiKVTotal += info.UnpackedBytesSentKVTotal
		s.UnpackedBytesReceivedTiKVTotal += info.UnpackedBytesReceivedKVTotal
		s.UnpackedBytesSentTiKVCrossZone += info.UnpackedBytesSentKVCrossZone
		s.UnpackedBytesReceivedTiKVCrossZone += info.UnpackedBytesReceivedKVCrossZone
		s.UnpackedBytesSentTiFlashTotal += info.UnpackedBytesSentMPPTotal
		s.UnpackedBytesReceivedTiFlashTotal += info.UnpackedBytesReceivedMPPTotal
		s.UnpackedBytesSentTiFlashCrossZone += info.UnpackedBytesSentMPPCrossZone
		s.UnpackedBytesReceivedTiFlashCrossZone += info.UnpackedBytesReceivedMPPCrossZone
	}
}
