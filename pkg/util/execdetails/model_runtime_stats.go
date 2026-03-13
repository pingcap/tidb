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

package execdetails

import (
	"bytes"
	"sort"
	"strconv"
	"time"
)

// ModelInferenceSummary holds model inference stats for explain analyze output.
type ModelInferenceSummary struct {
	ModelID            int64
	VersionID          int64
	Role               string
	Calls              int64
	Errors             int64
	TotalInferenceTime time.Duration
	TotalBatchSize     int64
	MaxBatchSize       int64
	TotalLoadTime      time.Duration
	LoadErrors         int64
}

// AvgBatchSize returns the average batch size for the inference summary.
func (s ModelInferenceSummary) AvgBatchSize() float64 {
	if s.Calls == 0 {
		return 0
	}
	return float64(s.TotalBatchSize) / float64(s.Calls)
}

// ModelRuntimeStats reports model inference runtime stats in explain analyze.
type ModelRuntimeStats struct {
	Summaries []ModelInferenceSummary
}

// String implements the RuntimeStats interface.
func (s *ModelRuntimeStats) String() string {
	if s == nil || len(s.Summaries) == 0 {
		return ""
	}
	summaries := append([]ModelInferenceSummary(nil), s.Summaries...)
	sort.Slice(summaries, func(i, j int) bool {
		if summaries[i].ModelID != summaries[j].ModelID {
			return summaries[i].ModelID < summaries[j].ModelID
		}
		if summaries[i].VersionID != summaries[j].VersionID {
			return summaries[i].VersionID < summaries[j].VersionID
		}
		return summaries[i].Role < summaries[j].Role
	})

	buf := bytes.NewBuffer(make([]byte, 0, 128))
	buf.WriteString("model_inference:")
	for i, summary := range summaries {
		if i == 0 {
			buf.WriteByte(' ')
		} else {
			buf.WriteString("; ")
		}
		buf.WriteString("model_id=")
		buf.WriteString(strconv.FormatInt(summary.ModelID, 10))
		buf.WriteString(", version_id=")
		buf.WriteString(strconv.FormatInt(summary.VersionID, 10))
		buf.WriteString(", role=")
		buf.WriteString(summary.Role)
		buf.WriteString(", calls=")
		buf.WriteString(strconv.FormatInt(summary.Calls, 10))
		buf.WriteString(", errors=")
		buf.WriteString(strconv.FormatInt(summary.Errors, 10))
		buf.WriteString(", total_ms=")
		buf.WriteString(strconv.FormatFloat(summary.TotalInferenceTime.Seconds()*1000, 'f', 2, 64))
		buf.WriteString(", avg_batch=")
		buf.WriteString(strconv.FormatFloat(summary.AvgBatchSize(), 'f', 2, 64))
		buf.WriteString(", max_batch=")
		buf.WriteString(strconv.FormatInt(summary.MaxBatchSize, 10))
		buf.WriteString(", load_ms=")
		buf.WriteString(strconv.FormatFloat(summary.TotalLoadTime.Seconds()*1000, 'f', 2, 64))
		buf.WriteString(", load_errors=")
		buf.WriteString(strconv.FormatInt(summary.LoadErrors, 10))
	}
	return buf.String()
}

// Merge implements the RuntimeStats interface.
func (s *ModelRuntimeStats) Merge(other RuntimeStats) {
	if other == nil {
		return
	}
	if rhs, ok := other.(*ModelRuntimeStats); ok {
		s.Summaries = append(s.Summaries, rhs.Summaries...)
	}
}

// Clone implements the RuntimeStats interface.
func (s *ModelRuntimeStats) Clone() RuntimeStats {
	if s == nil {
		return &ModelRuntimeStats{}
	}
	clone := make([]ModelInferenceSummary, len(s.Summaries))
	copy(clone, s.Summaries)
	return &ModelRuntimeStats{Summaries: clone}
}

// Tp implements the RuntimeStats interface.
func (*ModelRuntimeStats) Tp() int {
	return TpModelRuntimeStats
}
