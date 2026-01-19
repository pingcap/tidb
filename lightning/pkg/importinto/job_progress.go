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

package importinto

import (
	"strconv"

	"github.com/docker/go-units"
	"github.com/pingcap/tidb/pkg/importsdk"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"go.uber.org/zap"
)

type jobProgressEstimator struct {
	logger       log.Logger
	isGlobalSort bool
}

func newJobProgressEstimator(logger log.Logger) *jobProgressEstimator {
	return &jobProgressEstimator{logger: logger}
}

func (e *jobProgressEstimator) parseHumanSize(jobID int64, sizeText string, warnMsg string) (int64, bool) {
	if sizeText == "" {
		return 0, false
	}
	size, err := units.FromHumanSize(sizeText)
	if err != nil {
		e.logger.Warn(warnMsg, zap.String("size", sizeText), zap.Int64("jobID", jobID), zap.Error(err))
		return 0, false
	}
	return size, true
}

func (e *jobProgressEstimator) updateJobTotalSize(
	jobID int64,
	job *ImportJob,
	status *importsdk.JobStatus,
	jobTotalSize map[int64]int64,
) int64 {
	total := jobTotalSize[jobID]
	if job != nil && job.TableMeta != nil && job.TableMeta.TotalSize > 0 {
		total = max(total, job.TableMeta.TotalSize)
	}
	if total <= 0 {
		if size, ok := e.parseHumanSize(jobID, status.SourceFileSize, "failed to parse source file size"); ok {
			total = max(total, size)
		}
	}
	if total <= 0 {
		if size, ok := e.parseHumanSize(jobID, status.TotalSize, "failed to parse total size"); ok {
			total = max(total, size)
		}
	}
	if total > 0 && total != jobTotalSize[jobID] {
		jobTotalSize[jobID] = total
	}
	return total
}

type jobProgressPhase struct {
	phase string
	steps []string
}

func (*jobProgressEstimator) isGlobalSortStatus(status *importsdk.JobStatus) bool {
	switch status.Phase {
	case "global-sorting", "resolving-conflicts":
		return true
	}
	switch status.Step {
	case "encode", "merge-sort", "ingest", "collect-conflicts", "conflict-resolution":
		return true
	}
	return false
}

func jobProgressPhases(isGlobalSort bool) []jobProgressPhase {
	if isGlobalSort {
		return []jobProgressPhase{
			{phase: "global-sorting", steps: []string{"encode", "merge-sort"}},
			{phase: "importing", steps: []string{"ingest"}},
			{phase: "resolving-conflicts", steps: []string{"collect-conflicts", "conflict-resolution"}},
			{phase: "validating", steps: []string{"post-process"}},
		}
	}
	return []jobProgressPhase{
		{phase: "importing", steps: []string{"import"}},
		{phase: "validating", steps: []string{"post-process"}},
	}
}

func clamp01(v float64) float64 {
	return max(0.0, min(v, 1.0))
}

func (e *jobProgressEstimator) stepRatio(status *importsdk.JobStatus) float64 {
	if status.Percent == "" || status.Percent == "N/A" {
		return 0
	}

	p, err := strconv.ParseFloat(status.Percent, 64)
	if err != nil {
		e.logger.Warn("failed to parse progress percent", zap.String("percent", status.Percent), zap.Int64("jobID", status.JobID), zap.Error(err))
		return 0
	}
	return clamp01(p / 100.0)
}

func findPhase(phases []jobProgressPhase, phase string) (int, bool) {
	for i, ph := range phases {
		if ph.phase == phase {
			return i, true
		}
	}
	return 0, false
}

func findStep(steps []string, step string) (int, bool) {
	for i, s := range steps {
		if s == step {
			return i, true
		}
	}
	return 0, false
}

func (e *jobProgressEstimator) jobProgress(status *importsdk.JobStatus) float64 {
	isGlobalSort := e.isGlobalSort
	phases := jobProgressPhases(isGlobalSort)
	if len(phases) == 0 {
		return 0
	}

	phase := status.Phase
	if phase == "" {
		return 0
	}
	phaseIdx, ok := findPhase(phases, phase)
	if !ok {
		return 0
	}

	ratio := e.stepRatio(status)
	stepIdx, ok := findStep(phases[phaseIdx].steps, status.Step)
	if !ok {
		stepIdx = 0
		ratio = 0
	}
	phaseProgress := (float64(stepIdx) + ratio) / float64(len(phases[phaseIdx].steps))
	phaseProgress = clamp01(phaseProgress)

	progress := (float64(phaseIdx) + phaseProgress) / float64(len(phases))
	return clamp01(progress)
}

func (e *jobProgressEstimator) estimateJobFinishedSize(
	status *importsdk.JobStatus,
	jobTotal int64,
	prevFinished int64,
) int64 {
	finished := prevFinished

	switch {
	case status.IsFinished():
		if jobTotal > 0 {
			finished = jobTotal
		}
	case status.IsFailed() || status.IsCancelled():
	default:
		if jobTotal <= 0 {
			break
		}
		progress := e.jobProgress(status)
		finished = max(finished, int64(float64(jobTotal)*progress))
	}

	if jobTotal > 0 {
		finished = min(finished, jobTotal)
	}
	return finished
}

func (e *jobProgressEstimator) updateJobProgress(
	job *ImportJob,
	status *importsdk.JobStatus,
	jobTotalSize map[int64]int64,
	jobFinishedSize map[int64]int64,
) {
	if !e.isGlobalSort && e.isGlobalSortStatus(status) {
		e.isGlobalSort = true
	}

	jobID := status.JobID
	jobTotal := e.updateJobTotalSize(jobID, job, status, jobTotalSize)
	jobFinishedSize[jobID] = e.estimateJobFinishedSize(status, jobTotal, jobFinishedSize[jobID])
}
