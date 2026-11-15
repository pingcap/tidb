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

package remote

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/url"
	"time"

	"go.uber.org/zap"
)

const (
	spfreshBuildRetryInitialDelay = 100 * time.Millisecond
	spfreshBuildRetryMaxDelay     = 3 * time.Second
	// Flush and seal retain their existing bounded metadata fallback.
	maxConsecutiveSPFreshRecoveryMetadataMisses = 8
)

// spfreshRecoveryState is shared by PUT, flush, and seal responses. The
// fields are part of the SPFresh recovery wire protocol.
type spfreshRecoveryState struct {
	Status        string `json:"status"`
	RetryReason   string `json:"retry-reason"`
	RetryAttempt  uint64 `json:"retry-attempt"`
	NextRetryAtMs *int64 `json:"next-retry-at-ms"`
}

func isSPFreshBuildRecoveryStatus(status string) bool {
	return status == "retrying" || status == "paused"
}

func spfreshBuildRetryDelay(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	delay := spfreshBuildRetryInitialDelay
	for i := 0; i < attempt && delay < spfreshBuildRetryMaxDelay; i++ {
		if delay > spfreshBuildRetryMaxDelay/2 {
			return spfreshBuildRetryMaxDelay
		}
		delay *= 2
	}
	if delay > spfreshBuildRetryMaxDelay {
		return spfreshBuildRetryMaxDelay
	}
	return delay
}

// waitForSPFreshBuildRetry waits before polling the same idempotent barrier.
// A server-provided retry timestamp takes precedence. If it is absent, use a
// bounded exponential fallback and fail after repeated missing metadata so a
// malformed 202 response cannot create an unbounded hot loop.
func waitForSPFreshBuildRetry(
	ctx context.Context,
	nextRetryAtMs *int64,
	missingMetadataCount int,
) (int, error) {
	delay := spfreshBuildRetryInitialDelay
	if nextRetryAtMs == nil {
		if missingMetadataCount >= maxConsecutiveSPFreshRecoveryMetadataMisses {
			return missingMetadataCount, fmt.Errorf(
				"SPFresh build recovery response omitted retry metadata %d consecutive times",
				missingMetadataCount,
			)
		}
		delay = spfreshBuildRetryDelay(missingMetadataCount)
		missingMetadataCount++
	} else {
		missingMetadataCount = 0
		if until := time.Until(time.UnixMilli(*nextRetryAtMs)); until > delay {
			delay = until
		}
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return missingMetadataCount, ctx.Err()
	case <-timer.C:
		return missingMetadataCount, nil
	}
}

// spfreshBuildTaskState deliberately excludes aggregate writer watermarks:
// GET is a readiness hint, never an acknowledgement of this sender's chunk.
type spfreshBuildTaskState struct {
	spfreshRecoveryState
	TaskID string `json:"task-id"`
	Error  string `json:"error"`
}

// waitForSPFreshBuildRunning polls the original owner without uploading vector
// data. Only a subsequent PUT can acknowledge the pending writer/chunk pair.
// Invalid recovery metadata is terminal for the read-index subtask and causes
// DDL rollback; valid recovery has no poll-count limit.
func (c *chunkSender) waitForSPFreshBuildRunning(ctx context.Context, state spfreshRecoveryState) error {
	for attempt := 0; ; attempt++ {
		switch state.Status {
		case "retrying", "paused":
			if state.NextRetryAtMs == nil {
				return fmt.Errorf("invalid SPFresh build recovery response: %s requires next-retry-at-ms", state.Status)
			}
		case "running":
			// Running can race with a PUT's 202 decision and has no retry
			// timestamp. Use local backoff before confirming readiness.
		default:
			return fmt.Errorf("cannot resume SPFresh build PUT in task state %q", state.Status)
		}
		// Grow even when the worker keeps returning an expired timestamp.
		delay := spfreshBuildPollDelay(attempt, state.NextRetryAtMs, time.Now())
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}

		endpoint := fmt.Sprintf("%s/spfresh/build?cluster_id=%d&task_id=%s",
			c.e.addr, c.e.clusterID, url.QueryEscape(c.e.loadDataTaskID))
		body, err := sendLongOwnerBoundRequest(ctx, c.httpClient, http.MethodGet, endpoint, nil, c.e.retryCounter)
		if err != nil {
			return err
		}
		var task spfreshBuildTaskState
		if err := json.Unmarshal(body, &task); err != nil {
			return fmt.Errorf("invalid SPFresh build task state: %w", err)
		}
		if task.TaskID != c.e.loadDataTaskID {
			return fmt.Errorf("SPFresh build task identity mismatch: expected %q, got %q", c.e.loadDataTaskID, task.TaskID)
		}
		switch task.Status {
		case "running":
			if task.Error != "" {
				return fmt.Errorf("SPFresh build task failed: %s", task.Error)
			}
			return nil
		case "retrying", "paused":
			state = task.spfreshRecoveryState
		case "canceled":
			return ErrTaskCanceled
		default:
			return fmt.Errorf("cannot resume SPFresh build PUT in task state %q: %s", task.Status, task.Error)
		}
	}
}

func spfreshBuildPollDelay(attempt int, nextRetryAtMs *int64, now time.Time) time.Duration {
	// Equal jitter keeps a positive lower bound and avoids synchronized polls.
	base := spfreshBuildRetryDelay(attempt)
	delay := base/2 + time.Duration(rand.Int64N(int64(base/2)+1))
	if nextRetryAtMs != nil {
		delay = max(delay, time.UnixMilli(*nextRetryAtMs).Sub(now))
	}
	return delay
}

func (c *chunkSender) retrySPFreshBuildChunkRecovery(
	ctx context.Context,
	result *PutChunkResult,
	chunkID uint64,
) (*PutChunkResult, bool, error) {
	for {
		done, err := c.checkPutChunkTaskState(result)
		if err != nil || done {
			return result, done, err
		}
		if c.e.taskType != taskTypeSPFreshBuild ||
			(!result.acceptedForRecovery && !isSPFreshBuildRecoveryStatus(result.Status)) {
			return result, false, nil
		}

		c.e.logger.Debug("SPFresh build PUT accepted while remote task recovers",
			zap.String("taskID", c.e.loadDataTaskID),
			zap.String("status", result.Status),
			zap.String("retryReason", result.RetryReason),
			zap.Uint64("retryAttempt", result.RetryAttempt))
		if err := c.waitForSPFreshBuildRunning(ctx, result.spfreshRecoveryState); err != nil {
			return nil, false, err
		}
		buf, err := c.chunksCache.get(chunkID)
		if err != nil {
			return nil, false, err
		}
		result, err = c.sendPutChunk(ctx, chunkID, buf)
		if err != nil {
			return nil, false, err
		}
	}
}
