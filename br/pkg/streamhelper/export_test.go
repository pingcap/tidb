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

package streamhelper

import (
	"context"
	"time"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/streamhelper/spans"
	"github.com/tikv/client-go/v2/oracle"
)

func (c *CheckpointAdvancer) TESTResolveLockTargetCount() int {
	c.lastCheckpointMu.Lock()
	checkpointToResolve := c.lastCheckpoint
	c.lastCheckpointMu.Unlock()
	if checkpointToResolve == nil {
		return 0
	}
	currentTS, err := c.env.FetchCurrentTS(context.Background())
	if err != nil {
		return 0
	}
	upperBound := resolveLockTargetUpperBound(checkpointToResolve.TS, c.getResolveLockInterval(), currentTS)
	return len(c.resolveLockTargetsForCheckpoint(checkpointToResolve, upperBound))
}

func (c *CheckpointAdvancer) TESTSetLastCheckpointToCurrentMin() {
	var p *checkpoint
	c.WithCheckpoints(func(vsf *spans.ValueSortedFull) {
		p = NewCheckpointWithSpan(vsf.Min())
	})
	c.UpdateLastCheckpoint(p)
}

func (c *CheckpointAdvancer) TESTTryResolveLocksForCheckpoint() {
	c.tryResolveLocksForCheckpoint(context.Background())
}

func (c *CheckpointAdvancer) TESTRefreshLogBackupFlushInterval(ctx context.Context) {
	c.refreshLogBackupFlushInterval(ctx)
}

func (c *CheckpointAdvancer) TESTResolveLockInterval() time.Duration {
	return c.getResolveLockInterval()
}

func (c *CheckpointAdvancer) TESTDefaultStartPollThreshold() time.Duration {
	return c.getDefaultStartPollThreshold()
}

func (c *CheckpointAdvancer) TESTSubscriberErrorStartPollThreshold() time.Duration {
	return c.getSubscriberErrorStartPollThreshold()
}

func TESTResolveLockTargetUpperBound(checkpointTS uint64, resolveLockInterval time.Duration, now time.Time) uint64 {
	return resolveLockTargetUpperBound(checkpointTS, resolveLockInterval, oracle.GoTimeToTS(now))
}

func TESTResolveLockRetryLowerBound(checkpointTS uint64, maxVersion uint64) (uint64, bool) {
	return resolveLockRetryLowerBound(checkpointTS, maxVersion)
}

func TESTLowerResolveLockMaxVersion(maxVersion uint64, lowerBound uint64) (uint64, bool) {
	return lowerResolveLockMaxVersion(maxVersion, lowerBound)
}

func SetGlobalCheckpointStorageFactoryForTest(
	factory func(context.Context, *backuppb.StorageBackend, *storage.ExternalStorageOptions) (storage.ExternalStorage, error),
) func() {
	original := createGlobalCheckpointStorage
	createGlobalCheckpointStorage = factory
	return func() {
		createGlobalCheckpointStorage = original
	}
}

func SetMetadataWatchProgressForTest(interval, timeout time.Duration) func() {
	oldInterval := metadataWatchProgressInterval
	oldTimeout := metadataWatchIdleTimeout
	metadataWatchProgressInterval = interval
	metadataWatchIdleTimeout = timeout
	return func() {
		metadataWatchProgressInterval = oldInterval
		metadataWatchIdleTimeout = oldTimeout
	}
}
