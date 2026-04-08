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

package testutil

import (
	"context"
	"fmt"
	"sync"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	logbackup "github.com/pingcap/kvproto/pkg/logbackuppb"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
)

func (p *PDSim) GetLogBackupClient(ctx context.Context, storeID uint64) (logbackup.LogBackupClient, error) {
	return p.Cluster.GetLogBackupClient(ctx, storeID)
}

func (p *PDSim) ClearCache(ctx context.Context, storeID uint64) error {
	return p.Cluster.ClearCache(ctx, storeID)
}

func (p *PDSim) Begin(ctx context.Context, ch chan<- streamhelper.TaskEvent) error {
	_ = ctx
	p.mu.Lock()
	p.taskCh = ch
	taskName := p.taskName
	taskStart := p.taskStart
	p.mu.Unlock()

	ch <- streamhelper.TaskEvent{
		Type: streamhelper.EventAdd,
		Name: taskName,
		Info: &backuppb.StreamBackupTaskInfo{
			Name:    taskName,
			StartTs: taskStart,
		},
		Ranges: []kv.KeyRange{{}},
	}
	return nil
}

func (p *PDSim) UploadV3GlobalCheckpointForTask(ctx context.Context, taskName string, checkpoint uint64) error {
	_ = ctx
	p.mu.Lock()
	defer p.mu.Unlock()

	if taskName != p.taskName {
		return fmt.Errorf("unknown task %q", taskName)
	}
	if checkpoint < p.globalCheckpoint {
		return fmt.Errorf("checkpoint rollback: %d -> %d", p.globalCheckpoint, checkpoint)
	}
	waiters := p.checkpointWaiters
	p.checkpointWaiters = nil
	p.globalCheckpoint = checkpoint

	for _, waiter := range waiters {
		close(waiter)
	}
	return nil
}

func (p *PDSim) GetGlobalCheckpointForTask(ctx context.Context, taskName string) (uint64, error) {
	_ = ctx
	p.mu.Lock()
	defer p.mu.Unlock()

	if taskName != p.taskName {
		return 0, fmt.Errorf("unknown task %q", taskName)
	}
	return p.globalCheckpoint, nil
}

func (p *PDSim) WaitGlobalCheckpointAdvance(ctx context.Context, taskName string, current uint64) error {
	p.mu.Lock()
	unlock := sync.Once{}
	defer unlock.Do(p.mu.Unlock)
	if p.taskName != taskName {
		return fmt.Errorf("task name mismatch: %s and %s", taskName, p.taskName)
	}
	if p.globalCheckpoint > current {
		return nil
	}
	waiter := make(chan struct{})
	p.checkpointWaiters = append(p.checkpointWaiters, waiter)

	unlock.Do(p.mu.Unlock)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-waiter:
		return nil
	}
}

func (p *PDSim) ClearV3GlobalCheckpointForTask(ctx context.Context, taskName string) error {
	_ = ctx
	p.mu.Lock()
	defer p.mu.Unlock()

	if taskName != p.taskName {
		return fmt.Errorf("unknown task %q", taskName)
	}
	p.globalCheckpoint = 0
	return nil
}

func (p *PDSim) PauseTask(ctx context.Context, taskName string, opts ...streamhelper.PauseTaskOption) error {
	_ = ctx
	_ = opts
	p.mu.Lock()
	if taskName != p.taskName {
		p.mu.Unlock()
		return fmt.Errorf("unknown task %q", taskName)
	}
	ch := p.taskCh
	p.mu.Unlock()

	if ch != nil {
		ch <- streamhelper.TaskEvent{Type: streamhelper.EventPause, Name: taskName}
	}
	return nil
}

func (p *PDSim) Identifier() string {
	return "drr-pd-sim"
}

func (p *PDSim) GetStore() tikv.Storage {
	panic("PDSim does not provide tikv.Storage; lock resolving is unsupported in DRR harness")
}

func (p *PDSim) ScanLocksInOneRegion(
	bo *tikv.Backoffer,
	key []byte,
	endKey []byte,
	maxVersion uint64,
	limit uint32,
) ([]*txnlock.Lock, *tikv.KeyLocation, error) {
	_ = bo
	_ = key
	_ = endKey
	_ = maxVersion
	_ = limit
	return nil, &tikv.KeyLocation{}, fmt.Errorf("lock scanning is unsupported in DRR harness")
}

func (p *PDSim) ResolveLocksInOneRegion(
	bo *tikv.Backoffer,
	locks []*txnlock.Lock,
	loc *tikv.KeyLocation,
) (*tikv.KeyLocation, error) {
	_ = bo
	_ = locks
	return loc, fmt.Errorf("lock resolving is unsupported in DRR harness")
}
