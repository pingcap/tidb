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
	"io"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	logbackup "github.com/pingcap/kvproto/pkg/logbackuppb"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func (p *PDSim) GetLogBackupClient(ctx context.Context, storeID uint64) (logbackup.LogBackupClient, error) {
	_ = ctx
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, ok := p.stores[storeID]; !ok {
		return nil, fmt.Errorf("store %d not found", storeID)
	}
	return &logBackupClientSim{pd: p, storeID: storeID}, nil
}

func (p *PDSim) ClearCache(ctx context.Context, storeID uint64) error {
	_ = ctx
	_ = storeID
	return nil
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
	p.globalCheckpoint = checkpoint
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

type logBackupClientSim struct {
	pd      *PDSim
	storeID uint64
}

func (c *logBackupClientSim) GetLastFlushTSOfRegion(
	ctx context.Context,
	in *logbackup.GetLastFlushTSOfRegionRequest,
	opts ...grpc.CallOption,
) (*logbackup.GetLastFlushTSOfRegionResponse, error) {
	_ = ctx
	_ = in
	_ = opts
	return nil, status.Error(codes.Unimplemented, "GetLastFlushTSOfRegion is legacy and disabled in DRR harness")
}

type flushEventStreamClient struct {
	ctx context.Context
	ch  <-chan *logbackup.SubscribeFlushEventResponse
}

func (f *flushEventStreamClient) Recv() (*logbackup.SubscribeFlushEventResponse, error) {
	select {
	case msg, ok := <-f.ch:
		if !ok {
			return nil, io.EOF
		}
		return msg, nil
	case <-f.ctx.Done():
		return nil, status.Error(codes.Canceled, f.ctx.Err().Error())
	}
}

func (f *flushEventStreamClient) Header() (metadata.MD, error) {
	return metadata.MD{}, nil
}

func (f *flushEventStreamClient) Trailer() metadata.MD {
	return metadata.MD{}
}

func (f *flushEventStreamClient) CloseSend() error {
	return nil
}

func (f *flushEventStreamClient) Context() context.Context {
	return f.ctx
}

func (f *flushEventStreamClient) SendMsg(any) error {
	return nil
}

func (f *flushEventStreamClient) RecvMsg(any) error {
	return nil
}

func (p *PDSim) addSubscriber(storeID uint64, ch chan *logbackup.SubscribeFlushEventResponse) {
	p.mu.Lock()
	defer p.mu.Unlock()

	set, ok := p.subscribers[storeID]
	if !ok {
		set = make(map[chan *logbackup.SubscribeFlushEventResponse]struct{})
		p.subscribers[storeID] = set
	}
	set[ch] = struct{}{}
}

func (p *PDSim) removeSubscriber(storeID uint64, ch chan *logbackup.SubscribeFlushEventResponse) {
	p.mu.Lock()
	defer p.mu.Unlock()

	set, ok := p.subscribers[storeID]
	if !ok {
		return
	}
	delete(set, ch)
	if len(set) == 0 {
		delete(p.subscribers, storeID)
	}
}

func (c *logBackupClientSim) SubscribeFlushEvent(
	ctx context.Context,
	in *logbackup.SubscribeFlushEventRequest,
	opts ...grpc.CallOption,
) (logbackup.LogBackup_SubscribeFlushEventClient, error) {
	_ = in
	_ = opts

	ch := make(chan *logbackup.SubscribeFlushEventResponse, 1024)
	c.pd.addSubscriber(c.storeID, ch)
	go func() {
		<-ctx.Done()
		c.pd.removeSubscriber(c.storeID, ch)
	}()
	return &flushEventStreamClient{ctx: ctx, ch: ch}, nil
}

func (c *logBackupClientSim) FlushNow(
	ctx context.Context,
	in *logbackup.FlushNowRequest,
	opts ...grpc.CallOption,
) (*logbackup.FlushNowResponse, error) {
	_ = ctx
	_ = in
	_ = opts
	return &logbackup.FlushNowResponse{
		Results: []*logbackup.FlushResult{{TaskName: "drr", Success: true}},
	}, nil
}
