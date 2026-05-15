// Copyright 2024 PingCAP, Inc.
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

package preparesnap

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/pingcap/errors"
	brpb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/utils"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type eventType int

const (
	eventMiscErr eventType = iota
	eventWaitApplyDone
)

type event struct {
	ty eventType

	storeID uint64
	err     error
	region  *metapb.Region
}

func (e event) String() string {
	return fmt.Sprintf("Event(Type: %v, StoreID: %v, Error: %v, Region: %v)", e.ty, e.storeID, e.err, e.region)
}

type prepareStream struct {
	storeID       uint64
	cli           PrepareClient
	leaseDuration time.Duration

	output       chan<- event
	serverStream <-chan utils.Result[*brpb.PrepareSnapshotBackupResponse]

	clientLoopHandle *errgroup.Group
	stopBgTasks      context.CancelFunc
}

// InitConn initializes the connection to the stream (i.e. "active" the stream).
//
// Before calling this, make sure you have filled the store ID and output channel.
//
// Once this has been called, them **should not be changed** any more.
func (p *prepareStream) InitConn(ctx context.Context, cli PrepareClient) error {
	p.cli = cli
	p.clientLoopHandle, ctx = errgroup.WithContext(ctx)
	ctx, p.stopBgTasks = context.WithCancel(ctx)
	log.Info("initializing", zap.Uint64("store", p.storeID))
	return p.GoLeaseLoop(ctx, p.leaseDuration)
}

// Finalize cuts down this connection and remove the lease.
// This will block until all messages has been flushed to `output` channel.
// After this return, no more messages should be appended to the `output` channel.
func (p *prepareStream) Finalize(ctx context.Context) error {
	log.Info("shutting down", zap.Uint64("store", p.storeID))
	return p.stopClientLoop(ctx)
}

func (p *prepareStream) GoLeaseLoop(ctx context.Context, dur time.Duration) error {
	err := p.cli.Send(&brpb.PrepareSnapshotBackupRequest{
		Ty:             brpb.PrepareSnapshotBackupRequestType_UpdateLease,
		LeaseInSeconds: uint64(dur.Seconds()),
	})
	if err != nil {
		return errors.Annotate(err, "failed to initialize the lease")
	}
	msg, err := p.cli.Recv()
	if err != nil {
		return errors.Annotate(err, "failed to recv the initialize lease result")
	}
	if msg.Ty != brpb.PrepareSnapshotBackupEventType_UpdateLeaseResult {
		return errors.Errorf("unexpected type of response during creating lease loop: it is %s", msg.Ty)
	}
	p.serverStream = utils.AsyncStreamBy(p.cli.Recv)
	p.clientLoopHandle.Go(func() error { return p.clientLoop(ctx, dur) })
	return nil
}

func (p *prepareStream) onResponse(ctx context.Context, res utils.Result[*brpb.PrepareSnapshotBackupResponse]) error {
	if err := res.Err; err != nil {
		return err
	}
	resp := res.Item
	logutil.CL(ctx).Debug("received response", zap.Stringer("resp", resp))
	evt, needDeliver := p.convertToEvent(resp)
	if needDeliver {
		logutil.CL(ctx).Debug("generating internal event", zap.Stringer("event", evt))
		p.output <- evt
	}
	return nil
}

func (p *prepareStream) stopClientLoop(ctx context.Context) error {
	p.stopBgTasks()
	err := p.cli.Send(&brpb.PrepareSnapshotBackupRequest{
		Ty: brpb.PrepareSnapshotBackupRequestType_Finish,
	})
	if err != nil {
		return errors.Annotate(err, "failed to send finish request")
	}
recv_loop:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case res, ok := <-p.serverStream:
			err := p.onResponse(ctx, res)
			if err == io.EOF || !ok {
				logutil.CL(ctx).Info("close loop done.", zap.Uint64("store", p.storeID), zap.Bool("is-chan-closed", !ok))
				break recv_loop
			}
			if err != nil {
				return err
			}
		}
	}
	return p.clientLoopHandle.Wait()
}

func (p *prepareStream) clientLoop(ctx context.Context, dur time.Duration) error {
	ticker := time.NewTicker(dur / 4)
	lastSuccess := time.Unix(0, 0)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			logutil.CL(ctx).Info("client loop exits.", zap.Uint64("store", p.storeID))
			return nil
		case res := <-p.serverStream:
			if err := p.onResponse(ctx, res); err != nil {
				err = errors.Annotate(err, "failed to recv from the stream")
				p.sendErr(err)
				return err
			}
		case <-ticker.C:
			err := p.cli.Send(&brpb.PrepareSnapshotBackupRequest{
				Ty:             brpb.PrepareSnapshotBackupRequestType_UpdateLease,
				LeaseInSeconds: uint64(dur.Seconds()),
			})
			if err != nil {
				log.Warn("failed to update the lease loop", logutil.ShortError(err))
				if time.Since(lastSuccess) > dur {
					err := errors.Annotate(err, "too many times failed to update the lease, it is probably expired")
					p.output <- event{
						ty:      eventMiscErr,
						storeID: p.storeID,
						err:     err,
					}
					return err
				}
			} else {
				lastSuccess = time.Now()
			}
		}
	}
}

func (p *prepareStream) sendErr(err error) {
	p.output <- event{
		ty:      eventMiscErr,
		storeID: p.storeID,
		err:     err,
	}
}

func (p *prepareStream) convertToEvent(resp *brpb.PrepareSnapshotBackupResponse) (event, bool) {
	if resp == nil {
		log.Warn("Received nil message, that shouldn't happen in a normal cluster.", zap.Uint64("store", p.storeID))
		return event{}, false
	}
	switch resp.Ty {
	case brpb.PrepareSnapshotBackupEventType_WaitApplyDone:
		return event{
			ty:      eventWaitApplyDone,
			storeID: p.storeID,
			region:  resp.Region,
			err:     convertErr(resp.Error),
		}, true
	case brpb.PrepareSnapshotBackupEventType_UpdateLeaseResult:
		if !resp.LastLeaseIsValid {
			return event{
				ty:      eventMiscErr,
				storeID: p.storeID,
				err:     leaseExpired(),
			}, true
		}
		return event{}, false
	}
	return event{
		ty:      eventMiscErr,
		storeID: p.storeID,
		err: errors.Annotatef(unsupported(), "unknown response type %v (%d)",
			resp.Ty, resp.Ty),
	}, true
}
