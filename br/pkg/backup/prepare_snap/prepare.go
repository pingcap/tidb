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
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/google/btree"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	brpb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
)

const (
	/* The combination of defaultMaxRetry and defaultRetryBackoff limits
	   the whole procedure to about 5 min if there is a region always fail.
	   Also note that we are batching during retrying. Retrying many region
	   costs only one chance of retrying if they are batched. */

	defaultMaxRetry     = 60
	defaultRetryBackoff = 5 * time.Second
	defaultLeaseDur     = 120 * time.Second

	/* Give pd enough time to find the region. If we aren't able to fetch
	   the region, the whole procedure might be aborted. */

	regionCacheMaxBackoffMs = 60000
)

type pendingRequests map[uint64]*brpb.PrepareSnapshotBackupRequest

type rangeOrRegion struct {
	// If it is a range, this should be zero.
	id       uint64
	startKey []byte
	endKey   []byte
}

func (r rangeOrRegion) String() string {
	rng := logutil.StringifyRangeOf(r.startKey, r.endKey)
	if r.id == 0 {
		return fmt.Sprintf("range%s", rng)
	}
	return fmt.Sprintf("region(id=%d, range=%s)", r.id, rng)
}

func (r rangeOrRegion) compareWith(than rangeOrRegion) bool {
	return bytes.Compare(r.startKey, than.startKey) < 0
}

type Preparer struct {
	/* Environments. */
	env Env

	/* Internal Status. */
	inflightReqs         map[uint64]metapb.Region
	failed               []rangeOrRegion
	waitApplyDoneRegions btree.BTreeG[rangeOrRegion]
	retryTime            int
	nextRetry            *time.Timer

	/* Internal I/O. */
	eventChan chan event
	clients   map[uint64]*prepareStream

	/* Interface for caller. */
	waitApplyFinished bool

	/* Some configurations. They aren't thread safe.
	   You may need to configure them before starting the Preparer. */
	RetryBackoff  time.Duration
	RetryLimit    int
	LeaseDuration time.Duration

	/* Observers. Initialize them before starting.*/
	AfterConnectionsEstablished func()
}

func New(env Env) *Preparer {
	prep := &Preparer{
		env: env,

		inflightReqs:         make(map[uint64]metapb.Region),
		waitApplyDoneRegions: *btree.NewG(16, rangeOrRegion.compareWith),
		eventChan:            make(chan event, 128),
		clients:              make(map[uint64]*prepareStream),

		RetryBackoff:  defaultRetryBackoff,
		RetryLimit:    defaultMaxRetry,
		LeaseDuration: defaultLeaseDur,
	}
	return prep
}

func (p *Preparer) MarshalLogObject(om zapcore.ObjectEncoder) error {
	om.AddInt("inflight_requests", len(p.inflightReqs))
	reqs := 0
	for _, r := range p.inflightReqs {
		om.AddString("simple_inflight_region", rangeOrRegion{id: r.Id, startKey: r.StartKey, endKey: r.EndKey}.String())
		reqs += 1
		if reqs > 3 {
			break
		}
	}
	om.AddInt("failed_requests", len(p.failed))
	failed := 0
	for _, r := range p.failed {
		om.AddString("simple_failed_region", r.String())
		failed += 1
		if failed > 5 {
			break
		}
	}
	err := om.AddArray("connected_stores", zapcore.ArrayMarshalerFunc(func(ae zapcore.ArrayEncoder) error {
		for id := range p.clients {
			ae.AppendUint64(id)
		}
		return nil
	}))
	if err != nil {
		return err
	}
	om.AddInt("retry_time", p.retryTime)
	om.AddBool("wait_apply_finished", p.waitApplyFinished)
	return nil
}

// DriveLoopAndWaitPrepare drives the state machine and block the
// current goroutine until we are safe to start taking snapshot.
//
// After this invoked, you shouldn't share this `Preparer` with any other goroutines.
//
// After this the cluster will enter the land between normal and taking snapshot.
// This state will continue even this function returns, until `Finalize` invoked.
// Splitting, ingesting and conf changing will all be blocked.
func (p *Preparer) DriveLoopAndWaitPrepare(ctx context.Context) error {
	logutil.CL(ctx).Info("Start drive the loop.", zap.Duration("retry_backoff", p.RetryBackoff),
		zap.Int("retry_limit", p.RetryLimit),
		zap.Duration("lease_duration", p.LeaseDuration))
	p.retryTime = 0
	if err := p.PrepareConnections(ctx); err != nil {
		log.Error("failed to prepare connections", logutil.ShortError(err))
		return errors.Annotate(err, "failed to prepare connections")
	}
	if p.AfterConnectionsEstablished != nil {
		p.AfterConnectionsEstablished()
	}
	if err := p.AdvanceState(ctx); err != nil {
		log.Error("failed to check the progress of our work", logutil.ShortError(err))
		return errors.Annotate(err, "failed to begin step")
	}
	for !p.waitApplyFinished {
		if err := p.WaitAndHandleNextEvent(ctx); err != nil {
			log.Error("failed to wait and handle next event", logutil.ShortError(err))
			return errors.Annotate(err, "failed to step")
		}
	}
	return nil
}

// Finalize notify the cluster to go back to the normal mode.
// This will return an error if the cluster has already entered the normal mode when this is called.
func (p *Preparer) Finalize(ctx context.Context) error {
	eg := new(errgroup.Group)
	for id, cli := range p.clients {
		cli := cli
		id := id
		eg.Go(func() error {
			if err := cli.Finalize(ctx); err != nil {
				return errors.Annotatef(err, "failed to finalize the prepare stream for %d", id)
			}
			return nil
		})
	}
	errCh := make(chan error, 1)
	go func() {
		if err := eg.Wait(); err != nil {
			logutil.CL(ctx).Warn("failed to finalize some prepare streams.", logutil.ShortError(err))
			errCh <- err
			return
		}
		logutil.CL(ctx).Info("all connections to store have shuted down.")
		errCh <- nil
	}()
	for {
		select {
		case event, ok := <-p.eventChan:
			if !ok {
				return nil
			}
			if err := p.onEvent(ctx, event); err != nil {
				return err
			}
		case err, ok := <-errCh:
			if !ok {
				panic("unreachable.")
			}
			if err != nil {
				return err
			}
			// All streams are finialized, they shouldn't send more events to event chan.
			close(p.eventChan)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (p *Preparer) batchEvents(evts *[]event) {
	for {
		select {
		case evt := <-p.eventChan:
			*evts = append(*evts, evt)
		default:
			return
		}
	}
}

// WaitAndHandleNextEvent is exported for test usage.
// This waits the next event (wait apply done, errors, etc..) of preparing.
// Generally `DriveLoopAndWaitPrepare` is all you need.
func (p *Preparer) WaitAndHandleNextEvent(ctx context.Context) error {
	select {
	case <-ctx.Done():
		logutil.CL(ctx).Warn("User canceled.", logutil.ShortError(ctx.Err()))
		return ctx.Err()
	case evt := <-p.eventChan:
		logutil.CL(ctx).Debug("received event", zap.Stringer("event", evt))
		events := []event{evt}
		p.batchEvents(&events)
		for _, evt := range events {
			err := p.onEvent(ctx, evt)
			if err != nil {
				return errors.Annotatef(err, "failed to handle event %v", evt)
			}
		}
		return p.AdvanceState(ctx)
	case <-p.retryChan():
		return p.workOnPendingRanges(ctx)
	}
}

func (p *Preparer) removePendingRequest(r *metapb.Region) bool {
	r2, ok := p.inflightReqs[r.GetId()]
	if !ok {
		return false
	}
	matches := r2.GetRegionEpoch().GetVersion() == r.GetRegionEpoch().GetVersion() &&
		r2.GetRegionEpoch().GetConfVer() == r.GetRegionEpoch().GetConfVer()
	if !matches {
		return false
	}
	delete(p.inflightReqs, r.GetId())
	return true
}

func (p *Preparer) onEvent(ctx context.Context, e event) error {
	switch e.ty {
	case eventMiscErr:
		// Note: some of errors might be able to be retry.
		// But for now it seems there isn't one.
		return errors.Annotatef(e.err, "unrecoverable error at store %d", e.storeID)
	case eventWaitApplyDone:
		if !p.removePendingRequest(e.region) {
			logutil.CL(ctx).Warn("received unmatched response, perhaps stale, drop it", zap.Stringer("region", e.region))
			return nil
		}
		r := rangeOrRegion{
			id:       e.region.GetId(),
			startKey: e.region.GetStartKey(),
			endKey:   e.region.GetEndKey(),
		}
		if e.err != nil {
			logutil.CL(ctx).Warn("requesting a region failed.", zap.Uint64("store", e.storeID), logutil.ShortError(e.err))
			p.failed = append(p.failed, r)
			if p.nextRetry != nil {
				p.nextRetry.Stop()
			}
			// Reset the timer so we can collect more regions.
			// Note: perhaps it is better to make a deadline heap or something
			// so every region backoffs the same time.
			p.nextRetry = time.NewTimer(p.RetryBackoff)
			return nil
		}
		if item, ok := p.waitApplyDoneRegions.ReplaceOrInsert(r); ok {
			logutil.CL(ctx).Warn("overlapping in success region",
				zap.Stringer("old_region", item),
				zap.Stringer("new_region", r))
		}
	default:
		return errors.Annotatef(unsupported(), "unsupported event type %d", e.ty)
	}

	return nil
}

func (p *Preparer) retryChan() <-chan time.Time {
	if p.nextRetry == nil {
		return nil
	}
	return p.nextRetry.C
}

// AdvanceState is exported for test usage.
// This call will check whether now we are safe to forward the whole procedure.
// If we can, this will set `p.waitApplyFinished` to true.
// Generally `DriveLoopAndWaitPrepare` is all you need, you may not want to call this.
func (p *Preparer) AdvanceState(ctx context.Context) error {
	logutil.CL(ctx).Info("Checking the progress of our work.", zap.Object("current", p))
	if len(p.inflightReqs) == 0 && len(p.failed) == 0 {
		holes := p.checkHole()
		if len(holes) == 0 {
			p.waitApplyFinished = true
			return nil
		}
		logutil.CL(ctx).Warn("It seems there are still some works to be done.", zap.Stringers("regions", holes))
		p.failed = holes
		return p.workOnPendingRanges(ctx)
	}

	return nil
}

func (p *Preparer) checkHole() []rangeOrRegion {
	log.Info("Start checking the hole.", zap.Int("len", p.waitApplyDoneRegions.Len()))
	if p.waitApplyDoneRegions.Len() == 0 {
		return []rangeOrRegion{{}}
	}

	last := []byte("")
	failed := []rangeOrRegion{}
	p.waitApplyDoneRegions.Ascend(func(item rangeOrRegion) bool {
		if bytes.Compare(last, item.startKey) < 0 {
			failed = append(failed, rangeOrRegion{startKey: last, endKey: item.startKey})
		}
		last = item.endKey
		return true
	})
	// Not the end key of key space.
	if len(last) > 0 {
		failed = append(failed, rangeOrRegion{
			startKey: last,
		})
	}
	return failed
}

func (p *Preparer) workOnPendingRanges(ctx context.Context) error {
	p.nextRetry = nil
	if len(p.failed) == 0 {
		return nil
	}
	p.retryTime += 1
	if p.retryTime > p.RetryLimit {
		return retryLimitExceeded()
	}

	logutil.CL(ctx).Info("retrying some ranges incomplete.", zap.Int("ranges", len(p.failed)))
	preqs := pendingRequests{}
	for _, r := range p.failed {
		rs, err := p.env.LoadRegionsInKeyRange(ctx, r.startKey, r.endKey)
		if err != nil {
			return errors.Annotatef(err, "retrying range of %s: get region", logutil.StringifyRangeOf(r.startKey, r.endKey))
		}
		logutil.CL(ctx).Info("loaded regions in range for retry.", zap.Int("regions", len(rs)))
		for _, region := range rs {
			p.pushWaitApply(preqs, region)
		}
	}
	p.failed = nil
	return p.sendWaitApply(ctx, preqs)
}

func (p *Preparer) sendWaitApply(ctx context.Context, reqs pendingRequests) error {
	logutil.CL(ctx).Info("about to send wait apply to stores", zap.Int("to-stores", len(reqs)))
	for store, req := range reqs {
		logutil.CL(ctx).Info("sending wait apply requests to store", zap.Uint64("store", store), zap.Int("regions", len(req.Regions)))
		stream, err := p.streamOf(ctx, store)
		if err != nil {
			return errors.Annotatef(err, "failed to dial the store %d", store)
		}
		err = stream.cli.Send(req)
		if err != nil {
			return errors.Annotatef(err, "failed to send message to the store %d", store)
		}
	}
	return nil
}

func (p *Preparer) streamOf(ctx context.Context, storeID uint64) (*prepareStream, error) {
	_, ok := p.clients[storeID]
	if !ok {
		log.Warn("stream of store found a store not established connection", zap.Uint64("store", storeID))
		cli, err := p.env.ConnectToStore(ctx, storeID)
		if err != nil {
			return nil, errors.Annotatef(err, "failed to dial store %d", storeID)
		}
		if err := p.createAndCacheStream(ctx, cli, storeID); err != nil {
			return nil, errors.Annotatef(err, "failed to create and cache stream for store %d", storeID)
		}
	}
	return p.clients[storeID], nil
}

func (p *Preparer) createAndCacheStream(ctx context.Context, cli PrepareClient, storeID uint64) error {
	if _, ok := p.clients[storeID]; ok {
		return nil
	}

	s := new(prepareStream)
	s.storeID = storeID
	s.output = p.eventChan
	s.leaseDuration = p.LeaseDuration
	err := s.InitConn(ctx, cli)
	if err != nil {
		return err
	}
	p.clients[storeID] = s
	return nil
}

func (p *Preparer) pushWaitApply(reqs pendingRequests, region Region) {
	leader := region.GetLeaderStoreID()
	if _, ok := reqs[leader]; !ok {
		reqs[leader] = new(brpb.PrepareSnapshotBackupRequest)
		reqs[leader].Ty = brpb.PrepareSnapshotBackupRequestType_WaitApply
	}
	reqs[leader].Regions = append(reqs[leader].Regions, region.GetMeta())
	p.inflightReqs[region.GetMeta().Id] = *region.GetMeta()
}

// PrepareConnections prepares the connections for each store.
// This will pause the admin commands for each store.
func (p *Preparer) PrepareConnections(ctx context.Context) error {
	failpoint.Inject("PrepareConnectionsErr", func() {
		failpoint.Return(errors.New("mock PrepareConnectionsErr"))
	})
	log.Info("Preparing connections to stores.")
	stores, err := p.env.GetAllLiveStores(ctx)
	if err != nil {
		return errors.Annotate(err, "failed to get all live stores")
	}

	log.Info("Start to initialize the connections.", zap.Int("stores", len(stores)))
	clients := map[uint64]PrepareClient{}
	for _, store := range stores {
		cli, err := p.env.ConnectToStore(ctx, store.Id)
		if err != nil {
			return errors.Annotatef(err, "failed to dial the store %d", store.Id)
		}
		clients[store.Id] = cli
	}

	for id, cli := range clients {
		log.Info("Start to pause the admin commands.", zap.Uint64("store", id))
		if err := p.createAndCacheStream(ctx, cli, id); err != nil {
			return errors.Annotatef(err, "failed to create and cache stream for store %d", id)
		}
	}

	return nil
}
