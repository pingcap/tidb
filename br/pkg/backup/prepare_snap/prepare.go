package preparesnap

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/google/btree"
	"github.com/pingcap/errors"
	brpb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

const (
	/* The combination of defaultMaxRetry and defaultRetryBackoff limits
	   the whole procedure to about 5 min if there is a region always fail.
	   Also note that we are batching during retrying. Retrying many region
	   costs only one chance of retrying if they are batched. */

	defaultMaxRetry     = 60
	defaultRetryBackoff = 5 * time.Second
	defaultLeaseDur     = 300 * time.Second

	/* Give pd enough time to find the region. If we aren't able to fetch
	   the region, the whole procedure might be aborted. */

	regionCacheMaxBackoffMs = 60000
)

type pendingRequests map[uint64]*brpb.PrepareSnapshotBackupRequest

type region struct {
	maybeID  uint64
	startKey []byte
	endKey   []byte
}

func (r region) String() string {
	rng := logutil.StringifyRangeOf(r.startKey, r.endKey)
	if r.maybeID == 0 {
		return fmt.Sprintf("range%s", rng)
	}
	return fmt.Sprintf("region(id=%d, range=%s)", r.maybeID, rng)
}

func (r region) Less(than btree.Item) bool {
	return bytes.Compare(r.startKey, than.(region).startKey) < 0
}

type Preparer struct {
	/* Environments. */
	env Env

	/* Internal Status. */
	inflightReqs         map[uint64]metapb.Region
	failedRegions        []region
	waitApplyDoneRegions btree.BTree
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
}

func New(env Env) *Preparer {
	prep := &Preparer{
		env: env,

		inflightReqs:         make(map[uint64]metapb.Region),
		waitApplyDoneRegions: *btree.New(16),
		eventChan:            make(chan event, 128),
		clients:              make(map[uint64]*prepareStream),

		RetryBackoff:  defaultRetryBackoff,
		RetryLimit:    defaultMaxRetry,
		LeaseDuration: defaultLeaseDur,
	}
	return prep
}

// DriveLoopAndWaitPrepare drives the state machine and block the
// current goroutine until we are safe to start taking snapshot.
//
// After this invoked, you shouldn't share this `Preparer“ with any other goroutine.
//
// After this involed, the cluster will enter the land between normal and taking snapshot.
// Spliting, ingesting and conf changing may all be blocked until `Finalize` invoked.
func (p *Preparer) DriveLoopAndWaitPrepare(ctx context.Context) error {
	p.retryTime = 0
	if err := p.MaybeFinish(ctx); err != nil {
		return errors.Annotate(err, "failed to begin step")
	}
	for !p.waitApplyFinished {
		if err := p.WaitAndHandleNextEvent(ctx); err != nil {
			return errors.Annotate(err, "failed to step")
		}
	}
	return nil
}

// Finalize notify the cluster to go back to the normal mode.
// This will return an error if the cluster has already entered the normal mode when this is called.
func (p *Preparer) Finalize(ctx context.Context) error {
	for id, cli := range p.clients {
		if err := cli.Finalize(ctx); err != nil {
			return errors.Annotatef(err, "failed to finalize the prepare stream for %d", id)
		}
	}
	logutil.CL(ctx).Info("all connections to store have shuted down.")
	for {
		select {
		case event := <-p.eventChan:
			if err := p.onEvent(ctx, event); err != nil {
				return err
			}
		default:
			return nil
		}
	}
}

// WaitAndHandleNextEvent is exported for test usage.
// This waits the next event (wait apply done, errors, etc..) of preparing.
// Generally `DriveLoopAndWaitPrepare` is all you need.
func (p *Preparer) WaitAndHandleNextEvent(ctx context.Context) error {
	select {
	case <-ctx.Done():
		logutil.CL(ctx).Warn("User canceled, try to stop the whole procedure gracefully.")
		p.Finalize(context.Background())
		return ctx.Err()
	case evt := <-p.eventChan:
		logutil.CL(ctx).Debug("received event", zap.Stringer("event", evt))
		err := p.onEvent(ctx, evt)
		if err != nil {
			return errors.Annotatef(err, "failed to handle event %v", evt)
		}
		return p.MaybeFinish(ctx)
	case <-p.retryC():
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
		r := region{
			maybeID:  e.region.GetId(),
			startKey: e.region.GetStartKey(),
			endKey:   e.region.GetEndKey(),
		}
		if e.err != nil {
			logutil.CL(ctx).Warn("requesting a region failed.", zap.Uint64("store", e.storeID), logutil.ShortError(e.err))
			p.failedRegions = append(p.failedRegions, r)
			if p.nextRetry != nil {
				p.nextRetry.Stop()
			}
			p.nextRetry = time.NewTimer(p.RetryBackoff)
			return nil
		}
		if item := p.waitApplyDoneRegions.ReplaceOrInsert(r); item != nil {
			logutil.CL(ctx).Warn("overlapping in success region",
				zap.Stringer("old_region", item.(region)),
				zap.Stringer("new_region", r))
		}
		logutil.CL(ctx).Info("wait apply for a region done.", zap.Stringer("region", e.region))
	default:
		return errors.Annotatef(unsupported(), "unsupported event type %d", e.ty)
	}

	return nil
}

func (p *Preparer) retryC() <-chan time.Time {
	if p.nextRetry == nil {
		return nil
	}
	return p.nextRetry.C
}

// MaybeFinish is exported for test usage.
// This call will check whether now we are safe to forward the whole procedure.
// If we can, this will set `p.waitApplyFinished` to true.
// Generally `DriveLoopAndWaitPrepare` is all you need, you may not want to call this.
func (p *Preparer) MaybeFinish(ctx context.Context) error {
	logutil.CL(ctx).Info("Checking the progress of our work.",
		zap.Int("inflight_reqs", len(p.inflightReqs)), zap.Int("failed_ranges", len(p.failedRegions)))
	if len(p.inflightReqs) == 0 && len(p.failedRegions) == 0 {
		holes := p.checkHole()
		if len(holes) == 0 {
			p.waitApplyFinished = true
			return nil
		} else {
			logutil.CL(ctx).Warn("It seems there are still some works to be done.", zap.Stringers("regions", holes))
			p.failedRegions = holes
			return p.workOnPendingRanges(ctx)
		}
	}

	return nil
}

func (p *Preparer) checkHole() []region {
	if p.waitApplyDoneRegions.Len() == 0 {
		return []region{{}}
	}

	last := []byte("")
	failed := []region{}
	p.waitApplyDoneRegions.Ascend(func(item btree.Item) bool {
		i := item.(region)
		if bytes.Compare(last, i.startKey) < 0 {
			failed = append(failed, region{startKey: last, endKey: i.startKey})
		}
		last = i.endKey
		return true
	})
	// Not the end key of key space.
	if len(last) > 0 {
		failed = append(failed, region{
			startKey: last,
		})
	}
	return failed
}

func (p *Preparer) workOnPendingRanges(ctx context.Context) error {
	p.nextRetry = nil
	if len(p.failedRegions) == 0 {
		return nil
	}
	p.retryTime += 1
	if p.retryTime > p.RetryLimit {
		return retryLimitExceeded()
	}

	logutil.CL(ctx).Info("retrying some ranges incomplete.", zap.Int("ranges", len(p.failedRegions)))
	bo := tikv.NewBackoffer(ctx, regionCacheMaxBackoffMs)
	preqs := pendingRequests{}
	for _, r := range p.failedRegions {
		rs, err := p.env.LoadRegionsInKeyRange(bo, r.startKey, r.endKey)
		if err != nil {
			return errors.Annotatef(err, "retrying range of %s: get region", logutil.StringifyRangeOf(r.startKey, r.endKey))
		}
		logutil.CL(ctx).Info("loaded regions in range for retry.", zap.Int("regions", len(rs)))
		for _, region := range rs {
			p.pushWaitApply(preqs, region)
		}
	}
	p.failedRegions = nil
	return p.sendWaitApply(ctx, preqs)
}

func (p *Preparer) sendWaitApply(ctx context.Context, reqs pendingRequests) error {
	for store, req := range reqs {
		stream, err := p.streamOf(ctx, store)
		if err != nil {
			return errors.Annotatef(err, "failed to dial the store %d", store)
		}
		err = stream.cli.Send(req)
		if err != nil {
			return errors.Annotatef(err, "failed to send message to the store %d", store)
		}
		logutil.CL(ctx).Info("sent wait apply requests to store", zap.Uint64("store", store), zap.Int("regions", len(req.Regions)))
	}
	return nil
}

func (p *Preparer) streamOf(ctx context.Context, storeID uint64) (*prepareStream, error) {
	s, ok := p.clients[storeID]
	if !ok {
		cli, err := p.env.ConnectToStore(ctx, storeID)
		if err != nil {
			return nil, errors.Annotatef(err, "failed to dial store %d", storeID)
		}
		s = new(prepareStream)
		s.storeID = storeID
		s.output = p.eventChan
		s.leaseDuration = p.LeaseDuration
		err = s.InitConn(ctx, cli)
		if err != nil {
			return nil, err
		}
		p.clients[storeID] = s
	}
	return s, nil
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
