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

	output chan<- event

	bgTasks     *errgroup.Group
	stopBgTasks context.CancelFunc
}

// InitConn initializes the connection to the stream (i.e. "active" the stream).
//
// Before calling this, make sure you have filled the store ID and output channel.
//
// Once this has been called, them **should not be changed** any more.
func (p *prepareStream) InitConn(ctx context.Context, cli PrepareClient) error {
	p.cli = cli
	p.bgTasks, ctx = errgroup.WithContext(ctx)
	ctx, p.stopBgTasks = context.WithCancel(ctx)
	p.GoLeaseLoop(ctx, p.leaseDuration)
	p.bgTasks.Go(func() error { return p.RecvLoop(ctx) })
	return nil
}

func (p *prepareStream) Finalize(ctx context.Context) error {
	log.Info("shutting down", zap.Uint64("store", p.storeID))
	p.stopBgTasks()
	return p.bgTasks.Wait()
}

func (p *prepareStream) GoLeaseLoop(ctx context.Context, dur time.Duration) error {
	p.cli.Send(&brpb.PrepareSnapshotBackupRequest{
		Ty:             brpb.PrepareSnapshotBackupRequestType_UpdateLease,
		LeaseInSeconds: uint64(dur.Seconds()),
	})
	msg, err := p.cli.Recv()
	if err != nil {
		return errors.Annotate(err, "failed to initialize the lease for suspending the backup")
	}
	if msg.Ty != brpb.PrepareSnapshotBackupEventType_UpdateLeaseResult {
		return errors.New("unexpected type of response during creating lease loop")
	}
	p.bgTasks.Go(func() error { return p.LeaseLoop(ctx, dur) })
	return nil
}

func (p *prepareStream) LeaseLoop(ctx context.Context, dur time.Duration) error {
	ticker := time.NewTicker(dur / 4)
	lastSuccess := time.Unix(0, 0)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Info("lease loop exits.", zap.Uint64("store", p.storeID))
			return p.cli.Send(&brpb.PrepareSnapshotBackupRequest{
				Ty: brpb.PrepareSnapshotBackupRequestType_Finish,
			})
		case <-ticker.C:
			err := p.cli.Send(&brpb.PrepareSnapshotBackupRequest{
				Ty:             brpb.PrepareSnapshotBackupRequestType_UpdateLease,
				LeaseInSeconds: uint64(dur.Seconds()),
			})
			if err != nil {
				log.Warn("failed to update the lease loop", logutil.ShortError(err))
				err := errors.Annotate(err, "too many times failed to update the lease, it is probably expired")
				if time.Since(lastSuccess) > dur {
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

func (p *prepareStream) RecvLoop(ctx context.Context) error {
	for {
		resp, err := p.cli.Recv()
		if err != nil {
			if err == io.EOF {
				logutil.CL(ctx).Info("RecvLoop stopped.", zap.Uint64("store", p.storeID))
				return nil
			}
			p.sendErr(errors.Annotate(err, "failed to recv from the stream"))
			return err
		}
		logutil.CL(ctx).Debug("received response", zap.Stringer("resp", resp))
		evt, ok := p.convertToEvent(resp)
		if ok {
			logutil.CL(ctx).Debug("generating internal event", zap.Stringer("event", evt))
			p.output <- evt
		}
	}
}

func (p *prepareStream) convertToEvent(resp *brpb.PrepareSnapshotBackupResponse) (event, bool) {
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
				err:     errLeaseExpired(),
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
