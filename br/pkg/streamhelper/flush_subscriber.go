package streamhelper

import (
	"context"
	"io"
	"strconv"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	logbackup "github.com/pingcap/kvproto/pkg/logbackuppb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/streamhelper/spans"
	"github.com/pingcap/tidb/metrics"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type FlushSubscriber struct {
	dialer  LogBackupService
	cluster TiKVClusterMeta

	clients      map[uint64]subscription
	eventsTunnel chan spans.Valued
	errorsTunnel chan storeError
	masterCtx    context.Context
}

type storeError struct {
	id uint64
	error
}

type eventStream = logbackup.LogBackup_SubscribeFlushEventClient

type subscription struct {
	cancel context.CancelFunc
}

type SubscriberConfig func(*FlushSubscriber)

func WithMasterContext(ctx context.Context) SubscriberConfig {
	return func(fs *FlushSubscriber) { fs.masterCtx = ctx }
}

func NewSubscriber(dialer LogBackupService, cluster TiKVClusterMeta, config ...SubscriberConfig) *FlushSubscriber {
	subs := &FlushSubscriber{
		dialer:  dialer,
		cluster: cluster,

		clients:      map[uint64]subscription{},
		eventsTunnel: make(chan spans.Valued, 1024),
		errorsTunnel: make(chan storeError, 128),
		masterCtx:    context.Background(),
	}

	for _, c := range config {
		c(subs)
	}

	return subs
}

func (f *FlushSubscriber) AddSubscription(ctx context.Context, toStore uint64) error {
	log.Info("[log backup subscription manager] Adding subscription.", zap.Uint64("store", toStore))

	f.RemoveSubscription(toStore)
	c, err := f.dialer.GetLogBackupClient(ctx, toStore)
	if err != nil {
		return err
	}
	cx, cancel := context.WithCancel(f.masterCtx)
	cli, err := c.SubscribeFlushEvent(cx, &logbackup.SubscribeFlushEventRequest{
		ClientId: uuid.NewString(),
	})
	if err != nil {
		cancel()
		return err
	}
	go f.listenOver(toStore, cli)
	sub := subscription{
		cancel: cancel,
	}
	f.clients[toStore] = sub
	return nil
}

func (f *FlushSubscriber) RemoveSubscription(toStore uint64) {
	subs, ok := f.clients[toStore]
	if ok {
		log.Info("[log backup subscription manager] Removing subscription.", zap.Uint64("store", toStore))
		subs.cancel()
		delete(f.clients, toStore)
	}
}

func (f *FlushSubscriber) UpdateStoreTopology(ctx context.Context) error {
	stores, err := f.cluster.Stores(ctx)
	if err != nil {
		return errors.Annotate(err, "failed to get store list")
	}

	storeSet := map[uint64]struct{}{}
	for _, store := range stores {
		_, ok := f.clients[store.ID]
		if !ok {
			f.AddSubscription(ctx, store.ID)
		}
		storeSet[store.ID] = struct{}{}
	}

	for id := range f.clients {
		_, ok := storeSet[id]
		if !ok {
			f.RemoveSubscription(id)
		}
	}
	return nil
}

// Clear clears all the subscriptions,
func (f *FlushSubscriber) Clear() {
	log.Info("[log backup flush subscriber] Clearing.")
	for id := range f.clients {
		f.RemoveSubscription(id)
	}
}

// HandleErrors handles all pending errors.
// Return them if there are some error cannot be handle internally.
func (f *FlushSubscriber) HandleErrors(ctx context.Context) error {
	es := map[uint64]error{}

collect:
	for {
		select {
		case err := <-f.errorsTunnel:
			es[err.id] = multierr.Append(es[err.id], err)
		default:
			break collect
		}
	}

	for id, err := range es {
		retry := f.canBeRetried(err)
		log.Warn("[log backup flush subscriber] Meet error.", logutil.ShortError(err), zap.Bool("can-retry?", retry))

		if retry {
			f.AddSubscription(ctx, id)
		}
	}

	return nil
}

func (f *FlushSubscriber) Events() <-chan spans.Valued {
	return f.eventsTunnel
}

func (f *FlushSubscriber) listenOver(storeID uint64, cli eventStream) {
	log.Info("[log backup flush subscriber] Listen starting.", zap.Uint64("store", storeID))
	for {
		// Shall we use RecvMsg for better performance?
		// Note that the spans.Full requires the input slice be immutable.
		msg, err := cli.Recv()
		if err != nil {
			log.Info("[log backup flush subscriber] Listen stopped.", zap.Uint64("store", storeID), logutil.ShortError(err))
			if err == io.EOF || err == context.Canceled {
				return
			}
			f.onError(storeID, errors.Annotatef(err, "while receiving from store id %d", storeID))
			return
		}

		for _, m := range msg.Events {
			f.eventsTunnel <- spans.Valued{
				Key: spans.Span{
					StartKey: m.StartKey,
					EndKey:   m.EndKey,
				},
				Value: m.Checkpoint,
			}
		}
		metrics.RegionCheckpointSubscriptionEvent.WithLabelValues(strconv.Itoa(int(storeID))).Add(float64(len(msg.Events)))
	}
}

func (f *FlushSubscriber) onError(byStore uint64, err error) {
	f.errorsTunnel <- storeError{error: err, id: byStore}
}

func (f *FlushSubscriber) canBeRetried(err error) bool {
	for _, e := range multierr.Errors(err) {
		s := status.Convert(e)
		// Is there any other error cannot be retried?
		if s.Code() == codes.Unimplemented {
			return false
		}
	}
	return true
}
