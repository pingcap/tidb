package streamhelper

import (
	"context"
	"io"
	"strconv"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	logbackup "github.com/pingcap/kvproto/pkg/logbackuppb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/streamhelper/spans"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/util/codec"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type FlushSubscriber struct {
	dialer  LogBackupService
	cluster TiKVClusterMeta

	clients      map[uint64]*subscription
	eventsTunnel chan spans.Valued
	masterCtx    context.Context
}

type SubscriberConfig func(*FlushSubscriber)

func WithMasterContext(ctx context.Context) SubscriberConfig {
	return func(fs *FlushSubscriber) { fs.masterCtx = ctx }
}

func NewSubscriber(dialer LogBackupService, cluster TiKVClusterMeta, config ...SubscriberConfig) *FlushSubscriber {
	subs := &FlushSubscriber{
		dialer:  dialer,
		cluster: cluster,

		clients:      map[uint64]*subscription{},
		eventsTunnel: make(chan spans.Valued, 1024),
		masterCtx:    context.Background(),
	}

	for _, c := range config {
		c(subs)
	}

	return subs
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
			f.addSubscription(ctx, store.ID)
			f.clients[store.ID].connect(f.masterCtx, f.dialer)
		}
		storeSet[store.ID] = struct{}{}
	}

	for id := range f.clients {
		_, ok := storeSet[id]
		if !ok {
			f.removeSubscription(id)
		}
	}
	return nil
}

// Clear clears all the subscriptions.
func (f *FlushSubscriber) Clear() {
	log.Info("[log backup flush subscriber] Clearing.")
	for id := range f.clients {
		f.removeSubscription(id)
	}
}

// Drop terminates the lifetime of the subscriber.
// This subscriber would be no more usable.
func (f *FlushSubscriber) Drop() {
	f.Clear()
	close(f.eventsTunnel)
}

// HandleErrors handles all pending errors.
// Return them if there are some error cannot be handle internally.
func (f *FlushSubscriber) HandleErrors(ctx context.Context) error {
	for id, sub := range f.clients {
		err := sub.loadError()
		if err != nil {
			retry := f.canBeRetried(err)
			log.Warn("[log backup flush subscriber] Meet error.", logutil.ShortError(err), zap.Bool("can-retry?", retry), zap.Uint64("store", id))
			if retry {
				sub.connect(f.masterCtx, f.dialer)
			}
		}
	}
	return nil
}

func (f *FlushSubscriber) Events() <-chan spans.Valued {
	return f.eventsTunnel
}

type eventStream = logbackup.LogBackup_SubscribeFlushEventClient

type subscription struct {
	cancel context.CancelFunc
	err    atomic.Value

	// Immutable state.
	storeID uint64
	output  chan<- spans.Valued
}

func (s *subscription) emitError(err error) {
	s.err.Store(err)
}

func (s *subscription) loadError() error {
	err, ok := s.err.Load().(error)
	if !ok {
		return nil
	}
	return err
}

func newSubscription(toStore uint64, output chan<- spans.Valued) *subscription {
	return &subscription{
		storeID: toStore,
		output:  output,
	}
}

func (s *subscription) connect(ctx context.Context, dialer LogBackupService) error {
	log.Info("[log backup subscription manager] Adding subscription.", zap.Uint64("store", s.storeID))

	c, err := dialer.GetLogBackupClient(ctx, s.storeID)
	if err != nil {
		return err
	}
	cx, cancel := context.WithCancel(ctx)
	cli, err := c.SubscribeFlushEvent(cx, &logbackup.SubscribeFlushEventRequest{
		ClientId: uuid.NewString(),
	})
	if err != nil {
		cancel()
		return err
	}
	s.cancel = cancel
	go s.listenOver(cli)
	return nil
}

func (s *subscription) close() {
	if s.cancel != nil {
		s.cancel()
	}
	// HACK: don't close the internal channel here,
	// because it is a ever-sharing channel.
}

func (s *subscription) listenOver(cli eventStream) {
	storeID := s.storeID
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
			s.emitError(errors.Annotatef(err, "while receiving from store id %d", storeID))
			return
		}

		for _, m := range msg.Events {
			s.output <- spans.Valued{
				Key: spans.Span{
					StartKey: decodeKey(m.StartKey),
					EndKey:   decodeKey(m.EndKey),
				},
				Value: m.Checkpoint,
			}
		}
		metrics.RegionCheckpointSubscriptionEvent.WithLabelValues(strconv.Itoa(int(storeID))).Add(float64(len(msg.Events)))
	}
}

func (f *FlushSubscriber) addSubscription(ctx context.Context, toStore uint64) {
	f.clients[toStore] = newSubscription(toStore, f.eventsTunnel)
}

func (f *FlushSubscriber) removeSubscription(toStore uint64) {
	subs, ok := f.clients[toStore]
	if ok {
		log.Info("[log backup subscription manager] Removing subscription.", zap.Uint64("store", toStore))
		subs.close()
		delete(f.clients, toStore)
	}
}

// decodeKey decodes the key from TiKV, because the region range is encoded in TiKV.
func decodeKey(key []byte) []byte {
	if len(key) == 0 {
		return key
	}
	// Ignore the timestamp...
	_, data, err := codec.DecodeBytes(key, nil)
	if err != nil {
		log.Warn("the key from TiKV isn't encoded properly", logutil.Key("key", key), logutil.ShortError(err))
	}
	return data
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
