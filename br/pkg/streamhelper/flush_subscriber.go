// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package streamhelper

import (
	"context"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	logbackup "github.com/pingcap/kvproto/pkg/logbackuppb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/streamhelper/spans"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/util/codec"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// clearSubscriberTimeOut is the timeout for clearing the subscriber.
	clearSubscriberTimeOut = 1 * time.Minute
)

// FlushSubscriber maintains the state of subscribing to the cluster.
type FlushSubscriber struct {
	dialer  LogBackupService
	cluster TiKVClusterMeta

	// Current connections.
	subscriptions map[uint64]*subscription
	// The output channel.
	eventsTunnel chan spans.Valued
	// The background context for subscribes.
	masterCtx context.Context
}

// SubscriberConfig is a config which cloud be applied into the subscriber.
type SubscriberConfig func(*FlushSubscriber)

// WithMasterContext sets the "master context" for the subscriber,
// that context would be the "background" context for every subtasks created by the subscription manager.
func WithMasterContext(ctx context.Context) SubscriberConfig {
	return func(fs *FlushSubscriber) { fs.masterCtx = ctx }
}

// NewSubscriber creates a new subscriber via the environment and optional configs.
func NewSubscriber(dialer LogBackupService, cluster TiKVClusterMeta, config ...SubscriberConfig) *FlushSubscriber {
	subs := &FlushSubscriber{
		dialer:  dialer,
		cluster: cluster,

		subscriptions: map[uint64]*subscription{},
		eventsTunnel:  make(chan spans.Valued, 1024),
		masterCtx:     context.Background(),
	}

	for _, c := range config {
		c(subs)
	}

	return subs
}

// UpdateStoreTopology fetches the current store topology and try to adapt the subscription state with it.
func (f *FlushSubscriber) UpdateStoreTopology(ctx context.Context) error {
	stores, err := f.cluster.Stores(ctx)
	if err != nil {
		return errors.Annotate(err, "failed to get store list")
	}

	storeSet := map[uint64]struct{}{}
	for _, store := range stores {
		sub, ok := f.subscriptions[store.ID]
		if !ok {
			f.addSubscription(ctx, store)
			f.subscriptions[store.ID].connect(f.masterCtx, f.dialer)
		} else if sub.storeBootAt != store.BootAt {
			sub.storeBootAt = store.BootAt
			sub.connect(f.masterCtx, f.dialer)
		}
		storeSet[store.ID] = struct{}{}
	}

	for id := range f.subscriptions {
		_, ok := storeSet[id]
		if !ok {
			f.removeSubscription(ctx, id)
		}
	}
	return nil
}

// Clear clears all the subscriptions.
func (f *FlushSubscriber) Clear() {
	timeout := clearSubscriberTimeOut
	failpoint.Inject("FlushSubscriber.Clear.timeoutMs", func(v failpoint.Value) {
		//nolint:durationcheck
		timeout = time.Duration(v.(int)) * time.Millisecond
	})
	log.Info("Clearing.",
		zap.String("category", "log backup flush subscriber"),
		zap.Duration("timeout", timeout))
	cx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for id := range f.subscriptions {
		f.removeSubscription(cx, id)
	}
}

// Drop terminates the lifetime of the subscriber.
// This subscriber would be no more usable.
func (f *FlushSubscriber) Drop() {
	f.Clear()
	close(f.eventsTunnel)
}

// HandleErrors execute the handlers over all pending errors.
// Note that the handler may cannot handle the pending errors, at that time,
// you can fetch the errors via `PendingErrors` call.
func (f *FlushSubscriber) HandleErrors(ctx context.Context) {
	for id, sub := range f.subscriptions {
		err := sub.loadError()
		if err != nil {
			retry := f.canBeRetried(err)
			log.Warn("Meet error.", zap.String("category", "log backup flush subscriber"),
				logutil.ShortError(err), zap.Bool("can-retry?", retry), zap.Uint64("store", id))
			if retry {
				sub.connect(f.masterCtx, f.dialer)
			}
		}
	}
}

// Events returns the output channel of the events.
func (f *FlushSubscriber) Events() <-chan spans.Valued {
	return f.eventsTunnel
}

type eventStream = logbackup.LogBackup_SubscribeFlushEventClient

type joinHandle <-chan struct{}

func (jh joinHandle) Wait(ctx context.Context) {
	select {
	case <-jh:
	case <-ctx.Done():
		log.Warn("join handle timed out.", zap.StackSkip("caller", 1))
	}
}

func spawnJoinable(f func()) joinHandle {
	c := make(chan struct{})
	go func() {
		defer close(c)
		f()
	}()
	return c
}

// subscription is the state of subscription of one store.
// initially, it is IDLE, where cancel == nil.
// once `connect` called, it goto CONNECTED, where cancel != nil and err == nil.
// once some error (both foreground or background) happens, it goto ERROR, where err != nil.
type subscription struct {
	// the handle to cancel the worker goroutine.
	cancel context.CancelFunc
	// the handle to wait until the worker goroutine exits.
	background joinHandle
	errMu      sync.Mutex
	err        error

	// Immutable state.
	storeID uint64
	// We record start bootstrap time and once a store restarts
	// we need to try reconnect even there is a error cannot be retry.
	storeBootAt uint64
	output      chan<- spans.Valued

	onDaemonExit func()
}

func (s *subscription) emitError(err error) {
	s.errMu.Lock()
	defer s.errMu.Unlock()

	s.err = err
}

func (s *subscription) loadError() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()

	return s.err
}

func (s *subscription) clearError() {
	s.errMu.Lock()
	defer s.errMu.Unlock()

	s.err = nil
}

func newSubscription(toStore Store, output chan<- spans.Valued) *subscription {
	return &subscription{
		storeID:     toStore.ID,
		storeBootAt: toStore.BootAt,
		output:      output,
	}
}

func (s *subscription) connect(ctx context.Context, dialer LogBackupService) {
	err := s.doConnect(ctx, dialer)
	if err != nil {
		s.emitError(err)
	}
}

func (s *subscription) doConnect(ctx context.Context, dialer LogBackupService) error {
	log.Info("Adding subscription.", zap.String("category", "log backup subscription manager"),
		zap.Uint64("store", s.storeID), zap.Uint64("boot", s.storeBootAt))
	// We should shutdown the background task firstly.
	// Once it yields some error during shuting down, the error won't be brought to next run.
	s.close(ctx)
	s.clearError()

	c, err := dialer.GetLogBackupClient(ctx, s.storeID)
	if err != nil {
		return errors.Annotate(err, "failed to get log backup client")
	}
	cx, cancel := context.WithCancel(ctx)
	cli, err := c.SubscribeFlushEvent(cx, &logbackup.SubscribeFlushEventRequest{
		ClientId: uuid.NewString(),
	})
	if err != nil {
		cancel()
		_ = dialer.ClearCache(ctx, s.storeID)
		return errors.Annotate(err, "failed to subscribe events")
	}
	lcx := logutil.ContextWithField(cx, zap.Uint64("store-id", s.storeID),
		zap.String("category", "log backup flush subscriber"))
	s.cancel = cancel
	s.background = spawnJoinable(func() { s.listenOver(lcx, cli) })
	return nil
}

func (s *subscription) close(ctx context.Context) {
	if s.cancel != nil {
		s.cancel()
		s.background.Wait(ctx)
	}
	// HACK: don't close the internal channel here,
	// because it is a ever-sharing channel.
}

func (s *subscription) listenOver(ctx context.Context, cli eventStream) {
	storeID := s.storeID
	logutil.CL(ctx).Info("Listen starting.", zap.Uint64("store", storeID))
	defer func() {
		if s.onDaemonExit != nil {
			s.onDaemonExit()
		}

		if pData := recover(); pData != nil {
			log.Warn("Subscriber paniked.", zap.Uint64("store", storeID), zap.Any("panic-data", pData), zap.Stack("stack"))
			s.emitError(errors.Annotatef(berrors.ErrUnknown, "panic during executing: %v", pData))
		}
	}()
	for {
		// Shall we use RecvMsg for better performance?
		// Note that the spans.Full requires the input slice be immutable.
		msg, err := cli.Recv()
		if err != nil {
			logutil.CL(ctx).Info("Listen stopped.",
				zap.Uint64("store", storeID), logutil.ShortError(err))
			if err == io.EOF || err == context.Canceled || status.Code(err) == codes.Canceled {
				return
			}
			s.emitError(errors.Annotatef(err, "while receiving from store id %d", storeID))
			return
		}

		log.Debug("Sending events.", zap.Int("size", len(msg.Events)))
		for _, m := range msg.Events {
			start, err := decodeKey(m.StartKey)
			if err != nil {
				logutil.CL(ctx).Warn("start key not encoded, skipping",
					logutil.Key("event", m.StartKey), logutil.ShortError(err))
				continue
			}
			end, err := decodeKey(m.EndKey)
			if err != nil {
				logutil.CL(ctx).Warn("end key not encoded, skipping",
					logutil.Key("event", m.EndKey), logutil.ShortError(err))
				continue
			}
			failpoint.Inject("subscription.listenOver.aboutToSend", func() {})

			evt := spans.Valued{
				Key: spans.Span{
					StartKey: start,
					EndKey:   end,
				},
				Value: m.Checkpoint,
			}
			select {
			case s.output <- evt:
			case <-ctx.Done():
				logutil.CL(ctx).Warn("Context canceled while sending events.",
					zap.Uint64("store", storeID))
				return
			}
		}
		metrics.RegionCheckpointSubscriptionEvent.WithLabelValues(
			strconv.Itoa(int(storeID))).Observe(float64(len(msg.Events)))
	}
}

func (f *FlushSubscriber) addSubscription(ctx context.Context, toStore Store) {
	f.subscriptions[toStore.ID] = newSubscription(toStore, f.eventsTunnel)
}

func (f *FlushSubscriber) removeSubscription(ctx context.Context, toStore uint64) {
	subs, ok := f.subscriptions[toStore]
	if ok {
		log.Info("Removing subscription.", zap.String("category", "log backup subscription manager"),
			zap.Uint64("store", toStore))
		subs.close(ctx)
		delete(f.subscriptions, toStore)
	}
}

// decodeKey decodes the key from TiKV, because the region range is encoded in TiKV.
func decodeKey(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return key, nil
	}
	// Ignore the timestamp...
	_, data, err := codec.DecodeBytes(key, nil)
	if err != nil {
		return key, err
	}
	return data, err
}

func (f *FlushSubscriber) canBeRetried(err error) bool {
	for _, e := range multierr.Errors(errors.Cause(err)) {
		s := status.Convert(e)
		// Is there any other error cannot be retried?
		if s.Code() == codes.Unimplemented {
			return false
		}
	}
	return true
}

func (f *FlushSubscriber) PendingErrors() error {
	var allErr error
	for _, s := range f.subscriptions {
		if err := s.loadError(); err != nil {
			allErr = multierr.Append(allErr, errors.Annotatef(err, "store %d has error", s.storeID))
		}
	}
	return allErr
}
