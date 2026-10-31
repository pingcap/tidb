// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package streamhelper

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/util/redact"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type EventType int

const (
	EventAdd EventType = iota
	EventDel
	EventErr
	EventPause
	EventResume
)

func (t EventType) String() string {
	switch t {
	case EventAdd:
		return "Add"
	case EventDel:
		return "Del"
	case EventErr:
		return "Err"
	case EventPause:
		return "Pause"
	case EventResume:
		return "Resume"
	}
	return "Unknown"
}

type TaskEvent struct {
	Type   EventType
	Name   string
	Info   *backuppb.StreamBackupTaskInfo
	Ranges []kv.KeyRange
	Err    error
}

func (t *TaskEvent) String() string {
	if t.Err != nil {
		return fmt.Sprintf("%s(%s, err = %s)", t.Type, t.Name, t.Err)
	}
	return fmt.Sprintf("%s(%s)", t.Type, t.Name)
}

type AdvancerExt struct {
	MetaDataClient
}

var (
	// etcd's default periodic watch progress is too sparse for failover, so request it proactively.
	metadataWatchProgressInterval = 30 * time.Second
	metadataWatchCreateTimeouts   = []time.Duration{5 * time.Second, 10 * time.Second, 15 * time.Second}
	metadataRequestTimeouts       = []time.Duration{5 * time.Second, 10 * time.Second, 15 * time.Second}
	metadataWatchIdleTimeout      = 90 * time.Second
)

const (
	metadataWatchCreating int32 = iota
	metadataWatchCreated
	metadataWatchCreateTimedOut
)

func errorEvent(err error) TaskEvent {
	return TaskEvent{
		Type: EventErr,
		Err:  err,
	}
}

func resetWatchIdleTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(metadataWatchIdleTimeout)
}

func requestWatchProgress(ctx context.Context, watcher clientv3.Watcher) error {
	failpoint.Inject("advancer_skip_watch_progress_request", func() {
		failpoint.Return(nil)
	})
	progressCtx, cancelProgress := context.WithTimeout(ctx, metadataWatchProgressInterval)
	err := watcher.RequestProgress(progressCtx)
	cancelProgress()
	return err
}

func watchIdleTimeoutError(target string) error {
	return errors.Errorf("watching %s timed out after %s without etcd progress",
		target, metadataWatchIdleTimeout)
}

func isRetryableMetadataRequestError(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return false
	}
	if errors.Cause(err) == context.DeadlineExceeded {
		return true
	}
	switch status.Code(err) {
	case codes.DeadlineExceeded, codes.Unavailable:
		return true
	default:
		return false
	}
}

func runMetadataRequestWithRetry[T any](
	ctx context.Context,
	warnMessage string,
	fields []zap.Field,
	request func(context.Context) (T, error),
) (T, error) {
	var (
		lastErr error
		zero    T
	)
	for attempt, timeout := range metadataRequestTimeouts {
		requestCtx, cancel := context.WithTimeout(ctx, timeout)
		resp, err := request(requestCtx)
		cancel()
		if err == nil {
			return resp, nil
		}

		lastErr = err
		retryable := attempt+1 < len(metadataRequestTimeouts) &&
			isRetryableMetadataRequestError(ctx, err)
		logFields := make([]zap.Field, 0, len(fields)+7)
		logFields = append(logFields, zap.String("category", "log backup advancer"))
		logFields = append(logFields, fields...)
		logFields = append(logFields,
			zap.Int("attempt", attempt+1),
			zap.Int("max-attempts", len(metadataRequestTimeouts)),
			zap.Bool("retry", retryable),
			zap.Duration("timeout", timeout),
			logutil.ShortError(err))
		log.Warn(warnMessage, logFields...)
		if retryable {
			continue
		}
		return zero, err
	}
	return zero, lastErr
}

func (t AdvancerExt) toTaskEvent(ctx context.Context, event *clientv3.Event) (TaskEvent, error) {
	te := TaskEvent{}
	var prefix string

	if bytes.HasPrefix(event.Kv.Key, []byte(PrefixOfTask())) {
		prefix = PrefixOfTask()
		te.Name = strings.TrimPrefix(string(event.Kv.Key), prefix)
	} else if bytes.HasPrefix(event.Kv.Key, []byte(PrefixOfPause())) {
		prefix = PrefixOfPause()
		te.Name = strings.TrimPrefix(string(event.Kv.Key), prefix)
	} else {
		return TaskEvent{},
			errors.Annotatef(berrors.ErrInvalidArgument, "the path isn't a task/pause path (%s)",
				string(event.Kv.Key))
	}

	switch {
	case event.Type == clientv3.EventTypePut && prefix == PrefixOfTask():
		te.Type = EventAdd
	case event.Type == clientv3.EventTypeDelete && prefix == PrefixOfTask():
		te.Type = EventDel
	case event.Type == clientv3.EventTypePut && prefix == PrefixOfPause():
		te.Type = EventPause
	case event.Type == clientv3.EventTypeDelete && prefix == PrefixOfPause():
		te.Type = EventResume
	default:
		return TaskEvent{},
			errors.Annotatef(berrors.ErrInvalidArgument,
				"invalid event type or prefix: type=%s, prefix=%s", event.Type, prefix)
	}

	te.Info = new(backuppb.StreamBackupTaskInfo)
	if err := proto.Unmarshal(event.Kv.Value, te.Info); err != nil {
		return TaskEvent{}, err
	}

	var err error
	te.Ranges, err = t.MetaDataClient.TaskByInfo(*te.Info).Ranges(ctx)
	if err != nil {
		return TaskEvent{}, err
	}

	return te, nil
}

func (t AdvancerExt) eventFromWatch(ctx context.Context, resp clientv3.WatchResponse) ([]TaskEvent, error) {
	result := make([]TaskEvent, 0, len(resp.Events))
	if err := resp.Err(); err != nil {
		return nil, err
	}
	for _, event := range resp.Events {
		te, err := t.toTaskEvent(ctx, event)
		if err != nil {
			te.Type = EventErr
			te.Err = err
		}
		result = append(result, te)
	}
	return result, nil
}

func (t AdvancerExt) startListen(ctx context.Context, rev int64, ch chan<- TaskEvent) {
	watcher := t.getWatcher()
	taskCh := watcher.Watch(ctx, PrefixOfTask(), clientv3.WithPrefix(), clientv3.WithRev(rev))
	pauseCh := watcher.Watch(ctx, PrefixOfPause(), clientv3.WithPrefix(), clientv3.WithRev(rev))

	// inner function def
	handleResponse := func(resp clientv3.WatchResponse) bool {
		events, err := t.eventFromWatch(ctx, resp)
		if err != nil {
			log.Warn("Meet error during receiving the task event.",
				zap.String("category", "log backup advancer"), logutil.ShortError(err))
			ch <- errorEvent(err)
			return false
		}
		for _, event := range events {
			ch <- event
		}
		return true
	}

	// inner function def
	collectRemaining := func() {
		log.Info("Start collecting remaining events in the channel.", zap.String("category", "log backup advancer"),
			zap.Int("remained", len(taskCh)))
		defer log.Info("Finish collecting remaining events in the channel.", zap.String("category", "log backup advancer"))
		for {
			if taskCh == nil && pauseCh == nil {
				return
			}

			select {
			case resp, ok := <-taskCh:
				if !ok || !handleResponse(resp) {
					taskCh = nil
				}
			case resp, ok := <-pauseCh:
				if !ok || !handleResponse(resp) {
					pauseCh = nil
				}
			}
		}
	}

	go func() {
		defer close(ch)
		for {
			select {
			case resp, ok := <-taskCh:
				failpoint.Inject("advancer_close_channel", func() {
					// We cannot really close the channel, just simulating it.
					ok = false
				})
				if !ok {
					ch <- errorEvent(io.EOF)
					return
				}
				if !handleResponse(resp) {
					return
				}
			case resp, ok := <-pauseCh:
				failpoint.Inject("advancer_close_pause_channel", func() {
					// We cannot really close the channel, just simulating it.
					ok = false
				})
				if !ok {
					ch <- errorEvent(io.EOF)
					return
				}
				if !handleResponse(resp) {
					return
				}
			case <-ctx.Done():
				collectRemaining()
				ch <- errorEvent(ctx.Err())
				return
			}
		}
	}()
}

func (t AdvancerExt) getFullTasksAsEvent(ctx context.Context) ([]TaskEvent, int64, error) {
	tasks, rev, err := t.GetAllTasksWithRevision(ctx)
	if err != nil {
		return nil, 0, err
	}
	events := make([]TaskEvent, 0, len(tasks))
	for _, task := range tasks {
		ranges, err := task.Ranges(ctx)
		if err != nil {
			return nil, 0, err
		}
		te := TaskEvent{
			Type:   EventAdd,
			Name:   task.Info.Name,
			Info:   &(task.Info),
			Ranges: ranges,
		}
		events = append(events, te)
	}
	return events, rev, nil
}

func (t AdvancerExt) Begin(ctx context.Context, ch chan<- TaskEvent) error {
	initialTasks, rev, err := t.getFullTasksAsEvent(ctx)
	if err != nil {
		return err
	}
	// Note: maybe `go` here so we won't block?
	for _, task := range initialTasks {
		ch <- task
	}
	t.startListen(ctx, rev+1, ch)
	return nil
}

func (t AdvancerExt) GetGlobalCheckpointForTask(ctx context.Context, taskName string) (uint64, error) {
	checkpoint, _, err := t.getGlobalCheckpointWithRevision(ctx, taskName)
	return checkpoint, err
}

func (t MetaDataClient) WaitGlobalCheckpointAdvance(ctx context.Context, taskName string, current uint64) error {
	key := GlobalCheckpointOf(taskName)
	for {
		checkpoint, rev, err := t.getGlobalCheckpointWithRevision(ctx, taskName)
		if err != nil {
			return err
		}
		if checkpoint > current {
			return nil
		}

		err = t.waitCheckpointEvent(ctx, key, current, rev+1)
		if err == nil {
			return nil
		}
		if berrors.Is(err, berrors.ErrPiTRCheckpointWatchRestart) {
			continue
		}
		return err
	}
}

func (t MetaDataClient) getGlobalCheckpointWithRevision(ctx context.Context, taskName string) (uint64, int64, error) {
	key := GlobalCheckpointOf(taskName)
	redactedKey := redact.Key([]byte(key))
	resp, err := runMetadataRequestWithRetry(ctx,
		"failed to get global checkpoint from metadata store",
		[]zap.Field{
			zap.String("key", redactedKey),
			zap.String("task", taskName),
		},
		func(requestCtx context.Context) (*clientv3.GetResponse, error) {
			failpoint.Inject("advancer_get_global_checkpoint_request_timeout", func() {
				failpoint.Return(nil, context.DeadlineExceeded)
			})
			return t.KV.Get(requestCtx, key)
		})
	if err != nil {
		return 0, 0, err
	}

	if len(resp.Kvs) == 0 {
		return 0, resp.Header.Revision, nil
	}

	firstKV := resp.Kvs[0]
	checkpoint, err := parseGlobalCheckpointValue(firstKV.Value)
	if err != nil {
		return 0, 0, err
	}
	// Watch from the response revision rather than the key's ModRevision. The key
	// can stay unchanged long enough for its ModRevision to be compacted.
	return checkpoint, resp.Header.Revision, nil
}

func (t MetaDataClient) waitCheckpointEvent(
	ctx context.Context,
	key string,
	current uint64,
	rev int64,
) error {
	redactedKey := redact.Key([]byte(key))
	var (
		watchCtx    context.Context
		cancelWatch context.CancelFunc
		watcher     clientv3.Watcher
		watchCh     clientv3.WatchChan
	)
	for attempt, timeout := range metadataWatchCreateTimeouts {
		watchCtx, cancelWatch = context.WithCancel(clientv3.WithRequireLeader(ctx))
		// etcd Watch may block before returning the watch channel when creating the watch stream.
		watcher = t.getWatcher()
		var watchCreateState atomic.Int32
		watchCreateTimer := time.AfterFunc(timeout, func() {
			if !watchCreateState.CompareAndSwap(metadataWatchCreating, metadataWatchCreateTimedOut) {
				return
			}
			log.Warn("etcd watch creation timed out, resetting metadata watcher",
				zap.String("category", "log backup advancer"),
				zap.String("key", redactedKey),
				zap.Uint64("current-checkpoint", current),
				zap.Int64("revision", rev),
				zap.Int("attempt", attempt+1),
				zap.Int("max-attempts", len(metadataWatchCreateTimeouts)),
				zap.Bool("retry", attempt+1 < len(metadataWatchCreateTimeouts)),
				zap.Duration("timeout", timeout))
			cancelWatch()
			t.resetWatcher()
		})
		watchCh = watcher.Watch(watchCtx, key, clientv3.WithRev(rev), clientv3.WithProgressNotify())
		if watchCreateState.CompareAndSwap(metadataWatchCreating, metadataWatchCreated) {
			watchCreateTimer.Stop()
		}
		if watchCreateState.Load() == metadataWatchCreateTimedOut {
			log.Warn("global checkpoint watch returned after creation timeout",
				zap.String("category", "log backup advancer"),
				zap.String("key", redactedKey),
				zap.Uint64("current-checkpoint", current),
				zap.Int64("revision", rev),
				zap.Int("attempt", attempt+1),
				zap.Int("max-attempts", len(metadataWatchCreateTimeouts)))
			cancelWatch()
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if attempt+1 < len(metadataWatchCreateTimeouts) {
				continue
			}
			return berrors.ErrPiTRCheckpointWatchRestart.GenWithStackByArgs()
		}
		break
	}
	if watchCh == nil {
		return berrors.ErrPiTRCheckpointWatchRestart.GenWithStackByArgs()
	}
	defer cancelWatch()
	progressTicker := time.NewTicker(metadataWatchProgressInterval)
	defer progressTicker.Stop()
	idleTimer := time.NewTimer(metadataWatchIdleTimeout)
	defer idleTimer.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Info("stop waiting for global checkpoint event because context is done",
				zap.String("category", "log backup advancer"),
				zap.String("key", redactedKey),
				zap.Uint64("current-checkpoint", current),
				zap.Int64("revision", rev),
				logutil.ShortError(ctx.Err()))
			return ctx.Err()
		case resp, ok := <-watchCh:
			resetWatchIdleTimer(idleTimer)
			if !ok {
				log.Warn("global checkpoint watch channel closed",
					zap.String("category", "log backup advancer"),
					zap.String("key", redactedKey),
					zap.Uint64("current-checkpoint", current),
					zap.Int64("revision", rev))
				return berrors.ErrPiTRCheckpointWatchRestart.GenWithStackByArgs()
			}
			if err := resp.Err(); err != nil {
				log.Warn("global checkpoint watch response has error",
					zap.String("category", "log backup advancer"),
					zap.String("key", redactedKey),
					zap.Uint64("current-checkpoint", current),
					zap.Int64("revision", rev),
					zap.Int64("compact-revision", resp.CompactRevision),
					logutil.ShortError(err))
				if resp.CompactRevision != 0 {
					return berrors.ErrPiTRCheckpointWatchRestart.GenWithStackByArgs()
				}
				return err
			}
			for _, event := range resp.Events {
				if event.Type != clientv3.EventTypePut {
					continue
				}
				checkpoint, err := parseGlobalCheckpointValue(event.Kv.Value)
				if err != nil {
					return err
				}
				if checkpoint > current {
					return nil
				}
			}
		case <-progressTicker.C:
			if err := requestWatchProgress(watchCtx, watcher); err != nil {
				log.Warn("failed to request global checkpoint watch progress",
					zap.String("category", "log backup advancer"),
					zap.String("key", redactedKey),
					zap.Uint64("current-checkpoint", current),
					zap.Int64("revision", rev),
					logutil.ShortError(err))
				return err
			}
		case <-idleTimer.C:
			log.Warn("global checkpoint watch idle timeout",
				zap.String("category", "log backup advancer"),
				zap.String("key", redactedKey),
				zap.Uint64("current-checkpoint", current),
				zap.Int64("revision", rev),
				zap.Duration("timeout", metadataWatchIdleTimeout))
			return watchIdleTimeoutError("global checkpoint")
		}
	}
}

func parseGlobalCheckpointValue(value []byte) (uint64, error) {
	if len(value) != 8 {
		return 0, errors.Annotatef(berrors.ErrPiTRMalformedMetadata,
			"the global checkpoint isn't 64bits (it is %d bytes, value = %s)",
			len(value),
			redact.Key(value))
	}
	return binary.BigEndian.Uint64(value), nil
}

func (t AdvancerExt) UploadV3GlobalCheckpointForTask(ctx context.Context, taskName string, checkpoint uint64) error {
	key := GlobalCheckpointOf(taskName)
	value := string(encodeUint64(checkpoint))
	redactedKey := redact.Key([]byte(key))
	oldValue, err := t.GetGlobalCheckpointForTask(ctx, taskName)
	if err != nil {
		return err
	}

	if checkpoint < oldValue {
		log.Warn("skipping upload global checkpoint", zap.String("category", "log backup advancer"),
			zap.Uint64("old", oldValue), zap.Uint64("new", checkpoint))
		return nil
	}

	_, err = runMetadataRequestWithRetry(ctx,
		"failed to upload global checkpoint to metadata store",
		[]zap.Field{
			zap.String("key", redactedKey),
			zap.String("task", taskName),
			zap.Uint64("checkpoint", checkpoint),
		},
		func(requestCtx context.Context) (struct{}, error) {
			failpoint.Inject("advancer_upload_global_checkpoint_request_timeout", func() {
				failpoint.Return(struct{}{}, context.DeadlineExceeded)
			})
			_, err = t.KV.Put(requestCtx, key, value)
			if err == nil {
				failpoint.Inject("advancer_upload_global_checkpoint_commit_timeout", func() {
					err = context.DeadlineExceeded
				})
			}
			return struct{}{}, err
		},
	)
	if err != nil {
		return err
	}
	metrics.LastCheckpoint.WithLabelValues(taskName).Set(float64(checkpoint))
	return nil
}

func (t AdvancerExt) ClearV3GlobalCheckpointForTask(ctx context.Context, taskName string) error {
	key := GlobalCheckpointOf(taskName)
	_, err := t.KV.Delete(ctx, key)
	return err
}
