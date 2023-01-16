// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package streamhelper

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/redact"
	"github.com/pingcap/tidb/kv"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type EventType int

const (
	EventAdd EventType = iota
	EventDel
	EventErr
)

func (t EventType) String() string {
	switch t {
	case EventAdd:
		return "Add"
	case EventDel:
		return "Del"
	case EventErr:
		return "Err"
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

func errorEvent(err error) TaskEvent {
	return TaskEvent{
		Type: EventErr,
		Err:  err,
	}
}

func (t AdvancerExt) toTaskEvent(ctx context.Context, event *clientv3.Event) (TaskEvent, error) {
	if !bytes.HasPrefix(event.Kv.Key, []byte(PrefixOfTask())) {
		return TaskEvent{}, errors.Annotatef(berrors.ErrInvalidArgument, "the path isn't a task path (%s)", string(event.Kv.Key))
	}

	te := TaskEvent{}
	te.Name = strings.TrimPrefix(string(event.Kv.Key), PrefixOfTask())
	if event.Type == clientv3.EventTypeDelete {
		te.Type = EventDel
	} else if event.Type == clientv3.EventTypePut {
		te.Type = EventAdd
	} else {
		return TaskEvent{}, errors.Annotatef(berrors.ErrInvalidArgument, "event type is wrong (%s)", event.Type)
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
	c := t.Client.Watcher.Watch(ctx, PrefixOfTask(), clientv3.WithPrefix(), clientv3.WithRev(rev))
	handleResponse := func(resp clientv3.WatchResponse) bool {
		events, err := t.eventFromWatch(ctx, resp)
		if err != nil {
			ch <- errorEvent(err)
			return false
		}
		for _, event := range events {
			ch <- event
		}
		return true
	}

	go func() {
		defer close(ch)
		for {
			select {
			case resp, ok := <-c:
				if !ok {
					return
				}
				if !handleResponse(resp) {
					return
				}
			case <-ctx.Done():
				// drain the remain event from channel.
				for {
					select {
					case resp, ok := <-c:
						if !ok {
							return
						}
						if !handleResponse(resp) {
							return
						}
					default:
						return
					}
				}
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
	key := GlobalCheckpointOf(taskName)
	resp, err := t.KV.Get(ctx, key)
	if err != nil {
		return 0, err
	}

	if len(resp.Kvs) == 0 {
		return 0, nil
	}

	firstKV := resp.Kvs[0]
	value := firstKV.Value
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
	oldValue, err := t.GetGlobalCheckpointForTask(ctx, taskName)
	if err != nil {
		return err
	}

	if checkpoint < oldValue {
		log.Warn("[log backup advancer] skipping upload global checkpoint", zap.Uint64("old", oldValue), zap.Uint64("new", checkpoint))
		return nil
	}

	_, err = t.KV.Put(ctx, key, value)
	if err != nil {
		return err
	}
	return nil
}

func (t AdvancerExt) ClearV3GlobalCheckpointForTask(ctx context.Context, taskName string) error {
	key := GlobalCheckpointOf(taskName)
	_, err := t.KV.Delete(ctx, key)
	return err
}
