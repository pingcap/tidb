// Copyright 2023 PingCAP, Inc.
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

package tablestore

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/timer/api"
	"github.com/pingcap/tidb/util/logutil"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	notifyTimeout         = 15 * time.Second
	watchTimerEventCreate = "create"
	watchTimerEventUpdate = "update"
	watchTimerEventDelete = "delete"
)

type notifyMessage struct {
	Events []*notifyEvent `json:"events"`
}

type notifyEvent struct {
	Tp        string `json:"tp"`
	TimerID   string `json:"timer_id"`
	Timestamp int64  `json:"timestamp"`
}

func (e *notifyEvent) toWatchEvent() (*api.WatchTimerEvent, error) {
	if e.TimerID == "" {
		return nil, errors.Errorf("timerID is empty")
	}

	event := &api.WatchTimerEvent{
		TimerID: e.TimerID,
	}

	switch e.Tp {
	case watchTimerEventCreate:
		event.Tp = api.WatchTimerEventCreate
	case watchTimerEventUpdate:
		event.Tp = api.WatchTimerEventUpdate
	case watchTimerEventDelete:
		event.Tp = api.WatchTimerEventDelete
	default:
		return nil, errors.Errorf("invalid WatchTimerEventType: %s", e.Tp)
	}

	return event, nil
}

func newNotifyEvent(tp api.WatchTimerEventType, timerID string) (*notifyEvent, error) {
	event := &notifyEvent{TimerID: timerID, Timestamp: time.Now().Unix()}
	switch tp {
	case api.WatchTimerEventCreate:
		event.Tp = watchTimerEventCreate
	case api.WatchTimerEventUpdate:
		event.Tp = watchTimerEventUpdate
	case api.WatchTimerEventDelete:
		event.Tp = watchTimerEventDelete
	default:
		return nil, errors.Errorf("invalid WatchTimerEventType: %v, timer: %s", tp, timerID)
	}
	return event, nil
}

type etcdNotifier struct {
	ctx          context.Context
	cancel       func()
	wg           sync.WaitGroup
	mu           sync.Mutex
	etcd         *clientv3.Client
	keyPrefix    string
	key          string
	notifyBgChan chan struct{}
	events       []*notifyEvent
	logger       *zap.Logger
}

// NewEtcdNotifier creates a notifier based on etcd
func NewEtcdNotifier(clusterID uint64, etcd *clientv3.Client) api.TimerWatchEventNotifier {
	keyPrefix := fmt.Sprintf("/tidb/timer/cluster/%d/notify/", clusterID)
	ctx, cancel := context.WithCancel(context.Background())
	key := path.Join(keyPrefix, uuid.NewString())
	notifier := &etcdNotifier{
		ctx:          ctx,
		cancel:       cancel,
		etcd:         etcd,
		keyPrefix:    keyPrefix,
		key:          key,
		events:       make([]*notifyEvent, 0, 8),
		notifyBgChan: make(chan struct{}, 1),
		logger:       logutil.BgLogger().With(zap.String("EtcdKey", key)),
	}
	notifier.wg.Add(1)
	go notifier.notifyLoop()
	return notifier
}

func (n *etcdNotifier) Watch(ctx context.Context) api.WatchTimerChan {
	n.mu.Lock()
	defer n.mu.Unlock()
	ch := make(chan api.WatchTimerResponse)
	if n.cancel == nil {
		// it means closed
		close(ch)
		return ch
	}

	n.wg.Add(1)
	go func() {
		logger := n.logger.With(zap.String("watcherID", uuid.NewString()))
		logger.Info("new etcd watcher created to watch timer events")
		defer func() {
			logger.Info("etcd watcher exited to watch timer events")
			close(ch)
			n.wg.Done()
		}()

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		etcdCh := n.etcd.Watch(ctx, n.keyPrefix, clientv3.WithPrefix())
		for {
			select {
			case <-ctx.Done():
				return
			case <-n.ctx.Done():
				return
			case etcdResp, ok := <-etcdCh:
				if !ok {
					return
				}

				for _, evt := range etcdResp.Events {
					if evt.Type != mvccpb.PUT {
						continue
					}

					jsonStr := evt.Kv.Value
					var msg notifyMessage
					if err := json.Unmarshal(jsonStr, &msg); err != nil {
						logger.Error("failed to decode message", zap.Error(err))
						continue
					}

					resp := api.WatchTimerResponse{
						Events: make([]*api.WatchTimerEvent, 0, len(msg.Events)),
					}

					for _, event := range msg.Events {
						watchEvent, err := event.toWatchEvent()
						if err != nil {
							logger.Error("failed to make a watch event",
								zap.Error(err),
								zap.ByteString("json", jsonStr))
							continue
						}
						resp.Events = append(resp.Events, watchEvent)
					}

					select {
					case <-ctx.Done():
						return
					case <-n.ctx.Done():
						return
					case ch <- resp:
					}
				}
			}
		}
	}()
	return ch
}

func (n *etcdNotifier) Notify(tp api.WatchTimerEventType, timerID string) {
	event, err := newNotifyEvent(tp, timerID)
	if err != nil {
		n.logger.Error("failed to create notify event", zap.Any("tp", tp), zap.String("timerID", timerID))
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.cancel == nil {
		// it means closed
		return
	}

	n.events = append(n.events, event)
	select {
	case n.notifyBgChan <- struct{}{}:
	default:
	}
}

func (n *etcdNotifier) notifyLoop() {
	n.logger.Info("etcd notify loop to watch timer events started")
	defer func() {
		n.logger.Info("etcd notify loop to watch timer events stopped")
		n.wg.Done()
	}()
	var leaseID clientv3.LeaseID
	var keepAlive <-chan *clientv3.LeaseKeepAliveResponse
	for {
		select {
		case <-n.ctx.Done():
			return
		case _, ok := <-keepAlive:
			if !ok {
				leaseID = 0
				keepAlive = nil
			}
		case _, ok := <-n.notifyBgChan:
			if !ok {
				return
			}

			n.mu.Lock()
			var bs []byte
			var err error
			if len(n.events) > 0 {
				message := &notifyMessage{Events: n.events}
				bs, err = json.Marshal(message)
				n.events = n.events[:0]
			}
			n.mu.Unlock()

			if err != nil {
				terror.Log(err)
				continue
			}

			if bs != nil {
				n.sentEventBytes(bs, leaseID, keepAlive)
			}
		}
	}
}

func (n *etcdNotifier) sentEventBytes(bs []byte, leaseID clientv3.LeaseID, keepAlive <-chan *clientv3.LeaseKeepAliveResponse) (clientv3.LeaseID, <-chan *clientv3.LeaseKeepAliveResponse) {
	ctx, cancel := context.WithTimeout(n.ctx, notifyTimeout)
	defer cancel()

	if leaseID == 0 {
		keepAlive = nil
		resp, err := n.etcd.Grant(ctx, 100)
		if err != nil {
			n.logger.Error("failed to grant lease", zap.Error(err))
			return leaseID, keepAlive
		}
		leaseID = resp.ID
	}

	if keepAlive == nil {
		ch, err := n.etcd.KeepAlive(n.ctx, leaseID)
		if err != nil {
			n.logger.Error("failed to keep alive lease", zap.Error(err), zap.Any("lease", leaseID))
			return leaseID, keepAlive
		}
		keepAlive = ch
	}

	if _, err := n.etcd.Put(ctx, n.key, string(bs)); err != nil {
		n.logger.Error("failed to put key", zap.Error(err))
	}

	return leaseID, keepAlive
}

func (n *etcdNotifier) Close() {
	n.mu.Lock()
	if n.cancel != nil {
		n.cancel()
		n.cancel = nil
	}
	n.mu.Unlock()
	n.wg.Wait()
}
