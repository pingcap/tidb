// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"context"

	"github.com/pingcap/tidb/pkg/ddl/util"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const ttlNotificationPrefix string = "/tidb/ttl/notification/"

// NotificationClient is a client to notify other TTL workers
type NotificationClient interface {
	// Notify sends a notification
	Notify(ctx context.Context, typ string, data string) error
	// WatchNotification opens a channel, in which we could receive all notifications
	WatchNotification(ctx context.Context, typ string) clientv3.WatchChan
}

// NewNotificationClient creates a notification client with etcd
func NewNotificationClient(etcdCli *clientv3.Client) NotificationClient {
	return &etcdClient{
		etcdCli: etcdCli,
	}
}

// Notify stores the corresponding K-V in the etcd
func (c *etcdClient) Notify(ctx context.Context, typ string, data string) error {
	return util.PutKVToEtcd(ctx, c.etcdCli, 1, ttlNotificationPrefix+typ, data)
}

// WatchNotification returns a go channel to get notification
func (c *etcdClient) WatchNotification(ctx context.Context, typ string) clientv3.WatchChan {
	return c.etcdCli.Watch(ctx, ttlNotificationPrefix+typ)
}

// NewMockNotificationClient creates a mock notification client
func NewMockNotificationClient() NotificationClient {
	return &mockClient{
		store:                make(map[string]any),
		commandWatchers:      make([]chan *CmdRequest, 0, 1),
		notificationWatchers: make(map[string][]chan clientv3.WatchResponse),
	}
}

// Notify implements the NotificationClient
func (c *mockClient) Notify(ctx context.Context, typ string, data string) error {
	c.Lock()
	defer c.Unlock()

	watchers, ok := c.notificationWatchers[typ]
	if !ok {
		return nil
	}

	var unsent []chan clientv3.WatchResponse
loop:
	for i, ch := range watchers {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ch <- clientv3.WatchResponse{}:
		default:
			unsent = make([]chan clientv3.WatchResponse, len(watchers), 0)
			copy(unsent, watchers[i:])
			break loop
		}
	}

	if len(unsent) > 0 {
		go func() {
			for _, ch := range unsent {
				select {
				case <-ctx.Done():
					return
				case ch <- clientv3.WatchResponse{}:
				}
			}
		}()
	}

	return nil
}

// WatchNotification implements the NotificationClient
func (c *mockClient) WatchNotification(_ context.Context, typ string) clientv3.WatchChan {
	c.Lock()
	defer c.Unlock()

	ch := make(chan clientv3.WatchResponse, 8)
	c.notificationWatchers[typ] = append(c.notificationWatchers[typ], ch)
	return ch
}
