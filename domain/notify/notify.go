// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package notify

import (
	"github.com/coreos/etcd/clientv3"
	"github.com/juju/errors"
	goctx "golang.org/x/net/context"
)

const privilegeKey = "/tidb/privilege"

// UpdatePrivilege updates privilege key in etcd, TiDB client that watches
// the key will get notification.
func UpdatePrivilege(cli *clientv3.Client) error {
	_, err := cli.KV.Put(goctx.Background(), privilegeKey, "")
	return errors.Trace(err)
}

// WatchPrivilege returns a channel for receiving notification.
func WatchPrivilege(cli *clientv3.Client) clientv3.WatchChan {
	return cli.Watch(goctx.Background(), privilegeKey)
}
