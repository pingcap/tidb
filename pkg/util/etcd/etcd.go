// Copyright 2018 PingCAP, Inc.
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

package etcd

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/util/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/namespace"
	"go.uber.org/zap"
)

const (
	// KeyOpDefaultTimeout is the default timeout for each key operation.
	KeyOpDefaultTimeout = 2 * time.Second
	// KeyOpDefaultRetryCnt is the default retry times for each key operation.
	KeyOpDefaultRetryCnt = 5
	// KeyOpRetryInterval is the interval between two key operations.
	KeyOpRetryInterval = 30 * time.Millisecond
)

// SetEtcdCliByNamespace is used to add an etcd namespace prefix before etcd path.
func SetEtcdCliByNamespace(cli *clientv3.Client, namespacePrefix string) {
	cli.KV = namespace.NewKV(cli.KV, namespacePrefix)
	cli.Watcher = namespace.NewWatcher(cli.Watcher, namespacePrefix)
	cli.Lease = namespace.NewLease(cli.Lease, namespacePrefix)
}

// DeleteKeyFromEtcd deletes key value from etcd.
func DeleteKeyFromEtcd(key string, etcdCli *clientv3.Client, retryCnt int, timeout time.Duration) error {
	var err error
	ctx := context.Background()
	for i := range retryCnt {
		childCtx, cancel := context.WithTimeout(ctx, timeout)
		_, err = etcdCli.Delete(childCtx, key)
		cancel()
		if err == nil {
			return nil
		}
		metrics.RetryableErrorCount.WithLabelValues(err.Error()).Inc()
		logutil.BgLogger().Warn("etcd-cli delete key failed", zap.String("key", key), zap.Error(err), zap.Int("retryCnt", i))
	}
	return errors.Trace(err)
}
