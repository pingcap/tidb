// Copyright 2025 PingCAP, Inc.
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

package crossks

import (
	"context"
	"strconv"

	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/domain/serverinfo"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

type minStartTSReporter struct {
	coordinator *schemaCoordinator
	etcdCli     *clientv3.Client
	serverID    string
}

func (r *minStartTSReporter) ReportMinStartTS(store kv.Storage, session *concurrency.Session) {
	if r.etcdCli == nil {
		return
	}
	currentVer, err := store.CurrentVersion(kv.GlobalTxnScope)
	if err != nil {
		logutil.BgLogger().Warn("get cross keyspace min start TS failed", zap.Error(err))
		return
	}
	now := oracle.GetTimeFromTS(currentVer.Ver)
	lowerLimit := oracle.GoTimeToLowerLimitStartTS(now, vardef.GCMaxWaitTime.Load()*1000)
	minTS := r.coordinator.minStartTS(lowerLimit, oracle.GoTimeToTS(now))

	path := infosync.ServerMinStartTSPath + "/" + r.serverID
	codec := store.GetCodec()
	if pd.IsKeyspaceUsingKeyspaceLevelGC(codec.GetKeyspaceMeta()) {
		path = keyspace.MakeKeyspaceEtcdNamespace(codec) + path
	}
	// The runtime client is namespaced for user SQL metadata. Use its raw KV
	// connection so unified GC can report at the root, matching normal TiDB.
	client := clientv3.NewKV(r.etcdCli)
	for range serverinfo.KeyOpDefaultRetryCnt {
		ctx, cancel := context.WithTimeout(context.Background(), serverinfo.KeyOpDefaultTimeout)
		_, err = client.Put(ctx, path, strconv.FormatUint(minTS, 10), clientv3.WithLease(session.Lease()))
		cancel()
		if err == nil {
			return
		}
	}
	logutil.BgLogger().Warn("report cross keyspace min start TS failed", zap.String("path", path), zap.Error(err))
}
