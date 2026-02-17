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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package domain

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/globalconn"
	"github.com/pingcap/tidb/pkg/util/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)
func (do *Domain) ServerID() uint64 {
	return atomic.LoadUint64(&do.serverID)
}

// IsLostConnectionToPD indicates lost connection to PD or not.
func (do *Domain) IsLostConnectionToPD() bool {
	return do.isLostConnectionToPD.Load() != 0
}

// NextConnID return next connection ID.
func (do *Domain) NextConnID() uint64 {
	return do.connIDAllocator.NextID()
}

// ReleaseConnID releases connection ID.
func (do *Domain) ReleaseConnID(connID uint64) {
	do.connIDAllocator.Release(connID)
}

const (
	serverIDEtcdPath               = "/tidb/server_id"
	refreshServerIDRetryCnt        = 3
	acquireServerIDRetryInterval   = 300 * time.Millisecond
	acquireServerIDTimeout         = 10 * time.Second
	retrieveServerIDSessionTimeout = 10 * time.Second

	acquire32BitsServerIDRetryCnt = 3
)

var (
	// serverIDTTL should be LONG ENOUGH to avoid barbarically killing an on-going long-run SQL.
	serverIDTTL = 12 * time.Hour
	// serverIDTimeToKeepAlive is the interval that we keep serverID TTL alive periodically.
	serverIDTimeToKeepAlive = 5 * time.Minute
	// serverIDTimeToCheckPDConnectionRestored is the interval that we check connection to PD restored (after broken) periodically.
	serverIDTimeToCheckPDConnectionRestored = 10 * time.Second
	// lostConnectionToPDTimeout is the duration that when TiDB cannot connect to PD excceeds this limit,
	//   we realize the connection to PD is lost utterly, and server ID acquired before should be released.
	//   Must be SHORTER than `serverIDTTL`.
	lostConnectionToPDTimeout = 6 * time.Hour
)

var (
	ldflagIsGlobalKillTest                        = "0"  // 1:Yes, otherwise:No.
	ldflagServerIDTTL                             = "10" // in seconds.
	ldflagServerIDTimeToKeepAlive                 = "1"  // in seconds.
	ldflagServerIDTimeToCheckPDConnectionRestored = "1"  // in seconds.
	ldflagLostConnectionToPDTimeout               = "5"  // in seconds.
)

func initByLDFlagsForGlobalKill() {
	if ldflagIsGlobalKillTest == "1" {
		var (
			i   int
			err error
		)

		if i, err = strconv.Atoi(ldflagServerIDTTL); err != nil {
			panic("invalid ldflagServerIDTTL")
		}
		serverIDTTL = time.Duration(i) * time.Second

		if i, err = strconv.Atoi(ldflagServerIDTimeToKeepAlive); err != nil {
			panic("invalid ldflagServerIDTimeToKeepAlive")
		}
		serverIDTimeToKeepAlive = time.Duration(i) * time.Second

		if i, err = strconv.Atoi(ldflagServerIDTimeToCheckPDConnectionRestored); err != nil {
			panic("invalid ldflagServerIDTimeToCheckPDConnectionRestored")
		}
		serverIDTimeToCheckPDConnectionRestored = time.Duration(i) * time.Second

		if i, err = strconv.Atoi(ldflagLostConnectionToPDTimeout); err != nil {
			panic("invalid ldflagLostConnectionToPDTimeout")
		}
		lostConnectionToPDTimeout = time.Duration(i) * time.Second

		logutil.BgLogger().Info("global_kill_test is enabled", zap.Duration("serverIDTTL", serverIDTTL),
			zap.Duration("serverIDTimeToKeepAlive", serverIDTimeToKeepAlive),
			zap.Duration("serverIDTimeToCheckPDConnectionRestored", serverIDTimeToCheckPDConnectionRestored),
			zap.Duration("lostConnectionToPDTimeout", lostConnectionToPDTimeout))
	}
}

func (do *Domain) retrieveServerIDSession(ctx context.Context) (*concurrency.Session, error) {
	if do.serverIDSession != nil {
		return do.serverIDSession, nil
	}

	// `etcdClient.Grant` needs a shortterm timeout, to avoid blocking if connection to PD lost,
	//   while `etcdClient.KeepAlive` should be longterm.
	//   So we separately invoke `etcdClient.Grant` and `concurrency.NewSession` with leaseID.
	childCtx, cancel := context.WithTimeout(ctx, retrieveServerIDSessionTimeout)
	resp, err := do.etcdClient.Grant(childCtx, int64(serverIDTTL.Seconds()))
	cancel()
	if err != nil {
		logutil.BgLogger().Error("retrieveServerIDSession.Grant fail", zap.Error(err))
		return nil, err
	}
	leaseID := resp.ID

	session, err := concurrency.NewSession(do.etcdClient,
		concurrency.WithLease(leaseID), concurrency.WithContext(context.Background()))
	if err != nil {
		logutil.BgLogger().Error("retrieveServerIDSession.NewSession fail", zap.Error(err))
		return nil, err
	}
	do.serverIDSession = session
	return session, nil
}

func (do *Domain) acquireServerID(ctx context.Context) error {
	atomic.StoreUint64(&do.serverID, 0)

	session, err := do.retrieveServerIDSession(ctx)
	if err != nil {
		return err
	}

	conflictCnt := 0
	for {
		var proposeServerID uint64
		if config.GetGlobalConfig().Enable32BitsConnectionID {
			proposeServerID, err = do.proposeServerID(ctx, conflictCnt)
			if err != nil {
				return errors.Trace(err)
			}
		} else {
			// get a random serverID: [1, MaxServerID64]
			proposeServerID = uint64(rand.Int63n(int64(globalconn.MaxServerID64)) + 1) // #nosec G404
		}

		key := fmt.Sprintf("%s/%v", serverIDEtcdPath, proposeServerID)
		cmp := clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
		value := "0"

		childCtx, cancel := context.WithTimeout(ctx, acquireServerIDTimeout)
		txn := do.etcdClient.Txn(childCtx)
		t := txn.If(cmp)
		resp, err := t.Then(clientv3.OpPut(key, value, clientv3.WithLease(session.Lease()))).Commit()
		cancel()
		if err != nil {
			return err
		}
		if !resp.Succeeded {
			logutil.BgLogger().Info("propose serverID exists, try again", zap.Uint64("proposeServerID", proposeServerID))
			time.Sleep(acquireServerIDRetryInterval)
			conflictCnt++
			continue
		}

		atomic.StoreUint64(&do.serverID, proposeServerID)
		logutil.BgLogger().Info("acquireServerID", zap.Uint64("serverID", do.ServerID()),
			zap.String("lease id", strconv.FormatInt(int64(session.Lease()), 16)))
		return nil
	}
}

func (do *Domain) releaseServerID(context.Context) {
	serverID := do.ServerID()
	if serverID == 0 {
		return
	}
	atomic.StoreUint64(&do.serverID, 0)

	if do.etcdClient == nil {
		return
	}

	// closing session releases attached server id and etcd lease.
	leaseID := int64(do.serverIDSession.Lease())
	if err := do.serverIDSession.Close(); err != nil {
		logutil.BgLogger().Error("releaseServerID fail",
			zap.Uint64("serverID", serverID),
			zap.Int64("leaseID", leaseID),
			zap.Error(err))
	} else {
		logutil.BgLogger().Info("releaseServerID succeed",
			zap.Uint64("serverID", serverID),
			zap.Int64("leaseID", leaseID),
		)
	}
}

// propose server ID by random.
func (*Domain) proposeServerID(ctx context.Context, conflictCnt int) (uint64, error) {
	// get a random server ID in range [min, max]
	randomServerID := func(minv uint64, maxv uint64) uint64 {
		return uint64(rand.Int63n(int64(maxv-minv+1)) + int64(minv)) // #nosec G404
	}

	if conflictCnt < acquire32BitsServerIDRetryCnt {
		// get existing server IDs.
		allServerInfo, err := infosync.GetAllServerInfo(ctx)
		if err != nil {
			return 0, errors.Trace(err)
		}
		// `allServerInfo` contains current TiDB.
		if float32(len(allServerInfo)) <= 0.9*float32(globalconn.MaxServerID32) {
			serverIDs := make(map[uint64]struct{}, len(allServerInfo))
			for _, info := range allServerInfo {
				serverID := info.ServerIDGetter()
				if serverID <= globalconn.MaxServerID32 {
					serverIDs[serverID] = struct{}{}
				}
			}

			for range 15 {
				randServerID := randomServerID(1, globalconn.MaxServerID32)
				if _, ok := serverIDs[randServerID]; !ok {
					return randServerID, nil
				}
			}
		}
		logutil.BgLogger().Info("upgrade to 64 bits server ID due to used up", zap.Int("len(allServerInfo)", len(allServerInfo)))
	} else {
		logutil.BgLogger().Info("upgrade to 64 bits server ID due to conflict", zap.Int("conflictCnt", conflictCnt))
	}

	// upgrade to 64 bits.
	return randomServerID(globalconn.MaxServerID32+1, globalconn.MaxServerID64), nil
}

func (do *Domain) refreshServerIDTTL(ctx context.Context) error {
	session, err := do.retrieveServerIDSession(ctx)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("%s/%v", serverIDEtcdPath, do.ServerID())
	value := "0"
	err = ddlutil.PutKVToEtcd(ctx, do.etcdClient, refreshServerIDRetryCnt, key, value, clientv3.WithLease(session.Lease()))
	if err != nil {
		logutil.BgLogger().Error("refreshServerIDTTL fail", zap.Uint64("serverID", do.ServerID()), zap.Error(err))
	} else {
		logutil.BgLogger().Info("refreshServerIDTTL succeed", zap.Uint64("serverID", do.ServerID()),
			zap.String("lease id", strconv.FormatInt(int64(session.Lease()), 16)))
	}
	return err
}

func (do *Domain) serverIDKeeper() {
	defer func() {
		do.wg.Done()
		logutil.BgLogger().Info("serverIDKeeper exited.")
	}()
	defer util.Recover(metrics.LabelDomain, "serverIDKeeper", func() {
		logutil.BgLogger().Info("recover serverIDKeeper.")
		// should be called before `do.wg.Done()`, to ensure that Domain.Close() waits for the new `serverIDKeeper()` routine.
		do.wg.Add(1)
		go do.serverIDKeeper()
	}, false)

	tickerKeepAlive := time.NewTicker(serverIDTimeToKeepAlive)
	tickerCheckRestored := time.NewTicker(serverIDTimeToCheckPDConnectionRestored)
	defer func() {
		tickerKeepAlive.Stop()
		tickerCheckRestored.Stop()
	}()

	blocker := make(chan struct{}) // just used for blocking the sessionDone() when session is nil.
	sessionDone := func() <-chan struct{} {
		if do.serverIDSession == nil {
			return blocker
		}
		return do.serverIDSession.Done()
	}

	var lastSucceedTimestamp time.Time

	onConnectionToPDRestored := func() {
		logutil.BgLogger().Info("restored connection to PD")
		do.isLostConnectionToPD.Store(0)
		lastSucceedTimestamp = time.Now()

		if err := do.info.ServerInfoSyncer().StoreServerInfo(context.Background()); err != nil {
			logutil.BgLogger().Error("StoreServerInfo failed", zap.Error(err))
		}
	}

	onConnectionToPDLost := func() {
		logutil.BgLogger().Warn("lost connection to PD")
		do.isLostConnectionToPD.Store(1)

		// Kill all connections when lost connection to PD,
		//   to avoid the possibility that another TiDB instance acquires the same serverID and generates a same connection ID,
		//   which will lead to a wrong connection killed.
		do.InfoSyncer().GetSessionManager().KillAllConnections()
	}

	for {
		select {
		case <-tickerKeepAlive.C:
			if !do.IsLostConnectionToPD() {
				if err := do.refreshServerIDTTL(context.Background()); err == nil {
					lastSucceedTimestamp = time.Now()
				} else {
					if lostConnectionToPDTimeout > 0 && time.Since(lastSucceedTimestamp) > lostConnectionToPDTimeout {
						onConnectionToPDLost()
					}
				}
			}
		case <-tickerCheckRestored.C:
			if do.IsLostConnectionToPD() {
				if err := do.acquireServerID(context.Background()); err == nil {
					onConnectionToPDRestored()
				}
			}
		case <-sessionDone():
			// inform that TTL of `serverID` is expired. See https://godoc.org/github.com/coreos/etcd/clientv3/concurrency#Session.Done
			//   Should be in `IsLostConnectionToPD` state, as `lostConnectionToPDTimeout` is shorter than `serverIDTTL`.
			//   So just set `do.serverIDSession = nil` to restart `serverID` session in `retrieveServerIDSession()`.
			logutil.BgLogger().Info("serverIDSession need restart")
			do.serverIDSession = nil
		case <-do.exit:
			return
		}
	}
}

