// Copyright 2017 PingCAP, Inc.
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

package syncer

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx/variable"
	tidbutil "github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

const (
	// InitialVersion is the initial schema version for every server.
	// It's exported for testing.
	InitialVersion       = "0"
	putKeyNoRetry        = 1
	keyOpDefaultRetryCnt = 3
	putKeyRetryUnlimited = math.MaxInt64
	checkVersInterval    = 20 * time.Millisecond
	ddlPrompt            = "ddl-syncer"
)

var (
	// CheckVersFirstWaitTime is a waitting time before the owner checks all the servers of the schema version,
	// and it's an exported variable for testing.
	CheckVersFirstWaitTime = 50 * time.Millisecond
)

// SchemaSyncer is used to synchronize schema version between the DDL worker leader and followers through etcd.
type SchemaSyncer interface {
	// Init sets the global schema version path to etcd if it isn't exist,
	// then watch this path, and initializes the self schema version to etcd.
	Init(ctx context.Context) error
	// UpdateSelfVersion updates the current version to the self path on etcd.
	UpdateSelfVersion(ctx context.Context, jobID int64, version int64) error
	// OwnerUpdateGlobalVersion updates the latest version to the global path on etcd until updating is successful or the ctx is done.
	OwnerUpdateGlobalVersion(ctx context.Context, version int64) error
	// GlobalVersionCh gets the chan for watching global version.
	GlobalVersionCh() clientv3.WatchChan
	// WatchGlobalSchemaVer watches the global schema version.
	WatchGlobalSchemaVer(ctx context.Context)
	// Done returns a channel that closes when the syncer is no longer being refreshed.
	Done() <-chan struct{}
	// Restart restarts the syncer when it's on longer being refreshed.
	Restart(ctx context.Context) error
	// OwnerCheckAllVersions checks whether all followers' schema version are equal to
	// the latest schema version. (exclude the isolated TiDB)
	// It returns until all servers' versions are equal to the latest version.
	OwnerCheckAllVersions(ctx context.Context, jobID int64, latestVer int64) error
	// Close ends SchemaSyncer.
	Close()
}

type schemaVersionSyncer struct {
	selfSchemaVerPath string
	etcdCli           *clientv3.Client
	session           unsafe.Pointer
	mu                struct {
		sync.RWMutex
		globalVerCh clientv3.WatchChan
	}
	ddlID string
}

// NewSchemaSyncer creates a new SchemaSyncer.
func NewSchemaSyncer(etcdCli *clientv3.Client, id string) SchemaSyncer {
	return &schemaVersionSyncer{
		etcdCli:           etcdCli,
		selfSchemaVerPath: fmt.Sprintf("%s/%s", util.DDLAllSchemaVersions, id),
		ddlID:             id,
	}
}

// Init implements SchemaSyncer.Init interface.
func (s *schemaVersionSyncer) Init(ctx context.Context) error {
	startTime := time.Now()
	var err error
	defer func() {
		metrics.DeploySyncerHistogram.WithLabelValues(metrics.SyncerInit, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	}()

	_, err = s.etcdCli.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(util.DDLGlobalSchemaVersion), "=", 0)).
		Then(clientv3.OpPut(util.DDLGlobalSchemaVersion, InitialVersion)).
		Commit()
	if err != nil {
		return errors.Trace(err)
	}
	logPrefix := fmt.Sprintf("[%s] %s", ddlPrompt, s.selfSchemaVerPath)
	session, err := tidbutil.NewSession(ctx, logPrefix, s.etcdCli, tidbutil.NewSessionDefaultRetryCnt, util.SessionTTL)
	if err != nil {
		return errors.Trace(err)
	}
	s.storeSession(session)

	s.mu.Lock()
	s.mu.globalVerCh = s.etcdCli.Watch(ctx, util.DDLGlobalSchemaVersion)
	s.mu.Unlock()

	err = util.PutKVToEtcd(ctx, s.etcdCli, keyOpDefaultRetryCnt, s.selfSchemaVerPath, InitialVersion,
		clientv3.WithLease(s.loadSession().Lease()))
	return errors.Trace(err)
}

func (s *schemaVersionSyncer) loadSession() *concurrency.Session {
	return (*concurrency.Session)(atomic.LoadPointer(&s.session))
}

func (s *schemaVersionSyncer) storeSession(session *concurrency.Session) {
	atomic.StorePointer(&s.session, (unsafe.Pointer)(session))
}

// Done implements SchemaSyncer.Done interface.
func (s *schemaVersionSyncer) Done() <-chan struct{} {
	failpoint.Inject("ErrorMockSessionDone", func(val failpoint.Value) {
		if val.(bool) {
			err := s.loadSession().Close()
			logutil.BgLogger().Error("close session failed", zap.Error(err))
		}
	})

	return s.loadSession().Done()
}

// Restart implements SchemaSyncer.Restart interface.
func (s *schemaVersionSyncer) Restart(ctx context.Context) error {
	startTime := time.Now()
	var err error
	defer func() {
		metrics.DeploySyncerHistogram.WithLabelValues(metrics.SyncerRestart, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	}()

	logPrefix := fmt.Sprintf("[%s] %s", ddlPrompt, s.selfSchemaVerPath)
	// NewSession's context will affect the exit of the session.
	session, err := tidbutil.NewSession(ctx, logPrefix, s.etcdCli, tidbutil.NewSessionRetryUnlimited, util.SessionTTL)
	if err != nil {
		return errors.Trace(err)
	}
	s.storeSession(session)

	childCtx, cancel := context.WithTimeout(ctx, util.KeyOpDefaultTimeout)
	defer cancel()
	err = util.PutKVToEtcd(childCtx, s.etcdCli, putKeyRetryUnlimited, s.selfSchemaVerPath, InitialVersion,
		clientv3.WithLease(s.loadSession().Lease()))

	return errors.Trace(err)
}

// GlobalVersionCh implements SchemaSyncer.GlobalVersionCh interface.
func (s *schemaVersionSyncer) GlobalVersionCh() clientv3.WatchChan {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mu.globalVerCh
}

// WatchGlobalSchemaVer implements SchemaSyncer.WatchGlobalSchemaVer interface.
func (s *schemaVersionSyncer) WatchGlobalSchemaVer(ctx context.Context) {
	startTime := time.Now()
	// Make sure the globalVerCh doesn't receive the information of 'close' before we finish the rewatch.
	s.mu.Lock()
	s.mu.globalVerCh = nil
	s.mu.Unlock()

	go func() {
		defer func() {
			metrics.DeploySyncerHistogram.WithLabelValues(metrics.SyncerRewatch, metrics.RetLabel(nil)).Observe(time.Since(startTime).Seconds())
		}()
		ch := s.etcdCli.Watch(ctx, util.DDLGlobalSchemaVersion)

		s.mu.Lock()
		s.mu.globalVerCh = ch
		s.mu.Unlock()
		logutil.BgLogger().Info("[ddl] syncer watch global schema finished")
	}()
}

// UpdateSelfVersion implements SchemaSyncer.UpdateSelfVersion interface.
func (s *schemaVersionSyncer) UpdateSelfVersion(ctx context.Context, jobID int64, version int64) error {
	startTime := time.Now()
	ver := strconv.FormatInt(version, 10)
	var err error
	var path string
	if variable.EnableMDL.Load() {
		path = fmt.Sprintf("%s/%d/%s", util.DDLAllSchemaVersionsByJob, jobID, s.ddlID)
	} else {
		path = s.selfSchemaVerPath
	}

	err = util.PutKVToEtcd(ctx, s.etcdCli, putKeyNoRetry, path, ver,
		clientv3.WithLease(s.loadSession().Lease()))

	metrics.UpdateSelfVersionHistogram.WithLabelValues(metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	return errors.Trace(err)
}

// OwnerUpdateGlobalVersion implements SchemaSyncer.OwnerUpdateGlobalVersion interface.
func (s *schemaVersionSyncer) OwnerUpdateGlobalVersion(ctx context.Context, version int64) error {
	startTime := time.Now()
	ver := strconv.FormatInt(version, 10)
	// TODO: If the version is larger than the original global version, we need set the version.
	// Otherwise, we'd better set the original global version.
	err := util.PutKVToEtcd(ctx, s.etcdCli, putKeyRetryUnlimited, util.DDLGlobalSchemaVersion, ver)
	metrics.OwnerHandleSyncerHistogram.WithLabelValues(metrics.OwnerUpdateGlobalVersion, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	return errors.Trace(err)
}

// removeSelfVersionPath remove the self path from etcd.
func (s *schemaVersionSyncer) removeSelfVersionPath() error {
	startTime := time.Now()
	var err error
	defer func() {
		metrics.DeploySyncerHistogram.WithLabelValues(metrics.SyncerClear, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	}()

	err = util.DeleteKeyFromEtcd(s.selfSchemaVerPath, s.etcdCli, keyOpDefaultRetryCnt, util.KeyOpDefaultTimeout)
	return errors.Trace(err)
}

// OwnerCheckAllVersions implements SchemaSyncer.OwnerCheckAllVersions interface.
func (s *schemaVersionSyncer) OwnerCheckAllVersions(ctx context.Context, jobID int64, latestVer int64) error {
	startTime := time.Now()
	time.Sleep(CheckVersFirstWaitTime)
	notMatchVerCnt := 0
	intervalCnt := int(time.Second / checkVersInterval)

	var err error
	defer func() {
		metrics.OwnerHandleSyncerHistogram.WithLabelValues(metrics.OwnerCheckAllVersions, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	}()

	// If MDL is disabled, updatedMap is a cache. We need to ensure all the keys equal to the least version.
	// We can skip checking the key if it is checked in the cache(set by the previous loop).
	// If MDL is enabled, updatedMap is used to check if all the servers report the least version.
	// updatedMap is initialed to record all the server in every loop. We delete a server from the map if it gets the metadata lock(the key version equal the given version.
	// updatedMap should be empty if all the servers get the metadata lock.
	updatedMap := make(map[string]string)
	for {
		if util.IsContextDone(ctx) {
			// ctx is canceled or timeout.
			err = errors.Trace(ctx.Err())
			return err
		}

		// Prepare path and updatedMap.
		path := util.DDLAllSchemaVersions
		if variable.EnableMDL.Load() {
			path = fmt.Sprintf("%s/%d/", util.DDLAllSchemaVersionsByJob, jobID)
			serverInfos, err := infosync.GetAllServerInfo(ctx)
			if err != nil {
				return err
			}
			updatedMap = make(map[string]string)
			instance2id := make(map[string]string)

			// Set updatedMap according to the serverInfos, and remove some invalid serverInfos.
			for _, info := range serverInfos {
				instance := fmt.Sprintf("%s:%d", info.IP, info.Port)
				if id, ok := instance2id[instance]; ok {
					if info.StartTimestamp > serverInfos[id].StartTimestamp {
						// Replace it.
						delete(updatedMap, id)
						updatedMap[info.ID] = fmt.Sprintf("instance ip %s, port %d, id %s", info.IP, info.Port, info.ID)
						instance2id[instance] = info.ID
					}
				} else {
					updatedMap[info.ID] = fmt.Sprintf("instance ip %s, port %d, id %s", info.IP, info.Port, info.ID)
					instance2id[instance] = info.ID
				}
			}
		}

		// Get all the schema versions from ETCD.
		resp, err := s.etcdCli.Get(ctx, path, clientv3.WithPrefix())
		if err != nil {
			logutil.BgLogger().Info("[ddl] syncer check all versions failed, continue checking.", zap.Error(err))
			continue
		}

		// Check all schema versions.
		succ := true
		if variable.EnableMDL.Load() {
			for _, kv := range resp.Kvs {
				key := string(kv.Key)
				ver, err := strconv.Atoi(string(kv.Value))
				if err != nil {
					logutil.BgLogger().Info("[ddl] syncer check all versions, convert value to int failed, continue checking.", zap.String("ddl", string(kv.Key)), zap.String("value", string(kv.Value)), zap.Error(err))
					succ = false
					break
				}
				if int64(ver) < latestVer {
					if notMatchVerCnt%intervalCnt == 0 {
						logutil.BgLogger().Info("[ddl] syncer check all versions, someone is not synced, continue checking",
							zap.String("ddl", string(kv.Key)), zap.Int("currentVer", ver), zap.Int64("latestVer", latestVer))
					}
					succ = false
					notMatchVerCnt++
					break
				}
				delete(updatedMap, key[strings.LastIndex(key, "/")+1:])
			}
			if len(updatedMap) > 0 {
				succ = false
				for _, info := range updatedMap {
					logutil.BgLogger().Info("[ddl] syncer check all versions, someone is not synced", zap.String("info", info), zap.Int64("ddl job id", jobID), zap.Int64("ver", latestVer))
				}
			}
		} else {
			for _, kv := range resp.Kvs {
				if _, ok := updatedMap[string(kv.Key)]; ok {
					continue
				}

				ver, err := strconv.Atoi(string(kv.Value))
				if err != nil {
					logutil.BgLogger().Info("[ddl] syncer check all versions, convert value to int failed, continue checking.", zap.String("ddl", string(kv.Key)), zap.String("value", string(kv.Value)), zap.Error(err))
					succ = false
					break
				}
				if int64(ver) < latestVer {
					if notMatchVerCnt%intervalCnt == 0 {
						logutil.BgLogger().Info("[ddl] syncer check all versions, someone is not synced, continue checking",
							zap.String("ddl", string(kv.Key)), zap.Int("currentVer", ver), zap.Int64("latestVer", latestVer))
					}
					succ = false
					notMatchVerCnt++
					break
				}
				updatedMap[string(kv.Key)] = ""
			}
		}

		if succ {
			return nil
		}
		time.Sleep(checkVersInterval)
	}
}

func (s *schemaVersionSyncer) Close() {
	err := s.removeSelfVersionPath()
	if err != nil {
		logutil.BgLogger().Error("[ddl] remove self version path failed", zap.Error(err))
	}
}
