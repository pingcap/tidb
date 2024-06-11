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
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	disttaskutil "github.com/pingcap/tidb/pkg/util/disttask"
	"go.etcd.io/etcd/api/v3/mvccpb"
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

// Watcher is responsible for watching the etcd path related operations.
type Watcher interface {
	// WatchChan returns the chan for watching etcd path.
	WatchChan() clientv3.WatchChan
	// Watch watches the etcd path.
	Watch(ctx context.Context, etcdCli *clientv3.Client, path string)
	// Rewatch rewatches the etcd path.
	Rewatch(ctx context.Context, etcdCli *clientv3.Client, path string)
}

type watcher struct {
	sync.RWMutex
	wCh clientv3.WatchChan
}

// WatchChan implements SyncerWatch.WatchChan interface.
func (w *watcher) WatchChan() clientv3.WatchChan {
	w.RLock()
	defer w.RUnlock()
	return w.wCh
}

// Watch implements SyncerWatch.Watch interface.
func (w *watcher) Watch(ctx context.Context, etcdCli *clientv3.Client, path string) {
	w.Lock()
	w.wCh = etcdCli.Watch(ctx, path)
	w.Unlock()
}

// Rewatch implements SyncerWatch.Rewatch interface.
func (w *watcher) Rewatch(ctx context.Context, etcdCli *clientv3.Client, path string) {
	startTime := time.Now()
	// Make sure the wCh doesn't receive the information of 'close' before we finish the rewatch.
	w.Lock()
	w.wCh = nil
	w.Unlock()

	go func() {
		defer func() {
			metrics.DeploySyncerHistogram.WithLabelValues(metrics.SyncerRewatch, metrics.RetLabel(nil)).Observe(time.Since(startTime).Seconds())
		}()
		wCh := etcdCli.Watch(ctx, path)

		w.Lock()
		w.wCh = wCh
		w.Unlock()
		logutil.DDLLogger().Info("syncer rewatch global info finished")
	}()
}

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
	// SyncJobSchemaVerLoop syncs the schema versions on all TiDB nodes for DDL jobs.
	SyncJobSchemaVerLoop(ctx context.Context)
	// Close ends SchemaSyncer.
	Close()
}

// nodeVersions is used to record the schema versions of all TiDB nodes for a DDL job.
type nodeVersions struct {
	sync.Mutex
	nodeVersions map[string]int64
	// onceMatchFn is used to check if all the servers report the least version.
	// If all the servers report the least version, i.e. return true, it will be
	// set to nil.
	onceMatchFn func(map[string]int64) bool
}

func newNodeVersions(initialCap int, fn func(map[string]int64) bool) *nodeVersions {
	return &nodeVersions{
		nodeVersions: make(map[string]int64, initialCap),
		onceMatchFn:  fn,
	}
}

func (v *nodeVersions) add(nodeID string, ver int64) {
	v.Lock()
	defer v.Unlock()
	v.nodeVersions[nodeID] = ver
	if v.onceMatchFn != nil {
		if ok := v.onceMatchFn(v.nodeVersions); ok {
			v.onceMatchFn = nil
		}
	}
}

func (v *nodeVersions) del(nodeID string) {
	v.Lock()
	defer v.Unlock()
	delete(v.nodeVersions, nodeID)
	// we don't call onceMatchFn here, for only "add" can cause onceMatchFn return
	// true currently.
}

func (v *nodeVersions) len() int {
	v.Lock()
	defer v.Unlock()
	return len(v.nodeVersions)
}

// matchOrSet onceMatchFn must be nil before calling this method.
func (v *nodeVersions) matchOrSet(fn func(nodeVersions map[string]int64) bool) {
	v.Lock()
	defer v.Unlock()
	if ok := fn(v.nodeVersions); !ok {
		v.onceMatchFn = fn
	}
}

func (v *nodeVersions) clearData() {
	v.Lock()
	defer v.Unlock()
	v.nodeVersions = make(map[string]int64, len(v.nodeVersions))
}

func (v *nodeVersions) clearMatchFn() {
	v.Lock()
	defer v.Unlock()
	v.onceMatchFn = nil
}

func (v *nodeVersions) emptyAndNotUsed() bool {
	v.Lock()
	defer v.Unlock()
	return len(v.nodeVersions) == 0 && v.onceMatchFn == nil
}

// for test
func (v *nodeVersions) getMatchFn() func(map[string]int64) bool {
	v.Lock()
	defer v.Unlock()
	return v.onceMatchFn
}

type schemaVersionSyncer struct {
	selfSchemaVerPath string
	etcdCli           *clientv3.Client
	session           unsafe.Pointer
	globalVerWatcher  watcher
	ddlID             string

	mu               sync.RWMutex
	jobNodeVersions  map[int64]*nodeVersions
	jobNodeVerPrefix string
}

// NewSchemaSyncer creates a new SchemaSyncer.
func NewSchemaSyncer(etcdCli *clientv3.Client, id string) SchemaSyncer {
	return &schemaVersionSyncer{
		etcdCli:           etcdCli,
		selfSchemaVerPath: fmt.Sprintf("%s/%s", util.DDLAllSchemaVersions, id),
		ddlID:             id,

		jobNodeVersions:  make(map[int64]*nodeVersions),
		jobNodeVerPrefix: util.DDLAllSchemaVersionsByJob + "/",
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

	s.globalVerWatcher.Watch(ctx, s.etcdCli, util.DDLGlobalSchemaVersion)

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
			logutil.DDLLogger().Error("close session failed", zap.Error(err))
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
	return s.globalVerWatcher.WatchChan()
}

// WatchGlobalSchemaVer implements SchemaSyncer.WatchGlobalSchemaVer interface.
func (s *schemaVersionSyncer) WatchGlobalSchemaVer(ctx context.Context) {
	s.globalVerWatcher.Rewatch(ctx, s.etcdCli, util.DDLGlobalSchemaVersion)
}

// UpdateSelfVersion implements SchemaSyncer.UpdateSelfVersion interface.
func (s *schemaVersionSyncer) UpdateSelfVersion(ctx context.Context, jobID int64, version int64) error {
	startTime := time.Now()
	ver := strconv.FormatInt(version, 10)
	var err error
	var path string
	if variable.EnableMDL.Load() {
		path = fmt.Sprintf("%s/%d/%s", util.DDLAllSchemaVersionsByJob, jobID, s.ddlID)
		err = util.PutKVToEtcdMono(ctx, s.etcdCli, keyOpDefaultRetryCnt, path, ver)
	} else {
		path = s.selfSchemaVerPath
		err = util.PutKVToEtcd(ctx, s.etcdCli, putKeyNoRetry, path, ver,
			clientv3.WithLease(s.loadSession().Lease()))
	}

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
	if !variable.EnableMDL.Load() {
		time.Sleep(CheckVersFirstWaitTime)
	}
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
		if err := ctx.Err(); err != nil {
			// ctx is canceled or timeout.
			return errors.Trace(err)
		}

		if variable.EnableMDL.Load() {
			serverInfos, err := infosync.GetAllServerInfo(ctx)
			if err != nil {
				return err
			}
			updatedMap = make(map[string]string)
			instance2id := make(map[string]string)

			for _, info := range serverInfos {
				instance := disttaskutil.GenerateExecID(info)
				// if some node shutdown abnormally and start, we might see some
				// instance with different id, we should use the latest one.
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

		// Check all schema versions.
		if variable.EnableMDL.Load() {
			notifyCh := make(chan struct{})
			var unmatchedNodeID atomic.Pointer[string]
			matchFn := func(nodeVersions map[string]int64) bool {
				if len(nodeVersions) < len(updatedMap) {
					return false
				}
				for tidbID := range updatedMap {
					if nodeVer, ok := nodeVersions[tidbID]; !ok || nodeVer < latestVer {
						id := tidbID
						unmatchedNodeID.Store(&id)
						return false
					}
				}
				close(notifyCh)
				return true
			}
			item := s.jobSchemaVerMatchOrSet(jobID, matchFn)
			select {
			case <-notifyCh:
				return nil
			case <-ctx.Done():
				item.clearMatchFn()
				return errors.Trace(ctx.Err())
			case <-time.After(time.Second):
				item.clearMatchFn()
				if id := unmatchedNodeID.Load(); id != nil {
					logutil.DDLLogger().Info("syncer check all versions, someone is not synced",
						zap.String("info", *id),
						zap.Int64("ddl job id", jobID),
						zap.Int64("ver", latestVer))
				} else {
					logutil.DDLLogger().Info("syncer check all versions, all nodes are not synced",
						zap.Int64("ddl job id", jobID),
						zap.Int64("ver", latestVer))
				}
			}
		} else {
			// Get all the schema versions from ETCD.
			resp, err := s.etcdCli.Get(ctx, util.DDLAllSchemaVersions, clientv3.WithPrefix())
			if err != nil {
				logutil.DDLLogger().Info("syncer check all versions failed, continue checking.", zap.Error(err))
				continue
			}
			succ := true
			for _, kv := range resp.Kvs {
				if _, ok := updatedMap[string(kv.Key)]; ok {
					continue
				}

				succ = isUpdatedLatestVersion(string(kv.Key), string(kv.Value), latestVer, notMatchVerCnt, intervalCnt, true)
				if !succ {
					break
				}
				updatedMap[string(kv.Key)] = ""
			}

			if succ {
				return nil
			}
			time.Sleep(checkVersInterval)
			notMatchVerCnt++
		}
	}
}

// SyncJobSchemaVerLoop implements SchemaSyncer.SyncJobSchemaVerLoop interface.
func (s *schemaVersionSyncer) SyncJobSchemaVerLoop(ctx context.Context) {
	for {
		s.syncJobSchemaVer(ctx)
		logutil.DDLLogger().Info("schema version sync loop interrupted, retrying...")
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func (s *schemaVersionSyncer) syncJobSchemaVer(ctx context.Context) {
	resp, err := s.etcdCli.Get(ctx, s.jobNodeVerPrefix, clientv3.WithPrefix())
	if err != nil {
		logutil.DDLLogger().Info("get all job versions failed", zap.Error(err))
		return
	}
	s.mu.Lock()
	for jobID, item := range s.jobNodeVersions {
		item.clearData()
		// we might miss some DELETE events during retry, some items might be emptyAndNotUsed, remove them.
		if item.emptyAndNotUsed() {
			delete(s.jobNodeVersions, jobID)
		}
	}
	s.mu.Unlock()
	for _, oneKV := range resp.Kvs {
		s.handleJobSchemaVerKV(oneKV, mvccpb.PUT)
	}

	startRev := resp.Header.Revision + 1
	watchCtx, watchCtxCancel := context.WithCancel(ctx)
	defer watchCtxCancel()
	watchCtx = clientv3.WithRequireLeader(watchCtx)
	watchCh := s.etcdCli.Watch(watchCtx, s.jobNodeVerPrefix, clientv3.WithPrefix(), clientv3.WithRev(startRev))
	for {
		var (
			wresp clientv3.WatchResponse
			ok    bool
		)
		select {
		case <-watchCtx.Done():
			return
		case wresp, ok = <-watchCh:
			if !ok {
				// ctx must be cancelled, else we should have received a response
				// with err and caught by below err check.
				return
			}
		}
		failpoint.Inject("mockCompaction", func() {
			wresp.CompactRevision = 123
		})
		if err := wresp.Err(); err != nil {
			logutil.DDLLogger().Warn("watch job version failed", zap.Error(err))
			return
		}
		for _, ev := range wresp.Events {
			s.handleJobSchemaVerKV(ev.Kv, ev.Type)
		}
	}
}

func (s *schemaVersionSyncer) handleJobSchemaVerKV(kv *mvccpb.KeyValue, tp mvccpb.Event_EventType) {
	jobID, tidbID, schemaVer, valid := decodeJobVersionEvent(kv, tp, s.jobNodeVerPrefix)
	if !valid {
		logutil.DDLLogger().Error("invalid job version kv", zap.Stringer("kv", kv), zap.Stringer("type", tp))
		return
	}
	if tp == mvccpb.PUT {
		s.mu.Lock()
		item, exists := s.jobNodeVersions[jobID]
		if !exists {
			item = newNodeVersions(1, nil)
			s.jobNodeVersions[jobID] = item
		}
		s.mu.Unlock()
		item.add(tidbID, schemaVer)
	} else { // DELETE
		s.mu.Lock()
		if item, exists := s.jobNodeVersions[jobID]; exists {
			item.del(tidbID)
			if item.len() == 0 {
				delete(s.jobNodeVersions, jobID)
			}
		}
		s.mu.Unlock()
	}
}

func (s *schemaVersionSyncer) jobSchemaVerMatchOrSet(jobID int64, matchFn func(map[string]int64) bool) *nodeVersions {
	s.mu.Lock()
	defer s.mu.Unlock()

	item, exists := s.jobNodeVersions[jobID]
	if exists {
		item.matchOrSet(matchFn)
	} else {
		item = newNodeVersions(1, matchFn)
		s.jobNodeVersions[jobID] = item
	}
	return item
}

func decodeJobVersionEvent(kv *mvccpb.KeyValue, tp mvccpb.Event_EventType, prefix string) (jobID int64, tidbID string, schemaVer int64, valid bool) {
	left := strings.TrimPrefix(string(kv.Key), prefix)
	parts := strings.Split(left, "/")
	if len(parts) != 2 {
		return 0, "", 0, false
	}
	jobID, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, "", 0, false
	}
	// there is no Value in DELETE event, so we need to check it.
	if tp == mvccpb.PUT {
		schemaVer, err = strconv.ParseInt(string(kv.Value), 10, 64)
		if err != nil {
			return 0, "", 0, false
		}
	}
	return jobID, parts[1], schemaVer, true
}

func isUpdatedLatestVersion(key, val string, latestVer int64, notMatchVerCnt, intervalCnt int, nodeAlive bool) bool {
	ver, err := strconv.Atoi(val)
	if err != nil {
		logutil.DDLLogger().Info("syncer check all versions, convert value to int failed, continue checking.",
			zap.String("ddl", key), zap.String("value", val), zap.Error(err))
		return false
	}
	if int64(ver) < latestVer && nodeAlive {
		if notMatchVerCnt%intervalCnt == 0 {
			logutil.DDLLogger().Info("syncer check all versions, someone is not synced, continue checking",
				zap.String("ddl", key), zap.Int("currentVer", ver), zap.Int64("latestVer", latestVer))
		}
		return false
	}
	return true
}

func (s *schemaVersionSyncer) Close() {
	err := s.removeSelfVersionPath()
	if err != nil {
		logutil.DDLLogger().Error("remove self version path failed", zap.Error(err))
	}
}
