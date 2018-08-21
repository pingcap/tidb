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
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/owner"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

const (
	// DDLAllSchemaVersions is the path on etcd that is used to store all servers current schema versions.
	// It's exported for testing.
	DDLAllSchemaVersions = "/tidb/ddl/all_schema_versions"
	// DDLGlobalSchemaVersion is the path on etcd that is used to store the latest schema versions.
	// It's exported for testing.
	DDLGlobalSchemaVersion = "/tidb/ddl/global_schema_version"
	// InitialVersion is the initial schema version for every server.
	// It's exported for testing.
	InitialVersion       = "0"
	putKeyNoRetry        = 1
	keyOpDefaultRetryCnt = 3
	putKeyRetryUnlimited = math.MaxInt64
	keyOpDefaultTimeout  = 2 * time.Second
	keyOpRetryInterval   = 30 * time.Millisecond
	checkVersInterval    = 20 * time.Millisecond
)

var (
	// CheckVersFirstWaitTime is a waitting time before the owner checks all the servers of the schema version,
	// and it's an exported variable for testing.
	CheckVersFirstWaitTime = 50 * time.Millisecond
	// SyncerSessionTTL is the etcd session's TTL in seconds.
	// and it's an exported variable for testing.
	SyncerSessionTTL = 10 * 60
	// WaitTimeWhenErrorOccured is waiting interval when processing DDL jobs encounter errors.
	WaitTimeWhenErrorOccured = 1 * time.Second
)

// SchemaSyncer is used to synchronize schema version between the DDL worker leader and followers through etcd.
type SchemaSyncer interface {
	// Init sets the global schema version path to etcd if it isn't exist,
	// then watch this path, and initializes the self schema version to etcd.
	Init(ctx context.Context) error
	// UpdateSelfVersion updates the current version to the self path on etcd.
	UpdateSelfVersion(ctx context.Context, version int64) error
	// RemoveSelfVersionPath remove the self path from etcd.
	RemoveSelfVersionPath() error
	// OwnerUpdateGlobalVersion updates the latest version to the global path on etcd until updating is successful or the ctx is done.
	OwnerUpdateGlobalVersion(ctx context.Context, version int64) error
	// GlobalVersionCh gets the chan for watching global version.
	GlobalVersionCh() clientv3.WatchChan
	// WatchGlobalSchemaVer watches the global schema version.
	WatchGlobalSchemaVer(ctx context.Context)
	// MustGetGlobalVersion gets the global version. The only reason it fails is that ctx is done.
	MustGetGlobalVersion(ctx context.Context) (int64, error)
	// Done returns a channel that closes when the syncer is no longer being refreshed.
	Done() <-chan struct{}
	// Restart restarts the syncer when it's on longer being refreshed.
	Restart(ctx context.Context) error
	// OwnerCheckAllVersions checks whether all followers' schema version are equal to
	// the latest schema version. If the result is false, wait for a while and check again util the processing time reach 2 * lease.
	// It returns until all servers' versions are equal to the latest version or the ctx is done.
	OwnerCheckAllVersions(ctx context.Context, latestVer int64) error
}

type schemaVersionSyncer struct {
	selfSchemaVerPath string
	etcdCli           *clientv3.Client
	session           *concurrency.Session
	mu                struct {
		sync.RWMutex
		globalVerCh clientv3.WatchChan
	}
}

// NewSchemaSyncer creates a new SchemaSyncer.
func NewSchemaSyncer(etcdCli *clientv3.Client, id string) SchemaSyncer {
	return &schemaVersionSyncer{
		etcdCli:           etcdCli,
		selfSchemaVerPath: fmt.Sprintf("%s/%s", DDLAllSchemaVersions, id),
	}
}

// PutKVToEtcd puts key value to etcd.
// etcdCli is client of etcd.
// retryCnt is retry time when an error occurs.
// opts is configures of etcd Operations.
func PutKVToEtcd(ctx context.Context, etcdCli *clientv3.Client, retryCnt int, key, val string,
	opts ...clientv3.OpOption) error {
	var err error
	for i := 0; i < retryCnt; i++ {
		if isContextDone(ctx) {
			return errors.Trace(ctx.Err())
		}

		childCtx, cancel := context.WithTimeout(ctx, keyOpDefaultTimeout)
		_, err = etcdCli.Put(childCtx, key, val, opts...)
		cancel()
		if err == nil {
			return nil
		}
		log.Warnf("[etcd-cli] put key: %s value: %s failed %v no.%d", key, val, err, i)
		time.Sleep(keyOpRetryInterval)
	}
	return errors.Trace(err)
}

// Init implements SchemaSyncer.Init interface.
func (s *schemaVersionSyncer) Init(ctx context.Context) error {
	startTime := time.Now()
	var err error
	defer func() {
		metrics.DeploySyncerHistogram.WithLabelValues(metrics.SyncerInit, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	}()

	_, err = s.etcdCli.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(DDLGlobalSchemaVersion), "=", 0)).
		Then(clientv3.OpPut(DDLGlobalSchemaVersion, InitialVersion)).
		Commit()
	if err != nil {
		return errors.Trace(err)
	}
	logPrefix := fmt.Sprintf("[%s] %s", ddlPrompt, s.selfSchemaVerPath)
	s.session, err = owner.NewSession(ctx, logPrefix, s.etcdCli, owner.NewSessionDefaultRetryCnt, SyncerSessionTTL)
	if err != nil {
		return errors.Trace(err)
	}

	s.mu.Lock()
	s.mu.globalVerCh = s.etcdCli.Watch(ctx, DDLGlobalSchemaVersion)
	s.mu.Unlock()

	err = PutKVToEtcd(ctx, s.etcdCli, keyOpDefaultRetryCnt, s.selfSchemaVerPath, InitialVersion,
		clientv3.WithLease(s.session.Lease()))
	return errors.Trace(err)
}

// Done implements SchemaSyncer.Done interface.
func (s *schemaVersionSyncer) Done() <-chan struct{} {
	return s.session.Done()
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
	session, err := owner.NewSession(ctx, logPrefix, s.etcdCli, owner.NewSessionRetryUnlimited, SyncerSessionTTL)
	if err != nil {
		return errors.Trace(err)
	}
	s.session = session

	childCtx, cancel := context.WithTimeout(ctx, keyOpDefaultTimeout)
	defer cancel()
	err = PutKVToEtcd(childCtx, s.etcdCli, putKeyRetryUnlimited, s.selfSchemaVerPath, InitialVersion,
		clientv3.WithLease(s.session.Lease()))

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
		ch := s.etcdCli.Watch(ctx, DDLGlobalSchemaVersion)

		s.mu.Lock()
		s.mu.globalVerCh = ch
		s.mu.Unlock()
		log.Info("[syncer] watch global schema finished")
	}()
}

// UpdateSelfVersion implements SchemaSyncer.UpdateSelfVersion interface.
func (s *schemaVersionSyncer) UpdateSelfVersion(ctx context.Context, version int64) error {
	startTime := time.Now()
	ver := strconv.FormatInt(version, 10)
	err := PutKVToEtcd(ctx, s.etcdCli, putKeyNoRetry, s.selfSchemaVerPath, ver,
		clientv3.WithLease(s.session.Lease()))

	metrics.UpdateSelfVersionHistogram.WithLabelValues(metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	return errors.Trace(err)
}

// OwnerUpdateGlobalVersion implements SchemaSyncer.OwnerUpdateGlobalVersion interface.
func (s *schemaVersionSyncer) OwnerUpdateGlobalVersion(ctx context.Context, version int64) error {
	startTime := time.Now()
	ver := strconv.FormatInt(version, 10)
	// TODO: If the version is larger than the original global version, we need set the version.
	// Otherwise, we'd better set the original global version.
	err := PutKVToEtcd(ctx, s.etcdCli, putKeyRetryUnlimited, DDLGlobalSchemaVersion, ver)
	metrics.OwnerHandleSyncerHistogram.WithLabelValues(metrics.OwnerUpdateGlobalVersion, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	return errors.Trace(err)
}

// RemoveSelfVersionPath implements SchemaSyncer.RemoveSelfVersionPath interface.
func (s *schemaVersionSyncer) RemoveSelfVersionPath() error {
	startTime := time.Now()
	var err error
	defer func() {
		metrics.DeploySyncerHistogram.WithLabelValues(metrics.SyncerClear, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	}()

	err = DeleteKeyFromEtcd(s.selfSchemaVerPath, s.etcdCli, keyOpDefaultRetryCnt, keyOpDefaultTimeout)
	return errors.Trace(err)
}

// DeleteKeyFromEtcd deletes key value from etcd.
func DeleteKeyFromEtcd(key string, etcdCli *clientv3.Client, retryCnt int, timeout time.Duration) error {
	var err error
	ctx := context.Background()
	for i := 0; i < retryCnt; i++ {
		childCtx, cancel := context.WithTimeout(ctx, timeout)
		_, err = etcdCli.Delete(childCtx, key)
		cancel()
		if err == nil {
			return nil
		}
		log.Warnf("[etcd-cli] delete key %s failed %v no.%d", key, err, i)
	}
	return errors.Trace(err)
}

// MustGetGlobalVersion implements SchemaSyncer.MustGetGlobalVersion interface.
func (s *schemaVersionSyncer) MustGetGlobalVersion(ctx context.Context) (int64, error) {
	startTime := time.Now()
	var err error
	var resp *clientv3.GetResponse
	failedCnt := 0
	intervalCnt := int(time.Second / keyOpRetryInterval)

	defer func() {
		metrics.OwnerHandleSyncerHistogram.WithLabelValues(metrics.OwnerGetGlobalVersion, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	}()
	for {
		if err != nil {
			if failedCnt%intervalCnt == 0 {
				log.Infof("[syncer] get global version failed %v", err)
			}
			time.Sleep(keyOpRetryInterval)
			failedCnt++
		}

		if isContextDone(ctx) {
			err = errors.Trace(ctx.Err())
			return 0, err
		}

		resp, err = s.etcdCli.Get(ctx, DDLGlobalSchemaVersion)
		if err != nil {
			continue
		}
		if len(resp.Kvs) > 0 {
			var ver int
			ver, err = strconv.Atoi(string(resp.Kvs[0].Value))
			if err == nil {
				return int64(ver), nil
			}
		}
	}
}

func isContextDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
	}
	return false
}

// OwnerCheckAllVersions implements SchemaSyncer.OwnerCheckAllVersions interface.
func (s *schemaVersionSyncer) OwnerCheckAllVersions(ctx context.Context, latestVer int64) error {
	startTime := time.Now()
	time.Sleep(CheckVersFirstWaitTime)
	notMatchVerCnt := 0
	intervalCnt := int(time.Second / checkVersInterval)
	updatedMap := make(map[string]struct{})

	var err error
	defer func() {
		metrics.OwnerHandleSyncerHistogram.WithLabelValues(metrics.OwnerGetGlobalVersion, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	}()
	for {
		if isContextDone(ctx) {
			// ctx is canceled or timeout.
			err = errors.Trace(ctx.Err())
			return err
		}

		resp, err := s.etcdCli.Get(ctx, DDLAllSchemaVersions, clientv3.WithPrefix())
		if err != nil {
			log.Infof("[syncer] check all versions failed %v, continue checking.", err)
			continue
		}

		succ := true
		for _, kv := range resp.Kvs {
			if _, ok := updatedMap[string(kv.Key)]; ok {
				continue
			}

			ver, err := strconv.Atoi(string(kv.Value))
			if err != nil {
				log.Infof("[syncer] check all versions, ddl %s convert %v to int failed %v, continue checking.", kv.Key, kv.Value, err)
				succ = false
				break
			}
			if int64(ver) < latestVer {
				if notMatchVerCnt%intervalCnt == 0 {
					log.Infof("[syncer] check all versions, ddl %s is not synced, current ver %v, latest version %v, continue checking",
						kv.Key, ver, latestVer)
				}
				succ = false
				notMatchVerCnt++
				break
			}
			updatedMap[string(kv.Key)] = struct{}{}
		}
		if succ {
			return nil
		}
		time.Sleep(checkVersInterval)
	}
}
