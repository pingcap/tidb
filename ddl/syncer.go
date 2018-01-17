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
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/owner"
	log "github.com/sirupsen/logrus"
	goctx "golang.org/x/net/context"
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
)

// SchemaSyncer is used to synchronize schema version between the DDL worker leader and followers through etcd.
type SchemaSyncer interface {
	// Init sets the global schema version path to etcd if it isn't exist,
	// then watch this path, and initializes the self schema version to etcd.
	Init(ctx goctx.Context) error
	// UpdateSelfVersion updates the current version to the self path on etcd.
	UpdateSelfVersion(ctx goctx.Context, version int64) error
	// RemoveSelfVersionPath remove the self path from etcd.
	RemoveSelfVersionPath() error
	// OwnerUpdateGlobalVersion updates the latest version to the global path on etcd until updating is successful or the ctx is done.
	OwnerUpdateGlobalVersion(ctx goctx.Context, version int64) error
	// GlobalVersionCh gets the chan for watching global version.
	GlobalVersionCh() clientv3.WatchChan
	// MustGetGlobalVersion gets the global version. The only reason it fails is that ctx is done.
	MustGetGlobalVersion(ctx goctx.Context) (int64, error)
	// Done() returns a channel that closes when the syncer is no longer being refreshed.
	Done() <-chan struct{}
	// Restart restarts the syncer when it's on longer being refreshed.
	Restart(ctx goctx.Context) error
	// OwnerCheckAllVersions checks whether all followers' schema version are equal to
	// the latest schema version. If the result is false, wait for a while and check again util the processing time reach 2 * lease.
	// It returns until all servers' versions are equal to the latest version or the ctx is done.
	OwnerCheckAllVersions(ctx goctx.Context, latestVer int64) error
}

type schemaVersionSyncer struct {
	selfSchemaVerPath string
	etcdCli           *clientv3.Client
	session           *concurrency.Session
	globalVerCh       clientv3.WatchChan
}

// NewSchemaSyncer creates a new SchemaSyncer.
func NewSchemaSyncer(etcdCli *clientv3.Client, id string) SchemaSyncer {
	return &schemaVersionSyncer{
		etcdCli:           etcdCli,
		selfSchemaVerPath: fmt.Sprintf("%s/%s", DDLAllSchemaVersions, id),
	}
}

func (s *schemaVersionSyncer) putKV(ctx goctx.Context, retryCnt int, key, val string,
	opts ...clientv3.OpOption) error {
	var err error
	for i := 0; i < retryCnt; i++ {
		if isContextDone(ctx) {
			return errors.Trace(ctx.Err())
		}

		childCtx, cancel := goctx.WithTimeout(ctx, keyOpDefaultTimeout)
		_, err = s.etcdCli.Put(childCtx, key, val, opts...)
		cancel()
		if err == nil {
			return nil
		}
		log.Warnf("[syncer] put schema version %s failed %v no.%d", val, err, i)
		time.Sleep(keyOpRetryInterval)
	}
	return errors.Trace(err)
}

// Init implements SchemaSyncer.Init interface.
func (s *schemaVersionSyncer) Init(ctx goctx.Context) error {
	_, err := s.etcdCli.Txn(ctx).
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
	s.globalVerCh = s.etcdCli.Watch(ctx, DDLGlobalSchemaVersion)
	return s.putKV(ctx, keyOpDefaultRetryCnt, s.selfSchemaVerPath, InitialVersion,
		clientv3.WithLease(s.session.Lease()))
}

// Done implements SchemaSyncer.Done interface.
func (s *schemaVersionSyncer) Done() <-chan struct{} {
	return s.session.Done()
}

// Restart implements SchemaSyncer.Restart interface.
func (s *schemaVersionSyncer) Restart(ctx goctx.Context) error {
	logPrefix := fmt.Sprintf("[%s] %s", ddlPrompt, s.selfSchemaVerPath)

	// NewSession's context will affect the exit of the session.
	session, err := owner.NewSession(ctx, logPrefix, s.etcdCli, owner.NewSessionRetryUnlimited, SyncerSessionTTL)
	if err != nil {
		return errors.Trace(err)
	}
	s.session = session

	childCtx, cancel := goctx.WithTimeout(ctx, keyOpDefaultTimeout)
	defer cancel()
	return s.putKV(childCtx, putKeyRetryUnlimited, s.selfSchemaVerPath, InitialVersion,
		clientv3.WithLease(s.session.Lease()))
}

// GlobalVersionCh implements SchemaSyncer.GlobalVersionCh interface.
func (s *schemaVersionSyncer) GlobalVersionCh() clientv3.WatchChan {
	return s.globalVerCh
}

// UpdateSelfVersion implements SchemaSyncer.UpdateSelfVersion interface.
func (s *schemaVersionSyncer) UpdateSelfVersion(ctx goctx.Context, version int64) error {
	ver := strconv.FormatInt(version, 10)
	return s.putKV(ctx, putKeyNoRetry, s.selfSchemaVerPath, ver,
		clientv3.WithLease(s.session.Lease()))
}

// OwnerUpdateGlobalVersion implements SchemaSyncer.OwnerUpdateGlobalVersion interface.
func (s *schemaVersionSyncer) OwnerUpdateGlobalVersion(ctx goctx.Context, version int64) error {
	ver := strconv.FormatInt(version, 10)
	return s.putKV(ctx, putKeyRetryUnlimited, DDLGlobalSchemaVersion, ver)
}

// RemoveSelfVersionPath implements SchemaSyncer.RemoveSelfVersionPath interface.
func (s *schemaVersionSyncer) RemoveSelfVersionPath() error {
	ctx := goctx.Background()
	var err error
	for i := 0; i < keyOpDefaultRetryCnt; i++ {
		childCtx, cancel := goctx.WithTimeout(ctx, keyOpDefaultTimeout)
		_, err = s.etcdCli.Delete(childCtx, s.selfSchemaVerPath)
		cancel()
		if err == nil {
			return nil
		}
		log.Warnf("remove schema version path %s failed %v no.%d", s.selfSchemaVerPath, err, i)
	}
	return errors.Trace(err)
}

// MustGetGlobalVersion implements SchemaSyncer.MustGetGlobalVersion interface.
func (s *schemaVersionSyncer) MustGetGlobalVersion(ctx goctx.Context) (int64, error) {
	var err error
	var resp *clientv3.GetResponse
	failedCnt := 0
	intervalCnt := int(time.Second / keyOpRetryInterval)
	for {
		if err != nil {
			if failedCnt%intervalCnt == 0 {
				log.Infof("[syncer] get global version failed %v", err)
			}
			time.Sleep(keyOpRetryInterval)
			failedCnt++
		}

		if isContextDone(ctx) {
			return 0, errors.Trace(ctx.Err())
		}

		resp, err = s.etcdCli.Get(ctx, DDLGlobalSchemaVersion)
		if err != nil {
			continue
		}
		if err == nil && len(resp.Kvs) > 0 {
			var ver int
			ver, err = strconv.Atoi(string(resp.Kvs[0].Value))
			if err == nil {
				return int64(ver), nil
			}
		}
	}
}

func isContextDone(ctx goctx.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
	}
	return false
}

// OwnerCheckAllVersions implements SchemaSyncer.OwnerCheckAllVersions interface.
func (s *schemaVersionSyncer) OwnerCheckAllVersions(ctx goctx.Context, latestVer int64) error {
	time.Sleep(CheckVersFirstWaitTime)
	notMatchVerCnt := 0
	intervalCnt := int(time.Second / checkVersInterval)
	updatedMap := make(map[string]struct{})
	for {
		if isContextDone(ctx) {
			return errors.Trace(ctx.Err())
		}

		resp, err := s.etcdCli.Get(ctx, DDLAllSchemaVersions, clientv3.WithPrefix())
		if err != nil {
			log.Infof("[syncer] check all versions failed %v", err)
			continue
		}

		succ := true
		for _, kv := range resp.Kvs {
			if _, ok := updatedMap[string(kv.Key)]; ok {
				continue
			}

			ver, err := strconv.Atoi(string(kv.Value))
			if err != nil {
				log.Infof("[syncer] check all versions, ddl %s convert %v to int failed %v", kv.Key, kv.Value, err)
				succ = false
				break
			}
			if int64(ver) != latestVer {
				if notMatchVerCnt%intervalCnt == 0 {
					log.Infof("[syncer] check all versions, ddl %s current ver %v, latest version %v",
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
