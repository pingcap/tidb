// Copyright 2016 PingCAP, Inc.
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

package tikv

import (
	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
)

type txnLock struct {
	store *tikvStore
	// pl primary lock
	pl  pLock
	key []byte
	ver uint64
}

func newLock(store *tikvStore, pLock []byte, lockVer uint64, key []byte, ver uint64) txnLock {
	return txnLock{
		store: store,
		pl:    newPLock(pLock, lockVer),
		key:   key,
		ver:   ver,
	}
}

// txnLockBackoff is for transaction lock retry.
func txnLockBackoff() func() error {
	const (
		maxRetry  = 6
		sleepBase = 300
		sleepCap  = 3000
	)
	return NewBackoff(maxRetry, sleepBase, sleepCap, EqualJitter)
}

// locks after 3000ms is considered unusual (the client created the lock might
// be dead). Other client may cleanup this kind of lock.
// For locks created recently, we will do backoff and retry.
const lockTTL = 3000

// cleanup cleanup the lock
func (l *txnLock) cleanup() ([]byte, error) {
	expired, err := l.store.checkTimestampExpiredWithRetry(l.pl.version, lockTTL)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !expired {
		return nil, errors.Trace(errInnerRetryable)
	}
	req := &pb.Request{
		Type: pb.MessageType_CmdCleanup.Enum(),
		CmdCleanupReq: &pb.CmdCleanupRequest{
			Row: l.pl.key,
			Ts:  proto.Uint64(l.pl.version),
		},
	}
	var backoffErr error
	for backoff := regionMissBackoff(); backoffErr == nil; backoffErr = backoff() {
		region, err := l.store.regionCache.GetRegion(l.pl.key)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp, err := l.store.SendKVReq(req, region.VerID())
		if err != nil {
			return nil, errors.Trace(err)
		}
		if regionErr := resp.GetRegionError(); regionErr != nil {
			continue
		}
		cmdCleanupResp := resp.GetCmdCleanupResp()
		if cmdCleanupResp == nil {
			return nil, errors.Trace(errBodyMissing)
		}
		if keyErr := cmdCleanupResp.GetError(); keyErr != nil {
			return nil, errors.Errorf("unexpected cleanup err: %s", keyErr.String())
		}
		if cmdCleanupResp.CommitTs == nil {
			// cleanup successfully
			return l.rollbackThenGet()
		}
		// already committed
		return l.commitThenGet(cmdCleanupResp.GetCommitTs())
	}
	return nil, errors.Annotate(backoffErr, txnRetryableMark)
}

// If key == nil then only rollback but value is nil
func (l *txnLock) rollbackThenGet() ([]byte, error) {
	req := &pb.Request{
		Type: pb.MessageType_CmdRollbackThenGet.Enum(),
		CmdRbGetReq: &pb.CmdRollbackThenGetRequest{
			Row: defaultRow(l.key),
			Ts:  proto.Uint64(l.pl.version),
		},
	}
	var backoffErr error
	for backoff := regionMissBackoff(); backoffErr == nil; backoffErr = backoff() {
		region, err := l.store.regionCache.GetRegion(l.key)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp, err := l.store.SendKVReq(req, region.VerID())
		if err != nil {
			return nil, errors.Trace(err)
		}
		if regionErr := resp.GetRegionError(); regionErr != nil {
			continue
		}
		cmdRbGResp := resp.GetCmdRbGetResp()
		if cmdRbGResp == nil {
			return nil, errors.Trace(errBodyMissing)
		}
		rowVal := cmdRbGResp.GetRowValue()
		if rowVal == nil {
			return nil, errors.Trace(errBodyMissing)
		}
		if keyErr := rowVal.GetError(); keyErr != nil {
			return nil, errors.Errorf("unexpected rollback err: %s", keyErr.String())
		}
		return defaultRowValue(rowVal), nil
	}
	return nil, errors.Annotate(backoffErr, txnRetryableMark)
}

// If key == nil then only commit but value is nil
func (l *txnLock) commitThenGet(commitVersion uint64) ([]byte, error) {
	req := &pb.Request{
		Type: pb.MessageType_CmdCommitThenGet.Enum(),
		CmdCommitGetReq: &pb.CmdCommitThenGetRequest{
			Row:      defaultRow(l.key),
			StartTs:  proto.Uint64(l.pl.version),
			CommitTs: proto.Uint64(commitVersion),
			GetTs:    proto.Uint64(l.ver),
		},
	}
	var backoffErr error
	for backoff := regionMissBackoff(); backoffErr == nil; backoffErr = backoff() {
		region, err := l.store.regionCache.GetRegion(l.key)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp, err := l.store.SendKVReq(req, region.VerID())
		if err != nil {
			return nil, errors.Trace(err)
		}
		if regionErr := resp.GetRegionError(); regionErr != nil {
			continue
		}
		cmdCommitGetResp := resp.GetCmdCommitGetResp()
		if cmdCommitGetResp == nil {
			return nil, errors.Trace(errBodyMissing)
		}
		rowVal := cmdCommitGetResp.GetRowValue()
		if rowVal == nil {
			return nil, errors.Trace(errBodyMissing)
		}
		if keyErr := rowVal.GetError(); keyErr != nil {
			return nil, errors.Errorf("unexpected commit err: %s", keyErr.String())
		}
		return defaultRowValue(rowVal), nil
	}
	return nil, errors.Annotate(backoffErr, txnRetryableMark)
}

type pLock struct {
	key     []byte
	version uint64
}

func newPLock(key []byte, ver uint64) pLock {
	return pLock{
		key:     key,
		version: ver,
	}
}
