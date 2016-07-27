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

// locks after 3000ms is considered unusual (the client created the lock might
// be dead). Other client may cleanup this kind of lock.
// For locks created recently, we will do backoff and retry.
var lockTTL uint64 = 3000

// cleanup cleanup the lock
func (l *txnLock) cleanup(bo *Backoff) ([]byte, error) {
	expired, err := l.store.checkTimestampExpiredWithRetry(bo, l.pl.version, lockTTL)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !expired {
		return nil, errors.Trace(errInnerRetryable)
	}
	req := &pb.Request{
		Type: pb.MessageType_CmdCleanup.Enum(),
		CmdCleanupReq: &pb.CmdCleanupRequest{
			Key:          l.pl.key,
			StartVersion: proto.Uint64(l.pl.version),
		},
	}
	for {
		region, err := l.store.regionCache.GetRegion(bo, l.pl.key)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp, err := l.store.SendKVReq(bo, req, region.VerID())
		if err != nil {
			return nil, errors.Trace(err)
		}
		if regionErr := resp.GetRegionError(); regionErr != nil {
			err = bo.Backoff(boRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return nil, errors.Trace(err)
			}
			continue
		}
		cmdCleanupResp := resp.GetCmdCleanupResp()
		if cmdCleanupResp == nil {
			return nil, errors.Trace(errBodyMissing)
		}
		if keyErr := cmdCleanupResp.GetError(); keyErr != nil {
			return nil, errors.Errorf("unexpected cleanup err: %s", keyErr.String())
		}
		if cmdCleanupResp.CommitVersion == nil {
			// cleanup successfully
			return l.rollbackThenGet(bo)
		}
		// already committed
		return l.commitThenGet(bo, cmdCleanupResp.GetCommitVersion())
	}
}

// If key == nil then only rollback but value is nil
func (l *txnLock) rollbackThenGet(bo *Backoff) ([]byte, error) {
	req := &pb.Request{
		Type: pb.MessageType_CmdRollbackThenGet.Enum(),
		CmdRbGetReq: &pb.CmdRollbackThenGetRequest{
			Key:         l.key,
			LockVersion: proto.Uint64(l.pl.version),
		},
	}
	for {
		region, err := l.store.regionCache.GetRegion(bo, l.key)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp, err := l.store.SendKVReq(bo, req, region.VerID())
		if err != nil {
			return nil, errors.Trace(err)
		}
		if regionErr := resp.GetRegionError(); regionErr != nil {
			err = bo.Backoff(boRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return nil, errors.Trace(err)
			}
			continue
		}
		cmdRbGResp := resp.GetCmdRbGetResp()
		if cmdRbGResp == nil {
			return nil, errors.Trace(errBodyMissing)
		}
		if keyErr := cmdRbGResp.GetError(); keyErr != nil {
			return nil, errors.Errorf("unexpected rollback err: %s", keyErr.String())
		}
		return cmdRbGResp.GetValue(), nil
	}
}

// If key == nil then only commit but value is nil
func (l *txnLock) commitThenGet(bo *Backoff, commitVersion uint64) ([]byte, error) {
	req := &pb.Request{
		Type: pb.MessageType_CmdCommitThenGet.Enum(),
		CmdCommitGetReq: &pb.CmdCommitThenGetRequest{
			Key:           l.key,
			LockVersion:   proto.Uint64(l.pl.version),
			CommitVersion: proto.Uint64(commitVersion),
			GetVersion:    proto.Uint64(l.ver),
		},
	}
	for {
		region, err := l.store.regionCache.GetRegion(bo, l.key)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp, err := l.store.SendKVReq(bo, req, region.VerID())
		if err != nil {
			return nil, errors.Trace(err)
		}
		if regionErr := resp.GetRegionError(); regionErr != nil {
			err = bo.Backoff(boRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return nil, errors.Trace(err)
			}
			continue
		}
		cmdCommitGetResp := resp.GetCmdCommitGetResp()
		if cmdCommitGetResp == nil {
			return nil, errors.Trace(errBodyMissing)
		}
		if keyErr := cmdCommitGetResp.GetError(); keyErr != nil {
			return nil, errors.Errorf("unexpected commit err: %s", keyErr.String())
		}
		return cmdCommitGetResp.GetValue(), nil
	}
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
