// Copyright 2020 PingCAP, Inc.
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

// Package tikv provides tcp connection to kvserver.
package tikv

import (
	"context"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"golang.org/x/sync/singleflight"
)

var _ Client = reqCollapse{}

var resolveRegionSf singleflight.Group

type reqCollapse struct {
	Client
}

func (r reqCollapse) Close() error {
	if r.Client == nil {
		panic("client should not be nil")
	}
	return r.Client.Close()
}

func (r reqCollapse) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	if r.Client == nil {
		panic("client should not be nil")
	}
	if canCollapse, resp, err := r.tryCollapseRequest(ctx, addr, req, timeout); canCollapse {
		return resp, err
	}
	return r.Client.SendRequest(ctx, addr, req, timeout)
}

func (r reqCollapse) tryCollapseRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (canCollapse bool, resp *tikvrpc.Response, err error) {
	switch req.Type {
	case tikvrpc.CmdResolveLock:
		resolveLock := req.ResolveLock()
		if len(resolveLock.Keys) > 0 {
			// can not collapse resolve lock lite
			return
		}
		if len(resolveLock.TxnInfos) > 0 {
			// can not collapse batch resolve locks which is only used by GC worker.
			return
		}
		canCollapse = true
		key := strconv.FormatUint(resolveLock.Context.RegionId, 10) + "-" + strconv.FormatUint(resolveLock.StartVersion, 10)
		resp, err = r.collapse(ctx, key, &resolveRegionSf, addr, req, timeout)
		return
	default:
		// now we only support collapse resolve lock.
		return
	}
}

func (r reqCollapse) collapse(ctx context.Context, key string, sf *singleflight.Group,
	addr string, req *tikvrpc.Request, timeout time.Duration) (resp *tikvrpc.Response, err error) {
	rsC := sf.DoChan(key, func() (interface{}, error) {
		return r.Client.SendRequest(context.Background(), addr, req, readTimeoutShort) // use resolveLock timeout.
	})
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		err = errors.Trace(ctx.Err())
		return
	case <-timer.C:
		err = errors.Trace(context.DeadlineExceeded)
		return
	case rs := <-rsC:
		if rs.Err != nil {
			err = errors.Trace(rs.Err)
			return
		}
		resp = rs.Val.(*tikvrpc.Response)
		return
	}
}
