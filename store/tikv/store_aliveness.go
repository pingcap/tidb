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

package tikv

import (
	"context"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
	"time"
)

// StoreLiveness maintains the liveness of store.
type StoreLiveness struct {
	cli     Client
	sf      singleflight.Group
	timeout time.Duration
}

// NewStoreLiveness creates a store liveness.
func NewStoreLiveness(cli Client, timeout time.Duration) *StoreLiveness {
	return &StoreLiveness{
		cli:     cli,
		timeout: timeout,
	}
}

// IsAlive checks whether store process still alive.
func (s *StoreLiveness) IsAlive(store string) bool {
	if s == nil {
		return false
	}
	req := tikvrpc.NewRequest(tikvrpc.CmdEmpty, &tikvpb.BatchCommandsEmptyRequest{})
	req.ByPassSuperBatch = true
	_, err, _ := s.sf.Do(store, func() (interface{}, error) {
		return s.cli.SendRequest(context.Background(), store, req, s.timeout)
	})
	if err != nil {
		logutil.BgLogger().Warn("send fail and store is down, need invalid region cache and switch next peer",
			zap.String("store", store), zap.Error(err))
		return false
	}
	return true
}
