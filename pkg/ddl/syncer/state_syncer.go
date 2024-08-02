// Copyright 2023 PingCAP, Inc.
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

package syncer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/metrics"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	atomicutil "go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	// keyOpDefaultTimeout is the default time out for etcd store.
	keyOpDefaultTimeout = 1 * time.Second
	statePrompt         = "global-state-syncer"
	// StateUpgrading represents the cluster global state is upgrading. It is exports for testing.
	StateUpgrading = "upgrading"
	// StateNormalRunning represents the cluster global state is normal running. It is exports for testing.
	StateNormalRunning = ""
)

// StateSyncer is used to synchronize schema version between the DDL worker leader and followers through etcd.
type StateSyncer interface {
	// Init sets the global schema version path to etcd if it isn't exist,
	// then watch this path, and initializes the self schema version to etcd.
	Init(ctx context.Context) error
	// UpdateGlobalState updates the latest version to the global path on etcd until updating is successful or the ctx is done.
	UpdateGlobalState(ctx context.Context, stateInfo *StateInfo) error
	// GetGlobalState gets the global state from etcd.
	GetGlobalState(ctx context.Context) (*StateInfo, error)
	// IsUpgradingState returns whether the cluster state is upgrading.
	IsUpgradingState() bool
	WatchChan() clientv3.WatchChan
	Rewatch(ctx context.Context)
}

// StateInfo is the tidb cluster state.
// It will not be updated when the tidb cluster upgrading.
type StateInfo struct {
	State string `json:"state"`
}

// NewStateInfo is new a StateInfo.
func NewStateInfo(state string) *StateInfo {
	return &StateInfo{State: state}
}

// Marshal `StateInfo` into bytes.
func (info *StateInfo) Marshal() ([]byte, error) {
	infoBuf, err := json.Marshal(info)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return infoBuf, nil
}

// Unmarshal `StateInfo` from bytes.
func (info *StateInfo) Unmarshal(v []byte) error {
	return json.Unmarshal(v, info)
}

type serverStateSyncer struct {
	etcdPath           string
	prompt             string
	etcdCli            *clientv3.Client
	session            *concurrency.Session
	clusterState       *atomicutil.Pointer[StateInfo]
	globalStateWatcher watcher
}

// NewStateSyncer creates a new StateSyncer.
func NewStateSyncer(etcdCli *clientv3.Client, etcdPath string) StateSyncer {
	return &serverStateSyncer{
		etcdCli:      etcdCli,
		etcdPath:     etcdPath,
		clusterState: atomicutil.NewPointer(NewStateInfo(StateNormalRunning)),
		prompt:       statePrompt,
	}
}

// Init implements StateSyncer.Init interface.
func (s *serverStateSyncer) Init(ctx context.Context) error {
	startTime := time.Now()
	var err error
	defer func() {
		metrics.DeploySyncerHistogram.WithLabelValues(metrics.StateSyncerInit, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	}()

	logPrefix := fmt.Sprintf("[%s] %s", s.prompt, s.etcdPath)
	s.session, err = tidbutil.NewSession(ctx, logPrefix, s.etcdCli, tidbutil.NewSessionDefaultRetryCnt, util.SessionTTL)
	if err != nil {
		return errors.Trace(err)
	}

	clusterState, err := s.GetGlobalState(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	s.clusterState.Store(clusterState)
	s.globalStateWatcher.Watch(ctx, s.etcdCli, s.etcdPath)

	return errors.Trace(err)
}

// WatchChan implements StateSyncer.WatchChan interface.
func (s *serverStateSyncer) WatchChan() clientv3.WatchChan {
	return s.globalStateWatcher.WatchChan()
}

// Rewatch implements StateSyncer.Rewatch interface.
func (s *serverStateSyncer) Rewatch(ctx context.Context) {
	s.globalStateWatcher.Rewatch(ctx, s.etcdCli, s.etcdPath)
}

// IsUpgradingState implements StateSyncer.IsUpgradingState interface.
func (s *serverStateSyncer) IsUpgradingState() bool {
	return s.clusterState.Load().State == StateUpgrading
}

func (*serverStateSyncer) getKeyValue(ctx context.Context, etcdCli *clientv3.Client, key string, retryCnt int, timeout time.Duration, opts ...clientv3.OpOption) ([]*mvccpb.KeyValue, error) {
	var err error
	var resp *clientv3.GetResponse
	for i := 0; i < retryCnt; i++ {
		select {
		case <-ctx.Done():
			err = errors.Trace(ctx.Err())
			return nil, err
		default:
		}

		childCtx, cancel := context.WithTimeout(ctx, timeout)
		resp, err = etcdCli.Get(childCtx, key, opts...)
		cancel()
		if err != nil {
			logutil.DDLLogger().Info("get key failed", zap.String("key", key), zap.Error(err))
			time.Sleep(200 * time.Millisecond)
			continue
		}
		if len(resp.Kvs) == 0 {
			return nil, nil
		}
		return resp.Kvs, nil
	}
	return nil, errors.Trace(err)
}

// GetGlobalState implements StateSyncer.GetGlobalState interface.
func (s *serverStateSyncer) GetGlobalState(ctx context.Context) (*StateInfo, error) {
	startTime := time.Now()
	kvs, err := s.getKeyValue(ctx, s.etcdCli, s.etcdPath, keyOpDefaultRetryCnt, keyOpDefaultTimeout)
	if err != nil {
		return nil, errors.Trace(err)
	}
	state := &StateInfo{}
	if len(kvs) != 1 {
		if len(kvs) > 0 {
			return nil, errors.Errorf("get key value count:%d wrong", len(kvs))
		}
		s.clusterState.Store(state)
		return state, nil
	}
	err = state.Unmarshal(kvs[0].Value)
	if err != nil {
		logutil.DDLLogger().Warn("get global state failed", zap.String("key", s.etcdPath), zap.ByteString("value", kvs[0].Value), zap.Error(err))
		return nil, errors.Trace(err)
	}
	s.clusterState.Store(state)
	metrics.OwnerHandleSyncerHistogram.WithLabelValues(metrics.UpdateGlobalState, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	return state, nil
}

// UpdateGlobalState implements StateSyncer.UpdateGlobalState interface.
func (s *serverStateSyncer) UpdateGlobalState(ctx context.Context, stateInfo *StateInfo) error {
	startTime := time.Now()
	stateStr, err := stateInfo.Marshal()
	if err != nil {
		return err
	}
	err = util.PutKVToEtcd(ctx, s.etcdCli, keyOpDefaultRetryCnt, s.etcdPath, string(stateStr))
	metrics.OwnerHandleSyncerHistogram.WithLabelValues(metrics.UpdateGlobalState, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	return errors.Trace(err)
}
