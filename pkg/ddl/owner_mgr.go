// Copyright 2024 PingCAP, Inc.
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

package ddl

import (
	"context"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/owner"
	storepkg "github.com/pingcap/tidb/pkg/store"
	"github.com/pingcap/tidb/pkg/util/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

var globalOwnerManager = &ownerManager{}

// StartOwnerManager starts a global DDL owner manager.
func StartOwnerManager(ctx context.Context, store kv.Storage) error {
	return globalOwnerManager.Start(ctx, store)
}

// CloseOwnerManager closes the global DDL owner manager.
func CloseOwnerManager() {
	globalOwnerManager.Close()
}

// ownerManager is used to manage lifecycle of a global DDL owner manager which
// we only want it to init session once, to avoid DDL owner change after upgrade.
type ownerManager struct {
	etcdCli  *clientv3.Client
	id       string
	ownerMgr owner.Manager
	started  bool
}

// Start starts the TiDBInstance.
func (om *ownerManager) Start(ctx context.Context, store kv.Storage) error {
	// BR might start domain multiple times, we need to avoid it. when BR have refactored
	// this part, we can remove this.
	if om.started {
		return nil
	}
	if config.GetGlobalConfig().Store != "tikv" {
		return nil
	}
	cli, err := storepkg.NewEtcdCli(store)
	if err != nil {
		return errors.Trace(err)
	}
	failpoint.InjectCall("injectEtcdClient", &cli)
	if cli == nil {
		return errors.New("etcd client is nil, maybe the server is not started with PD")
	}
	om.id = uuid.New().String()
	om.etcdCli = cli
	om.ownerMgr = owner.NewOwnerManager(ctx, om.etcdCli, Prompt, om.id, DDLOwnerKey)
	om.started = true
	return nil
}

// Close closes the TiDBInstance.
func (om *ownerManager) Close() {
	if om.ownerMgr != nil {
		om.ownerMgr.Close()
	}
	if om.etcdCli != nil {
		if err := om.etcdCli.Close(); err != nil {
			logutil.BgLogger().Error("close etcd client failed", zap.Error(err))
		}
	}
	om.started = false
}

func (om *ownerManager) ID() string {
	return om.id
}

func (om *ownerManager) OwnerManager() owner.Manager {
	return om.ownerMgr
}
