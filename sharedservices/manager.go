// Copyright 2022 PingCAP, Inc.
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

package sharedservices

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/session"
	kvstore "github.com/pingcap/tidb/store"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const SharedServiceKeyPrefix = "/tidb/shared-service"
const SharedServiceClustersPrefix = SharedServiceKeyPrefix + "/clusters/"

func GetClusterServiceKey(clusterID string, service ServiceTp) string {
	return fmt.Sprintf(SharedServiceClustersPrefix+"%s/%s", clusterID, service)
}

func GetClusterInfoKey(clusterID string) string {
	return fmt.Sprintf(SharedServiceClustersPrefix+"%s/info", clusterID)
}

func CheckDomainAddr(addr string) (err error) {
	store, err := kvstore.New(addr)
	if err != nil {
		return err
	}

	defer func() {
		err = store.Close()
	}()

	return nil
}

type ClusterInfo struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}

type ServiceTp string

const (
	ServiceTpDDL ServiceTp = "ddl"
)

type ServicesManager struct {
	mu   sync.Mutex
	etcd *clientv3.Client
	// domains to manage
	domains *session.DomainMap
	ctx     context.Context
	cancel  func()
}

func (m *ServicesManager) Start() error {
	go m.manage()
	return nil
}

func (m *ServicesManager) Stop() error {
	m.cancel()
	_ = m.etcd.Close()
	return nil
}

func (m *ServicesManager) manage() {
	ticker := time.NewTicker(time.Second)
	watch := m.etcd.Watch(m.ctx, SharedServiceClustersPrefix, clientv3.WithPrefix())
	lastSyncTime := int64(0)
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-watch:
			log.Info("etcd values changed, do sync")
			if err := m.sync(); err != nil {
				log.Error("sync failed")
			}
		case <-ticker.C:
			now := time.Now().Unix()
			if now < lastSyncTime || now-lastSyncTime < 60 {
				continue
			}
			lastSyncTime = now

			if err := m.sync(); err != nil {
				log.Error("sync failed")
			}
		}
	}
}

func (m *ServicesManager) sync() error {
	resp, err := m.etcd.Get(m.ctx, SharedServiceClustersPrefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	clusters := make(map[string]*ClusterInfo)
	clusterServices := make(map[string][]ServiceTp)

	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		suffix := key[len(SharedServiceClustersPrefix):]
		items := strings.SplitN(suffix, "/", 2)
		if len(items) < 2 {
			log.Warn("invalid service key", zap.String("key", key))
			continue
		}
		serviceID := items[0]
		if items[1] == "info" {
			var info ClusterInfo
			if err = json.Unmarshal(kv.Value, &info); err != nil {
				log.Warn("invalid domain info", zap.ByteString("key", kv.Value))
			}
			clusters[serviceID] = &info
		} else {
			srvTp := ServiceTp(items[1])
			clusterServices[serviceID] = append(clusterServices[serviceID], srvTp)
		}
	}

	for clusterID, clusterInfo := range clusters {
		services, ok := clusterServices[clusterID]
		if !ok {
			continue
		}

		dom, err := m.registerLocalDomain(clusterInfo.Addr)
		if err != nil {
			log.Warn("register domain failed", zap.String("addr", clusterInfo.Addr))
		}

		for _, srvTp := range services {
			switch srvTp {
			case ServiceTpDDL:
				err = dom.DDL().EnableWorker()
			default:
				log.Warn("Unsupported service", zap.Any("serviceTp", srvTp))
			}
		}
	}

	return nil
}

func (m *ServicesManager) registerLocalDomain(addr string) (*domain.Domain, error) {
	store, err := kvstore.New(addr)
	if err != nil {
		return nil, err
	}

	dom, err := m.domains.Register(store)
	if err != nil {
		_ = store.Close()
		return nil, err
	}

	return dom, nil
}

func (m *ServicesManager) RegisterCluster(info *ClusterInfo) error {
	data, err := json.Marshal(info)
	if err != nil {
		return err
	}

	if info.ID == "" {
		return errors.New("id cannot be empty")
	}

	if err := CheckDomainAddr(info.Addr); err != nil {
		return err
	}

	key := GetClusterInfoKey(info.ID)
	resp, err := m.etcd.Txn(m.ctx).If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).Then(
		clientv3.OpPut(key, string(data)),
	).Commit()

	if err != nil {
		return err
	}

	if !resp.Succeeded {
		return errors.New("cluster: " + info.ID + " registered")
	}

	return nil
}

func (m *ServicesManager) EnableClusterService(clusterID string, serviceTp ServiceTp) error {
	if clusterID == "" {
		return errors.New("clusterID cannot be empty")
	}

	switch serviceTp {
	case ServiceTpDDL:
	default:
		return errors.New("Unknown service: " + string(serviceTp))
	}

	resp, err := m.etcd.Get(m.ctx, GetClusterInfoKey(clusterID))
	if err != nil {
		return err
	}

	if resp.Count == 0 {
		return errors.New("Cluster id not registered: " + clusterID)
	}

	_, err = m.etcd.Put(m.ctx, GetClusterServiceKey(clusterID, serviceTp), "")
	return err
}
