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

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/session"
	kvstore "github.com/pingcap/tidb/store"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const SharedServiceKeyPrefix = "/tidb/shared-service"
const SharedServiceClustersPrefix = SharedServiceKeyPrefix + "/clusters/"

func GetServiceKey(id string, service ServiceTp) string {
	return fmt.Sprintf(SharedServiceClustersPrefix+"%s/%s", id, service)
}

func GetClusterDomainKey(id string) string {
	return fmt.Sprintf(SharedServiceClustersPrefix+"%s/domain", id)
}

func GetDomainUUID(addr string) (uuid string, err error) {
	store, err := kvstore.New(addr)
	if err != nil {
		return "", err
	}

	defer func() {
		err = store.Close()
	}()

	return store.UUID(), nil
}

type DomainInfo struct {
	Addr string `json:"addr"`
}

type ServiceTp string

type ServiceDesc struct {
	Tp     ServiceTp   `json:"type"`
	Domain *DomainInfo `json:"domain"`
}

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
	ticker := time.NewTicker(time.Second * 60)
	watch := m.etcd.Watch(m.ctx, SharedServiceClustersPrefix, clientv3.WithPrefix())
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

	domains := make(map[string]*DomainInfo)
	domainServices := make(map[string][]ServiceTp)

	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		suffix := key[len(SharedServiceClustersPrefix):]
		items := strings.SplitN(suffix, "/", 2)
		if len(items) < 2 {
			log.Warn("invalid service key", zap.String("key", key))
			continue
		}
		serviceID := items[0]
		if items[1] == "domain" {
			var domainInfo DomainInfo
			if err = json.Unmarshal(kv.Value, &domainInfo); err != nil {
				log.Warn("invalid domain info", zap.ByteString("key", kv.Value))
			}
			domains[serviceID] = &domainInfo
		} else {
			srvTp := ServiceTp(items[1])
			domainServices[serviceID] = append(domainServices[serviceID], srvTp)
		}
	}

	for serviceID, domainInfo := range domains {
		services, ok := domainServices[serviceID]
		if !ok {
			continue
		}

		dom, err := m.registerLocalDomain(domainInfo.Addr)
		if err != nil {
			log.Warn("register domain failed", zap.String("addr", domainInfo.Addr))
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

func (m *ServicesManager) registerServiceInfo(srv *ServiceDesc) error {
	serviceID, err := GetDomainUUID(srv.Domain.Addr)
	if err != nil {
		return nil
	}

	data, err := json.Marshal(srv.Domain)
	if err != nil {
		return err
	}

	_, err = m.etcd.Put(m.ctx, GetClusterDomainKey(serviceID), string(data))
	if err != nil {
		return err
	}

	_, err = m.etcd.Put(m.ctx, GetServiceKey(serviceID, srv.Tp), "")
	if err != nil {
		return err
	}

	return nil
}
