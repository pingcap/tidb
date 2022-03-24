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
	"sync"
	"time"

	"github.com/pingcap/tidb/domain"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/session"
	kvstore "github.com/pingcap/tidb/store"
)

type DomainDesc struct {
	Addr string `json:"addr"`
}

type ServiceTp string

type ServiceDesc struct {
	Tp     ServiceTp   `json:"type"`
	Domain *DomainDesc `json:"domain"`
}

const (
	ServiceTpDDL ServiceTp = "ddl"
)

type ServicesManager struct {
	mu sync.Mutex
	// self domain
	do *domain.Domain
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
	return nil
}

func (m *ServicesManager) manage() {
	ticker := time.NewTicker(time.Second * 10)
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (m *ServicesManager) doRegisterService(srv *ServiceDesc) error {
	store, err := kvstore.New(srv.Domain.Addr)
	if err != nil {
		return err
	}

	dom, err := m.domains.Register(store)
	if err != nil {
		return err
	}

	switch srv.Tp {
	case ServiceTpDDL:
		err = dom.DDL().EnableWorker()
	default:
		err = errors.Errorf("Unsupported service: %s", srv.Tp)
	}

	return err
}
