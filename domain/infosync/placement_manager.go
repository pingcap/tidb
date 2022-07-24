// Copyright 2021 PingCAP, Inc.
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

package infosync

import (
	"bytes"
	"context"
	"encoding/json"
	"path"
	"sync"

	"github.com/pingcap/tidb/ddl/placement"
	"github.com/pingcap/tidb/util/pdapi"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// PlacementManager manages placement settings
type PlacementManager interface {
	// GetRuleBundle is used to get one specific rule bundle from PD.
	GetRuleBundle(ctx context.Context, name string) (*placement.Bundle, error)
	// GetAllRuleBundles is used to get all rule bundles from PD. It is used to load full rules from PD while fullload infoschema.
	GetAllRuleBundles(ctx context.Context) ([]*placement.Bundle, error)
	// PutRuleBundles is used to post specific rule bundles to PD.
	PutRuleBundles(ctx context.Context, bundles []*placement.Bundle) error
}

// PDPlacementManager manages placement with pd
type PDPlacementManager struct {
	etcdCli *clientv3.Client
}

// GetRuleBundle is used to get one specific rule bundle from PD.
func (m *PDPlacementManager) GetRuleBundle(ctx context.Context, name string) (*placement.Bundle, error) {
	bundle := &placement.Bundle{ID: name}
	res, err := doRequest(ctx, "GetPlacementRule", m.etcdCli.Endpoints(), path.Join(pdapi.Config, "placement-rule", name), "GET", nil)
	if err == nil && res != nil {
		err = json.Unmarshal(res, bundle)
	}
	return bundle, err
}

// GetAllRuleBundles is used to get all rule bundles from PD. It is used to load full rules from PD while fullload infoschema.
func (m *PDPlacementManager) GetAllRuleBundles(ctx context.Context) ([]*placement.Bundle, error) {
	var bundles []*placement.Bundle
	res, err := doRequest(ctx, "GetAllPlacementRules", m.etcdCli.Endpoints(), path.Join(pdapi.Config, "placement-rule"), "GET", nil)
	if err == nil && res != nil {
		err = json.Unmarshal(res, &bundles)
	}
	return bundles, err
}

// PutRuleBundles is used to post specific rule bundles to PD.
func (m *PDPlacementManager) PutRuleBundles(ctx context.Context, bundles []*placement.Bundle) error {
	if len(bundles) == 0 {
		return nil
	}

	b, err := json.Marshal(bundles)
	if err != nil {
		return err
	}

	_, err = doRequest(ctx, "PutPlacementRules", m.etcdCli.Endpoints(), path.Join(pdapi.Config, "placement-rule")+"?partial=true", "POST", bytes.NewReader(b))
	return err
}

type mockPlacementManager struct {
	sync.Mutex
	bundles map[string]*placement.Bundle
}

func (m *mockPlacementManager) GetRuleBundle(_ context.Context, name string) (*placement.Bundle, error) {
	m.Lock()
	defer m.Unlock()

	if bundle, ok := m.bundles[name]; ok {
		return bundle, nil
	}

	return &placement.Bundle{ID: name}, nil
}

func (m *mockPlacementManager) GetAllRuleBundles(_ context.Context) ([]*placement.Bundle, error) {
	m.Lock()
	defer m.Unlock()

	bundles := make([]*placement.Bundle, 0, len(m.bundles))
	for _, bundle := range m.bundles {
		bundles = append(bundles, bundle)
	}
	return bundles, nil
}

func (m *mockPlacementManager) PutRuleBundles(_ context.Context, bundles []*placement.Bundle) error {
	m.Lock()
	defer m.Unlock()

	if m.bundles == nil {
		m.bundles = make(map[string]*placement.Bundle)
	}

	for _, bundle := range bundles {
		if bundle.IsEmpty() {
			delete(m.bundles, bundle.ID)
		} else {
			m.bundles[bundle.ID] = bundle
		}
	}

	return nil
}
