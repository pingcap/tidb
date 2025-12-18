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
	"context"

	"github.com/pingcap/tidb/pkg/util/logutil"
	pd "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
)

// TableGroupManager manages affinity group settings
type TableGroupManager interface {
	CreateAffinityGroup(ctx context.Context, affinityGroups map[string][]pd.AffinityGroupKeyRange) (map[string]*pd.AffinityGroupState, error)
	GetAllAffinityGroups(ctx context.Context) (map[string]*pd.AffinityGroupState, error)
	AddAffinityGroupKeyRanges(ctx context.Context, groupKeyRanges map[string][]pd.AffinityGroupKeyRange) (map[string]*pd.AffinityGroupState, error)
	RemoveAffinityGroupKeyRanges(ctx context.Context, groupKeyRanges map[string][]pd.AffinityGroupKeyRange) (map[string]*pd.AffinityGroupState, error)
	BatchDeleteAffinityGroups(ctx context.Context, groupIDs []string) error
}

// PDTableGroupManager manages placement with pd
type PDTableGroupManager struct {
	pdHTTPCli pd.Client
}

// CreateAffinityGroup creates affinity groups in PD.
func (m *PDTableGroupManager) CreateAffinityGroup(ctx context.Context, affinityGroups map[string][]pd.AffinityGroupKeyRange) (map[string]*pd.AffinityGroupState, error) {
	if len(affinityGroups) == 0 {
		return nil, nil
	}
	state, err := m.pdHTTPCli.CreateAffinityGroups(ctx, affinityGroups)
	logutil.BgLogger().Info("CreateAffinityGroup", zap.Any("groups", affinityGroups), zap.Any("state", state), zap.Error(err))
	return state, err
}

// GetAllAffinityGroups retrieves all affinity groups from PD.
func (m *PDTableGroupManager) GetAllAffinityGroups(ctx context.Context) (map[string]*pd.AffinityGroupState, error) {
	state, err := m.pdHTTPCli.GetAllAffinityGroups(ctx)
	if err != nil {
		logutil.BgLogger().Warn("GetAllAffinityGroups failed", zap.Error(err))
	} else {
		logutil.BgLogger().Debug("GetAllAffinityGroups success", zap.Any("state", state))
	}
	return state, err
}

// AddAffinityGroupKeyRanges adds key ranges to existing affinity groups in PD.
func (m *PDTableGroupManager) AddAffinityGroupKeyRanges(ctx context.Context, groupKeyRanges map[string][]pd.AffinityGroupKeyRange) (map[string]*pd.AffinityGroupState, error) {
	state, err := m.pdHTTPCli.AddAffinityGroupKeyRanges(ctx, groupKeyRanges)
	logutil.BgLogger().Info("AddAffinityGroupKeyRanges", zap.Any("groups", groupKeyRanges), zap.Any("state", state), zap.Error(err))
	return state, err
}

// RemoveAffinityGroupKeyRanges removes key ranges from affinity groups in PD.
func (m *PDTableGroupManager) RemoveAffinityGroupKeyRanges(ctx context.Context, groupKeyRanges map[string][]pd.AffinityGroupKeyRange) (map[string]*pd.AffinityGroupState, error) {
	state, err := m.pdHTTPCli.RemoveAffinityGroupKeyRanges(ctx, groupKeyRanges)
	logutil.BgLogger().Info("RemoveAffinityGroupKeyRanges", zap.Any("groups", groupKeyRanges), zap.Any("state", state), zap.Error(err))
	return state, err
}

// BatchDeleteAffinityGroups deletes multiple affinity groups by their IDs.
func (m *PDTableGroupManager) BatchDeleteAffinityGroups(ctx context.Context, groupIDs []string) error {
	if len(groupIDs) == 0 {
		return nil
	}
	err := m.pdHTTPCli.BatchDeleteAffinityGroups(ctx, groupIDs, true)
	logutil.BgLogger().Info("BatchDeleteAffinityGroups", zap.Strings("name", groupIDs), zap.Error(err))
	return err
}

type mockTableGroupManager struct {
}

func (m *mockTableGroupManager) CreateAffinityGroup(ctx context.Context, affinityGroups map[string][]pd.AffinityGroupKeyRange) (map[string]*pd.AffinityGroupState, error) {
	return nil, nil
}

func (m *mockTableGroupManager) GetAllAffinityGroups(ctx context.Context) (map[string]*pd.AffinityGroupState, error) {
	return nil, nil
}

func (m *mockTableGroupManager) AddAffinityGroupKeyRanges(ctx context.Context, groupKeyRanges map[string][]pd.AffinityGroupKeyRange) (map[string]*pd.AffinityGroupState, error) {
	return nil, nil
}

func (m *mockTableGroupManager) RemoveAffinityGroupKeyRanges(ctx context.Context, groupKeyRanges map[string][]pd.AffinityGroupKeyRange) (map[string]*pd.AffinityGroupState, error) {
	return nil, nil
}

func (m *mockTableGroupManager) BatchDeleteAffinityGroups(ctx context.Context, groupIDs []string) error {
	return nil
}
