// Copyright 2025 PingCAP, Inc.
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

package affinity

import (
	"context"
	"sync"

	pdhttp "github.com/tikv/pd/client/http"
)

// Manager manages affinity groups with PD.
type Manager interface {
	CreateAffinityGroupsIfNotExists(ctx context.Context, groups map[string][]pdhttp.AffinityGroupKeyRange) error
	DeleteAffinityGroups(ctx context.Context, ids []string) error
	GetAffinityGroups(ctx context.Context, ids []string) (map[string]*pdhttp.AffinityGroupState, error)
}

type pdManager struct {
	pdhttp.Client
}

// TODO: remove these interface shims after the pdhttp.Client includes these methods directly.
type affinityGroupsCreator interface {
	CreateAffinityGroupsWithSkipExistCheck(ctx context.Context, affinityGroups map[string][]pdhttp.AffinityGroupKeyRange) (map[string]*pdhttp.AffinityGroupState, error)
}

type affinityGroupsGetter interface {
	GetAffinityGroups(ctx context.Context, ids []string) (map[string]*pdhttp.AffinityGroupState, error)
}

const (
	maxAffinityGroupIDsQueryLen = 4096
	maxAffinityGroupIDsCount    = 100
)

// NewPDManager creates a new affinity manager that uses PD HTTP client.
func NewPDManager(client pdhttp.Client) Manager {
	return &pdManager{client}
}

// CreateAffinityGroupsIfNotExists creates affinity groups in PD.
// It checks which groups already exist and only creates the ones that don't exist.
// This makes the operation safe for DDL job retries.
func (m *pdManager) CreateAffinityGroupsIfNotExists(ctx context.Context, groups map[string][]pdhttp.AffinityGroupKeyRange) error {
	if len(groups) == 0 {
		return nil
	}

	if creator, ok := m.Client.(affinityGroupsCreator); ok {
		_, err := creator.CreateAffinityGroupsWithSkipExistCheck(ctx, groups)
		return err
	}

	// Collect group IDs to check
	groupIDs := make([]string, 0, len(groups))
	for id := range groups {
		groupIDs = append(groupIDs, id)
	}

	// Fallback for older PD clients: check which groups already exist.
	existingGroups, err := m.GetAffinityGroups(ctx, groupIDs)
	if err != nil {
		return err
	}

	// Filter out groups that already exist
	groupsToCreate := make(map[string][]pdhttp.AffinityGroupKeyRange)
	for id, ranges := range groups {
		if _, exists := existingGroups[id]; !exists {
			groupsToCreate[id] = ranges
		}
	}

	// If all groups already exist, return success
	if len(groupsToCreate) == 0 {
		return nil
	}

	// Create only the groups that don't exist
	_, err = m.Client.CreateAffinityGroups(ctx, groupsToCreate)
	return err
}

// DeleteAffinityGroups deletes affinity groups in PD (force=true).
func (m *pdManager) DeleteAffinityGroups(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	return m.Client.BatchDeleteAffinityGroups(ctx, ids, true)
}

// GetAffinityGroups gets affinity groups from PD.
func (m *pdManager) GetAffinityGroups(ctx context.Context, ids []string) (map[string]*pdhttp.AffinityGroupState, error) {
	if len(ids) == 0 {
		return make(map[string]*pdhttp.AffinityGroupState), nil
	}

	if len(ids) > maxAffinityGroupIDsCount || affinityGroupIDsQueryLen(ids) > maxAffinityGroupIDsQueryLen {
		allGroups, err := m.Client.GetAllAffinityGroups(ctx)
		if err != nil {
			return nil, err
		}
		return filterAffinityGroups(allGroups, ids), nil
	}

	if getter, ok := m.Client.(affinityGroupsGetter); ok {
		return getter.GetAffinityGroups(ctx, ids)
	}

	allGroups, err := m.Client.GetAllAffinityGroups(ctx)
	if err != nil {
		return nil, err
	}

	return filterAffinityGroups(allGroups, ids), nil
}

func affinityGroupIDsQueryLen(ids []string) int {
	if len(ids) == 0 {
		return 0
	}
	total := 0
	for i, id := range ids {
		if i == 0 {
			total += len("ids=")
		} else {
			total += len("&ids=")
		}
		total += len(id)
	}
	return total
}

func filterAffinityGroups(groups map[string]*pdhttp.AffinityGroupState, ids []string) map[string]*pdhttp.AffinityGroupState {
	result := make(map[string]*pdhttp.AffinityGroupState, len(ids))
	if len(groups) == 0 {
		return result
	}
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		if group, ok := groups[id]; ok {
			result[id] = group
		}
	}
	return result
}

type mockManager struct {
	sync.RWMutex
	groups map[string]*pdhttp.AffinityGroupState
}

// NewMockManager creates a mock affinity manager for testing.
func NewMockManager() Manager {
	return &mockManager{
		groups: make(map[string]*pdhttp.AffinityGroupState),
	}
}

func (m *mockManager) CreateAffinityGroupsIfNotExists(_ context.Context, groups map[string][]pdhttp.AffinityGroupKeyRange) error {
	if len(groups) == 0 {
		return nil
	}
	m.Lock()
	defer m.Unlock()

	if m.groups == nil {
		m.groups = make(map[string]*pdhttp.AffinityGroupState)
	}

	// Idempotent: only create groups that don't already exist
	for id, ranges := range groups {
		if _, exists := m.groups[id]; !exists {
			m.groups[id] = &pdhttp.AffinityGroupState{
				AffinityGroup: pdhttp.AffinityGroup{
					ID: id,
				},
				RangeCount: len(ranges),
			}
		}
	}
	return nil
}

func (m *mockManager) DeleteAffinityGroups(_ context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	m.Lock()
	for _, id := range ids {
		delete(m.groups, id)
	}
	m.Unlock()
	return nil
}

func (m *mockManager) GetAffinityGroups(_ context.Context, ids []string) (map[string]*pdhttp.AffinityGroupState, error) {
	m.RLock()
	defer m.RUnlock()

	result := make(map[string]*pdhttp.AffinityGroupState)
	for _, id := range ids {
		if state, ok := m.groups[id]; ok {
			result[id] = state
		}
	}
	return result, nil
}
