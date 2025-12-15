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

package infosync

import (
	"context"
	"sync"

	pdhttp "github.com/tikv/pd/client/http"
)

// AffinityManager manages affinity groups with PD.
type AffinityManager interface {
	CreateAffinityGroupsIfNotExists(ctx context.Context, groups map[string][]pdhttp.AffinityGroupKeyRange) error
	DeleteAffinityGroups(ctx context.Context, ids []string) error
	GetAffinityGroups(ctx context.Context, ids []string) (map[string]*pdhttp.AffinityGroupState, error)
}

type pdAffinityManager struct {
	pdhttp.Client
}

// CreateAffinityGroupsIfNotExists creates affinity groups in PD.
// It checks which groups already exist and only creates the ones that don't exist.
// This makes the operation safe for DDL job retries.
func (m *pdAffinityManager) CreateAffinityGroupsIfNotExists(ctx context.Context, groups map[string][]pdhttp.AffinityGroupKeyRange) error {
	if len(groups) == 0 {
		return nil
	}

	// Collect group IDs to check
	groupIDs := make([]string, 0, len(groups))
	for id := range groups {
		groupIDs = append(groupIDs, id)
	}

	// Check which groups already exist
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
func (m *pdAffinityManager) DeleteAffinityGroups(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	return m.Client.BatchDeleteAffinityGroups(ctx, ids, true)
}

// GetAffinityGroups gets affinity groups from PD.
func (m *pdAffinityManager) GetAffinityGroups(ctx context.Context, ids []string) (map[string]*pdhttp.AffinityGroupState, error) {
	if len(ids) == 0 {
		return make(map[string]*pdhttp.AffinityGroupState), nil
	}

	// TODO: avoid using GetAllAffinityGroups
	allGroups, err := m.Client.GetAllAffinityGroups(ctx)
	if err != nil {
		return nil, err
	}

	// Filter by requested IDs
	result := make(map[string]*pdhttp.AffinityGroupState)
	for _, id := range ids {
		if group, ok := allGroups[id]; ok {
			result[id] = group
		}
	}
	return result, nil
}

type mockAffinityManager struct {
	sync.RWMutex
	groups map[string]*pdhttp.AffinityGroupState
}

func (m *mockAffinityManager) CreateAffinityGroupsIfNotExists(_ context.Context, groups map[string][]pdhttp.AffinityGroupKeyRange) error {
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
				Phase:      "ready",
				RangeCount: len(ranges),
			}
		}
	}
	return nil
}

func (m *mockAffinityManager) DeleteAffinityGroups(_ context.Context, ids []string) error {
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

func (m *mockAffinityManager) GetAffinityGroups(_ context.Context, ids []string) (map[string]*pdhttp.AffinityGroupState, error) {
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
