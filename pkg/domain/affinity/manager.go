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
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/errno"
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

const (
	// Keep ids query under a conservative URI budget and fall back to full-scan filtering
	// for larger requests to avoid request-line limits in proxies and gateways.
	maxAffinityGroupIDsQueryLen = 4096
	// Cap ids count so large lookups use full-scan filtering instead of building huge query strings.
	maxAffinityGroupIDsCount = 100
)

// NewPDManager creates a new affinity manager that uses PD HTTP client.
func NewPDManager(client pdhttp.Client) Manager {
	return &pdManager{client}
}

// CreateAffinityGroupsIfNotExists creates affinity groups in PD.
// It first uses the skip_exist_check API. If that behavior is rejected by an older
// PD, it falls back to checking existing IDs and creating only missing groups.
// This makes the operation safe for DDL job retries in mixed-version deployments.
func (m *pdManager) CreateAffinityGroupsIfNotExists(ctx context.Context, groups map[string][]pdhttp.AffinityGroupKeyRange) error {
	if len(groups) == 0 {
		return nil
	}

	_, err := m.Client.CreateAffinityGroups(ctx, groups, pdhttp.WithSkipExistCheck())
	if err == nil {
		return nil
	}
	if !shouldFallbackCreateAffinityGroups(err) {
		return err
	}

	return m.createAffinityGroupsIfNotExistsByFiltering(ctx, groups)
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

	if shouldUseGetAllAffinityGroups(ids) {
		return m.getAffinityGroupsByScanningAll(ctx, ids)
	}

	groups, err := m.Client.GetAffinityGroups(ctx, ids)
	if err != nil {
		if shouldFallbackGetAffinityGroups(err) {
			return m.getAffinityGroupsByScanningAll(ctx, ids)
		}
		return nil, err
	}

	// Older PD versions may ignore ids query parameters and return all groups.
	// Filter client-side so requested IDs semantics stay stable in mixed versions.
	return filterAffinityGroups(groups, ids), nil
}

func (m *pdManager) createAffinityGroupsIfNotExistsByFiltering(ctx context.Context, groups map[string][]pdhttp.AffinityGroupKeyRange) error {
	groupIDs := make([]string, 0, len(groups))
	for id := range groups {
		groupIDs = append(groupIDs, id)
	}
	sort.Strings(groupIDs)

	existingGroups, err := m.GetAffinityGroups(ctx, groupIDs)
	if err != nil {
		return err
	}

	groupsToCreate := make(map[string][]pdhttp.AffinityGroupKeyRange, len(groups))
	for id, ranges := range groups {
		if _, exists := existingGroups[id]; !exists {
			groupsToCreate[id] = ranges
		}
	}
	if len(groupsToCreate) == 0 {
		return nil
	}

	_, err = m.Client.CreateAffinityGroups(ctx, groupsToCreate)
	return err
}

func (m *pdManager) getAffinityGroupsByScanningAll(ctx context.Context, ids []string) (map[string]*pdhttp.AffinityGroupState, error) {
	allGroups, err := m.Client.GetAllAffinityGroups(ctx)
	if err != nil {
		return nil, err
	}
	return filterAffinityGroups(allGroups, ids), nil
}

func shouldUseGetAllAffinityGroups(ids []string) bool {
	return len(ids) > maxAffinityGroupIDsCount || affinityGroupIDsEscapedQueryLen(ids) > maxAffinityGroupIDsQueryLen
}

func affinityGroupIDsEscapedQueryLen(ids []string) int {
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
		total += len(url.QueryEscape(id))
	}
	return total
}

func shouldFallbackCreateAffinityGroups(err error) bool {
	return isPDHTTPStatusError(err, http.StatusBadRequest) ||
		isPDHTTPStatusError(err, http.StatusConflict) ||
		isPDHTTPServiceErrorWithoutStatus(err)
}

func shouldFallbackGetAffinityGroups(err error) bool {
	return isPDHTTPStatusError(err, http.StatusBadRequest) ||
		isPDHTTPStatusError(err, http.StatusNotFound) ||
		isPDHTTPStatusError(err, http.StatusRequestURITooLong) ||
		isPDHTTPServiceErrorWithoutStatus(err)
}

func isPDHTTPStatusError(err error, statusCode int) bool {
	code, ok := extractPDHTTPStatusCode(err)
	return ok && code == statusCode
}

func extractPDHTTPStatusCode(err error) (int, bool) {
	if err == nil {
		return 0, false
	}

	const statusPrefix = "status: '"
	msg := err.Error()
	idx := strings.Index(msg, statusPrefix)
	if idx < 0 {
		return 0, false
	}

	start := idx + len(statusPrefix)
	end := start
	for end < len(msg) && msg[end] >= '0' && msg[end] <= '9' {
		end++
	}
	if end == start {
		return 0, false
	}

	code, convErr := strconv.Atoi(msg[start:end])
	if convErr != nil {
		return 0, false
	}
	return code, true
}

func isPDHTTPServiceErrorWithoutStatus(err error) bool {
	if err == nil {
		return false
	}
	if _, ok := extractPDHTTPStatusCode(err); ok {
		return false
	}

	rootErr, ok := errors.Cause(err).(*errors.Error)
	if !ok {
		return false
	}
	// TiDB's injected PD response handler returns ErrHTTPServiceError without the
	// original HTTP status code in the message, so treat it as compatibility case.
	return rootErr.Code() == errors.ErrCode(errno.ErrHTTPServiceError)
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
