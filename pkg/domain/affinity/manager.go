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
	stderrors "errors"
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
	// Keep each ids query under a conservative URI budget to avoid request-line
	// limits in proxies and gateways.
	maxAffinityGroupIDsQueryLen = 4096
	// Cap the number of IDs in each PD request.
	maxAffinityGroupIDsCount = 100
)

// NewPDManager creates a new affinity manager that uses PD HTTP client.
func NewPDManager(client pdhttp.Client) Manager {
	return &pdManager{client}
}

// CreateAffinityGroupsIfNotExists creates affinity groups in PD.
// It first uses skip_exist_check and validates the returned state for every group.
// On older PD versions a batch conflict means at least one group already exists.
func (m *pdManager) CreateAffinityGroupsIfNotExists(ctx context.Context, groups map[string][]pdhttp.AffinityGroupKeyRange) error {
	if len(groups) == 0 {
		return nil
	}

	states, err := m.Client.CreateAffinityGroups(ctx, groups, pdhttp.WithSkipExistCheck())
	if err == nil {
		return validateCreatedAffinityGroups(groups, states)
	}
	if !isPDHTTPStatusError(err, http.StatusConflict) {
		return err
	}
	if len(groups) == 1 {
		return nil
	}

	return m.createAffinityGroupsIfNotExistsByFiltering(ctx, groups)
}

// DeleteAffinityGroups force-deletes affinity groups in PD. It succeeds if the
// groups are already absent or the PD version lacks the affinity endpoint.
func (m *pdManager) DeleteAffinityGroups(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	err := m.Client.BatchDeleteAffinityGroups(ctx, ids, true)
	// PD ignores missing group IDs for a forced batch delete. Older PD
	// versions without the affinity endpoint return 404.
	if isPDHTTPStatusError(err, http.StatusNotFound) {
		return nil
	}
	return err
}

// GetAffinityGroups gets affinity groups from PD.
func (m *pdManager) GetAffinityGroups(ctx context.Context, ids []string) (map[string]*pdhttp.AffinityGroupState, error) {
	if len(ids) == 0 {
		return make(map[string]*pdhttp.AffinityGroupState), nil
	}

	uniqueIDs := make([]string, 0, len(ids))
	idIndexes := make(map[string]int, len(ids))
	for _, id := range ids {
		if _, exists := idIndexes[id]; exists {
			continue
		}
		if affinityGroupIDsEscapedQueryLen([]string{id}) > maxAffinityGroupIDsQueryLen {
			return m.getAffinityGroupsByScanningAll(ctx, ids, nil)
		}
		idIndexes[id] = len(uniqueIDs)
		uniqueIDs = append(uniqueIDs, id)
	}

	result := make(map[string]*pdhttp.AffinityGroupState, len(uniqueIDs))
	for start := 0; start < len(uniqueIDs); {
		end, queryLen := start, 0
		for end < len(uniqueIDs) && end-start < maxAffinityGroupIDsCount {
			idQueryLen := affinityGroupIDsEscapedQueryLen(uniqueIDs[end : end+1])
			if end > start {
				idQueryLen++ // The separator between ids parameters.
			}
			if queryLen+idQueryLen > maxAffinityGroupIDsQueryLen {
				break
			}
			queryLen += idQueryLen
			end++
		}

		groups, err := m.Client.GetAffinityGroups(ctx, uniqueIDs[start:end])
		if err != nil {
			if shouldFallbackGetAffinityGroups(err) {
				return m.getAffinityGroupsByScanningAll(ctx, uniqueIDs, result)
			}
			return nil, err
		}
		for id, group := range groups {
			index, requested := idIndexes[id]
			if !requested || index < start || index >= end {
				// Older PD versions ignore ids and return all groups. Reuse that
				// response for the entire lookup instead of requesting more batches.
				return filterAffinityGroups(groups, uniqueIDs), nil
			}
			result[id] = group
		}
		start = end
	}
	return result, nil
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

	groupsToCreate := make(map[string][]pdhttp.AffinityGroupKeyRange)
	for id, ranges := range groups {
		if !isValidAffinityGroupState(id, existingGroups[id]) {
			groupsToCreate[id] = ranges
		}
	}
	if len(groupsToCreate) == 0 {
		return nil
	}

	states, err := m.Client.CreateAffinityGroups(ctx, groupsToCreate)
	if isPDHTTPStatusError(err, http.StatusConflict) {
		// Another caller may have created the missing groups since the lookup.
		// Recheck even on the last attempt. If the scheduling service's view
		// still lags, the caller retries within the existing budget.
		existingGroups, readErr := m.GetAffinityGroups(ctx, groupIDs)
		if readErr != nil {
			return readErr
		}
		for _, id := range groupIDs {
			if !isValidAffinityGroupState(id, existingGroups[id]) {
				return &affinityGroupCreateConflict{error: err}
			}
		}
		return nil
	}
	if err != nil {
		return err
	}
	return validateCreatedAffinityGroups(groupsToCreate, states)
}

// affinityGroupCreateConflict marks the fallback POST conflict as retryable.
type affinityGroupCreateConflict struct{ error }

func (err *affinityGroupCreateConflict) Unwrap() error { return err.error }

func isValidAffinityGroupState(id string, state *pdhttp.AffinityGroupState) bool {
	return state != nil && state.ID == id && state.RangeCount > 0
}

func validateCreatedAffinityGroups(groups map[string][]pdhttp.AffinityGroupKeyRange, states map[string]*pdhttp.AffinityGroupState) error {
	for id := range groups {
		if !isValidAffinityGroupState(id, states[id]) {
			return errors.Errorf("PD returned no valid affinity group state for group %q after creation", id)
		}
	}
	return nil
}

func (m *pdManager) getAffinityGroupsByScanningAll(
	ctx context.Context,
	ids []string,
	partial map[string]*pdhttp.AffinityGroupState,
) (map[string]*pdhttp.AffinityGroupState, error) {
	allGroups, err := m.Client.GetAllAffinityGroups(ctx)
	if err != nil {
		// Older PD versions do not serve affinity GETs. Keep any previously
		// fetched states so SHOW AFFINITY can still report them.
		if isPDHTTPStatusError(err, http.StatusNotFound) {
			if partial != nil {
				return partial, nil
			}
			return make(map[string]*pdhttp.AffinityGroupState), nil
		}
		return nil, err
	}
	return filterAffinityGroups(allGroups, ids), nil
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

	var statusErr interface{ HTTPStatusCode() int }
	if stderrors.As(err, &statusErr) {
		return statusErr.HTTPStatusCode(), true
	}

	const statusPrefix = "request pd http api failed with status: '"
	status, ok := strings.CutPrefix(errors.Cause(err).Error(), statusPrefix)
	if !ok {
		return 0, false
	}

	end := 0
	for end < len(status) && status[end] >= '0' && status[end] <= '9' {
		end++
	}
	if end == 0 {
		return 0, false
	}

	code, convErr := strconv.Atoi(status[:end])
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
	// Preserve lookup compatibility for clients returning a bare service error.
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
