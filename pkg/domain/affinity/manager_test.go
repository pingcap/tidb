// Copyright 2026 PingCAP, Inc.
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
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	pdhttp "github.com/tikv/pd/client/http"
)

type mockPDClient struct {
	pdhttp.Client
	mock.Mock
}

func (m *mockPDClient) CreateAffinityGroups(
	ctx context.Context,
	groups map[string][]pdhttp.AffinityGroupKeyRange,
	opts ...pdhttp.CreateAffinityGroupsOption,
) (map[string]*pdhttp.AffinityGroupState, error) {
	args := m.Called(ctx, groups, len(opts))
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[string]*pdhttp.AffinityGroupState), args.Error(1)
}

func (m *mockPDClient) GetAffinityGroups(ctx context.Context, ids []string) (map[string]*pdhttp.AffinityGroupState, error) {
	args := m.Called(ctx, ids)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[string]*pdhttp.AffinityGroupState), args.Error(1)
}

func (m *mockPDClient) BatchDeleteAffinityGroups(ctx context.Context, ids []string, force bool) error {
	return m.Called(ctx, ids, force).Error(0)
}

func TestDeleteAffinityGroups(t *testing.T) {
	for _, tc := range []struct {
		name    string
		err     error
		wantErr bool
	}{
		{"already absent", nil, false},
		{"unsupported endpoint", pdStatusErr(http.StatusNotFound), false},
		{"service unavailable", pdStatusErr(http.StatusServiceUnavailable), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &mockPDClient{}
			manager := NewPDManager(client)
			ctx := context.Background()
			ids := []string{"g1"}
			client.On("BatchDeleteAffinityGroups", ctx, ids, true).Return(tc.err).Once()
			err := manager.DeleteAffinityGroups(ctx, ids)
			if tc.wantErr {
				require.ErrorIs(t, err, tc.err)
			} else {
				require.NoError(t, err)
			}
			client.AssertExpectations(t)
		})
	}
}

func (m *mockPDClient) GetAllAffinityGroups(ctx context.Context) (map[string]*pdhttp.AffinityGroupState, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[string]*pdhttp.AffinityGroupState), args.Error(1)
}

func TestCreateAffinityGroupsIfNotExistsUseSkipExistCheck(t *testing.T) {
	ctx := context.Background()
	client := &mockPDClient{}
	manager := &pdManager{Client: client}

	groups := map[string][]pdhttp.AffinityGroupKeyRange{
		"g1": {{StartKey: []byte("a"), EndKey: []byte("b")}},
	}
	client.On("CreateAffinityGroups", mock.Anything, groups, 1).Return(map[string]*pdhttp.AffinityGroupState{"g1": affinityGroupState("g1")}, nil).Once()

	require.NoError(t, manager.CreateAffinityGroupsIfNotExists(ctx, groups))
	client.AssertExpectations(t)
	client.AssertNotCalled(t, "GetAffinityGroups", mock.Anything, mock.Anything)
}

func TestCreateAffinityGroupsIfNotExistsFallbackWhenSkipExistRejected(t *testing.T) {
	t.Run("single group", testCreateGroupsIfNotExistsRechecksSingleGroupConflict)

	ctx := context.Background()
	client := &mockPDClient{}
	manager := &pdManager{Client: client}

	groups := map[string][]pdhttp.AffinityGroupKeyRange{
		"g1": {{StartKey: []byte("a"), EndKey: []byte("b")}},
		"g2": {{StartKey: []byte("c"), EndKey: []byte("d")}},
	}
	client.On("CreateAffinityGroups", mock.Anything, groups, 1).Return(nil, pdStatusErr(http.StatusConflict)).Once()
	existing := client.On("GetAffinityGroups", mock.Anything, []string{"g1", "g2"}).Return(
		map[string]*pdhttp.AffinityGroupState{"g1": affinityGroupState("g1")}, nil,
	).Once()
	missing := client.On("CreateAffinityGroups", mock.Anything, map[string][]pdhttp.AffinityGroupKeyRange{
		"g2": groups["g2"],
	}, 0).Return(map[string]*pdhttp.AffinityGroupState{"g2": affinityGroupState("g2")}, nil).Once()
	mock.InOrder(existing, missing)

	require.NoError(t, manager.CreateAffinityGroupsIfNotExists(ctx, groups))
	client.AssertExpectations(t)
}

func testCreateGroupsIfNotExistsRechecksSingleGroupConflict(t *testing.T) {
	ctx := context.Background()
	conflict := pdStatusErr(http.StatusConflict)
	groups := map[string][]pdhttp.AffinityGroupKeyRange{
		"g1": {{StartKey: []byte("a"), EndKey: []byte("b")}},
	}
	for _, tc := range []struct {
		name       string
		rangeCount int
		attempts   int
	}{
		{"positive range count", 1, 1},
		{"zero range count", 0, maxRetryTimes + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &mockPDClient{}
			InitManager(client)
			t.Cleanup(func() { InitManager(nil) })
			state := affinityGroupState("g1")
			state.RangeCount = tc.rangeCount
			states := map[string]*pdhttp.AffinityGroupState{"g1": state}
			ids := []string{"g1"}
			var calls []*mock.Call
			for range tc.attempts {
				calls = append(calls,
					client.On("CreateAffinityGroups", ctx, groups, 1).Return(nil, conflict).Once(),
					client.On("GetAffinityGroups", ctx, ids).Return(states, nil).Once(),
				)
				if tc.rangeCount > 0 {
					continue
				}
				calls = append(calls,
					client.On("CreateAffinityGroups", ctx, groups, 0).Return(nil, conflict).Once(),
					client.On("GetAffinityGroups", ctx, ids).Return(states, nil).Once(),
				)
			}
			mock.InOrder(calls...)

			err := CreateGroupsIfNotExists(ctx, groups)
			if tc.rangeCount > 0 {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, conflict)
			}
			client.AssertExpectations(t)
		})
	}
}

func TestCreateGroupsIfNotExistsRechecksBatchConflict(t *testing.T) {
	ctx := context.Background()
	conflict := pdStatusErr(http.StatusConflict)
	groups := map[string][]pdhttp.AffinityGroupKeyRange{
		"g1": {{StartKey: []byte("a"), EndKey: []byte("b")}},
		"g2": {{StartKey: []byte("c"), EndKey: []byte("d")}},
		"g3": {{StartKey: []byte("e"), EndKey: []byte("f")}},
	}
	ids := []string{"g1", "g2", "g3"}
	first := map[string]*pdhttp.AffinityGroupState{"g1": affinityGroupState("g1")}
	firstTwo := map[string]*pdhttp.AffinityGroupState{"g1": affinityGroupState("g1"), "g2": affinityGroupState("g2")}
	all := map[string]*pdhttp.AffinityGroupState{"g1": affinityGroupState("g1"), "g2": affinityGroupState("g2"), "g3": affinityGroupState("g3")}
	missing := map[string][]pdhttp.AffinityGroupKeyRange{"g2": groups["g2"], "g3": groups["g3"]}
	remaining := map[string][]pdhttp.AffinityGroupKeyRange{"g3": groups["g3"]}

	for _, tc := range []struct {
		name      string
		conflicts int
		finalRead map[string]*pdhttp.AffinityGroupState
		wantErr   bool
	}{
		{"all created concurrently", 1, all, false},
		{"watcher catches up and one group still needs creation", 1, first, false},
		{"final read sees all groups", maxRetryTimes + 1, all, false},
		{"watcher still stale after final conflict", maxRetryTimes + 1, first, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &mockPDClient{}
			InitManager(client)
			t.Cleanup(func() { InitManager(nil) })
			var calls []*mock.Call
			for i := 0; i < tc.conflicts; i++ {
				states := first
				if i == tc.conflicts-1 {
					states = tc.finalRead
				}
				calls = append(calls,
					client.On("CreateAffinityGroups", ctx, groups, 1).Return(nil, conflict).Once(),
					client.On("GetAffinityGroups", ctx, ids).Return(first, nil).Once(),
					client.On("CreateAffinityGroups", ctx, missing, 0).Return(nil, conflict).Once(),
					client.On("GetAffinityGroups", ctx, ids).Return(states, nil).Once(),
				)
			}
			if tc.conflicts == 1 && len(tc.finalRead) != len(groups) {
				calls = append(calls,
					client.On("CreateAffinityGroups", ctx, groups, 1).Return(nil, conflict).Once(),
					client.On("GetAffinityGroups", ctx, ids).Return(firstTwo, nil).Once(),
					client.On("CreateAffinityGroups", ctx, remaining, 0).Return(
						map[string]*pdhttp.AffinityGroupState{"g3": all["g3"]}, nil,
					).Once(),
				)
			}
			mock.InOrder(calls...)

			err := CreateGroupsIfNotExists(ctx, groups)
			if tc.wantErr {
				require.ErrorIs(t, err, conflict)
			} else {
				require.NoError(t, err)
			}
			client.AssertExpectations(t)
		})
	}
}

func TestCreateAffinityGroupsIfNotExistsDoNotFallbackForNonCompatibilityError(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"range conflict", pdStatusErr(http.StatusBadRequest)},
		{"no HTTP status", pdHTTPServiceErr("mock service error")},
		{"PD error text in service body", pdHTTPServiceErr(pdStatusErr(http.StatusConflict).Error())},
		{"internal server error", pdStatusErr(http.StatusInternalServerError)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &mockPDClient{}
			manager := &pdManager{Client: client}
			groups := map[string][]pdhttp.AffinityGroupKeyRange{
				"g1": {{StartKey: []byte("a"), EndKey: []byte("b")}},
			}
			client.On("CreateAffinityGroups", mock.Anything, groups, 1).Return(nil, tc.err).Once()
			// A scheduling service can still see a group deleted by the PD leader.
			client.On("GetAffinityGroups", mock.Anything, []string{"g1"}).Return(
				map[string]*pdhttp.AffinityGroupState{"g1": affinityGroupState("g1")}, nil,
			).Maybe()

			require.ErrorIs(t, manager.CreateAffinityGroupsIfNotExists(context.Background(), groups), tc.err)
			client.AssertExpectations(t)
			client.AssertNotCalled(t, "GetAffinityGroups", mock.Anything, mock.Anything)
		})
	}
}

func TestCreateAffinityGroupsIfNotExistsRejectsIncompleteResponse(t *testing.T) {
	groups := map[string][]pdhttp.AffinityGroupKeyRange{
		"g1": {{StartKey: []byte("a"), EndKey: []byte("b")}},
		"g2": {{StartKey: []byte("c"), EndKey: []byte("d")}},
	}
	for _, tc := range []struct {
		name  string
		state *pdhttp.AffinityGroupState
	}{
		{"missing", nil},
		{"nil", nil},
		{"zero value", &pdhttp.AffinityGroupState{}},
		{"wrong ID", affinityGroupState("other")},
		{"no ranges", &pdhttp.AffinityGroupState{AffinityGroup: pdhttp.AffinityGroup{ID: "g2"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &mockPDClient{}
			manager := &pdManager{Client: client}
			response := map[string]*pdhttp.AffinityGroupState{"g1": affinityGroupState("g1")}
			if tc.name != "missing" {
				response["g2"] = tc.state
			}
			client.On("CreateAffinityGroups", mock.Anything, groups, 1).Return(response, nil).Once()

			require.ErrorContains(t, manager.CreateAffinityGroupsIfNotExists(context.Background(), groups), "g2")
			client.AssertExpectations(t)
		})
	}
}

func TestGetAffinityGroupsFilterDirectResponse(t *testing.T) {
	ctx := context.Background()
	client := &mockPDClient{}
	manager := &pdManager{Client: client}

	ids := []string{"g1", "g2"}
	client.On("GetAffinityGroups", mock.Anything, ids).Return(
		map[string]*pdhttp.AffinityGroupState{
			"g1": affinityGroupState("g1"),
			"g2": affinityGroupState("g2"),
			"g3": affinityGroupState("g3"),
		}, nil,
	).Once()

	result, err := manager.GetAffinityGroups(ctx, ids)
	require.NoError(t, err)
	require.Len(t, result, 2)
	require.Contains(t, result, "g1")
	require.Contains(t, result, "g2")
	require.NotContains(t, result, "g3")
	client.AssertExpectations(t)
}

func TestGetAffinityGroupsFallbackByEscapedQueryLen(t *testing.T) {
	ctx := context.Background()
	client := &mockPDClient{}
	manager := &pdManager{Client: client}

	id := strings.Repeat("/", 1365)
	ids := []string{"short", id}
	client.On("GetAllAffinityGroups", mock.Anything).Return(
		map[string]*pdhttp.AffinityGroupState{
			id:      affinityGroupState(id),
			"other": affinityGroupState("other"),
		}, nil,
	).Once()

	result, err := manager.GetAffinityGroups(ctx, ids)
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Contains(t, result, id)
	client.AssertExpectations(t)
	client.AssertNotCalled(t, "GetAffinityGroups", mock.Anything, mock.Anything)
}

func TestGetAffinityGroupsFallbackWhenIDsQueryUnsupported(t *testing.T) {
	ctx := context.Background()
	client := &mockPDClient{}
	manager := &pdManager{Client: client}

	ids := []string{"g1"}
	client.On("GetAffinityGroups", mock.Anything, ids).Return(nil, pdStatusErr(http.StatusBadRequest)).Once()
	client.On("GetAllAffinityGroups", mock.Anything).Return(
		map[string]*pdhttp.AffinityGroupState{
			"g1": affinityGroupState("g1"),
			"g2": affinityGroupState("g2"),
		}, nil,
	).Once()

	result, err := manager.GetAffinityGroups(ctx, ids)
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Contains(t, result, "g1")
	require.NotContains(t, result, "g2")
	client.AssertExpectations(t)
}

func TestGetAffinityGroupsBatches(t *testing.T) {
	for _, test := range []struct {
		name       string
		count      int
		suffix     string
		batchSizes []int
	}{
		{name: "ID count and duplicates", count: 250, batchSizes: []int{100, 100, 50}},
		{name: "escaped query bytes", count: 30, suffix: strings.Repeat("/", 200), batchSizes: []int{6, 6, 6, 6, 6}},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := &mockPDClient{}
			manager := &pdManager{Client: client}
			ids := make([]string, test.count)
			expected := make(map[string]*pdhttp.AffinityGroupState, test.count)
			for i := range ids {
				ids[i] = fmt.Sprintf("g%03d%s", i, test.suffix)
				expected[ids[i]] = affinityGroupState(ids[i])
			}
			start := 0
			for _, size := range test.batchSizes {
				batch := ids[start : start+size]
				require.LessOrEqual(t, len(batch), maxAffinityGroupIDsCount)
				require.LessOrEqual(t, affinityGroupIDsEscapedQueryLen(batch), maxAffinityGroupIDsQueryLen)
				client.On("GetAffinityGroups", mock.Anything, batch).Return(filterAffinityGroups(expected, batch), nil).Once()
				start += size
			}
			ids = append(ids, ids[0], ids[len(ids)-1])
			original := append([]string(nil), ids...)
			result, err := manager.GetAffinityGroups(context.Background(), ids)
			require.NoError(t, err)
			require.Equal(t, expected, result)
			require.Equal(t, original, ids)
			client.AssertExpectations(t)
			client.AssertNumberOfCalls(t, "GetAffinityGroups", len(test.batchSizes))
			client.AssertNotCalled(t, "GetAllAffinityGroups", mock.Anything)
		})
	}
}

func TestGetAffinityGroupsBatchesCompatibility(t *testing.T) {
	const unrequestedID = "unrequested"
	ids := make([]string, 250)
	allGroups := make(map[string]*pdhttp.AffinityGroupState, len(ids)+1)
	for i := range ids {
		ids[i] = fmt.Sprintf("g%03d", i)
		allGroups[ids[i]] = affinityGroupState(ids[i])
	}
	allGroups[unrequestedID] = affinityGroupState(unrequestedID)
	expected := filterAffinityGroups(allGroups, ids)
	secondBatchID := ids[maxAffinityGroupIDsCount]

	t.Run("unsupported query on a later batch", func(t *testing.T) {
		client := &mockPDClient{}
		manager := &pdManager{Client: client}
		client.On("GetAffinityGroups", mock.Anything, ids[:100]).Return(filterAffinityGroups(allGroups, ids[:100]), nil).Once()
		client.On("GetAffinityGroups", mock.Anything, ids[100:200]).Return(nil, pdStatusErr(http.StatusBadRequest)).Once()
		client.On("GetAllAffinityGroups", mock.Anything).Return(allGroups, nil).Once()
		result, err := manager.GetAffinityGroups(context.Background(), ids)
		require.NoError(t, err)
		require.Equal(t, expected, result)
		client.AssertExpectations(t)
		client.AssertNumberOfCalls(t, "GetAffinityGroups", 2)
		client.AssertNumberOfCalls(t, "GetAllAffinityGroups", 1)
	})

	for name, response := range map[string]map[string]*pdhttp.AffinityGroupState{
		"PD ignores query with unrequested groups": allGroups,
		"PD ignores query with requested groups":   expected,
	} {
		t.Run(name, func(t *testing.T) {
			client := &mockPDClient{}
			manager := &pdManager{Client: client}
			client.On("GetAffinityGroups", mock.Anything, ids[:100]).Return(response, nil).Once()
			result, err := manager.GetAffinityGroups(context.Background(), ids)
			require.NoError(t, err)
			require.Equal(t, expected, result)
			client.AssertExpectations(t)
			client.AssertNumberOfCalls(t, "GetAffinityGroups", 1)
			client.AssertNotCalled(t, "GetAllAffinityGroups", mock.Anything)
		})
	}

	for _, tc := range []struct {
		name      string
		firstRead bool
		scan      map[string]*pdhttp.AffinityGroupState
		scanErr   error
		expected  map[string]*pdhttp.AffinityGroupState
	}{
		{"first batch and full scan return 404", false, nil, pdStatusErr(http.StatusNotFound), map[string]*pdhttp.AffinityGroupState{}},
		{"later batch and full scan return 404", true, nil, pdStatusErr(http.StatusNotFound), map[string]*pdhttp.AffinityGroupState{ids[0]: allGroups[ids[0]], ids[1]: allGroups[ids[1]]}},
		{"full scan replaces partial results and drops deleted groups", true,
			map[string]*pdhttp.AffinityGroupState{
				ids[0]:        {ID: ids[0], RangeCount: 2},
				secondBatchID: allGroups[secondBatchID],
				unrequestedID: allGroups[unrequestedID],
			}, nil,
			map[string]*pdhttp.AffinityGroupState{
				ids[0]:        {ID: ids[0], RangeCount: 2},
				secondBatchID: allGroups[secondBatchID],
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &mockPDClient{}
			manager := NewPDManager(client)
			start := 0
			if tc.firstRead {
				client.On("GetAffinityGroups", mock.Anything, ids[:100]).Return(
					map[string]*pdhttp.AffinityGroupState{ids[0]: allGroups[ids[0]], ids[1]: allGroups[ids[1]]}, nil,
				).Once()
				start = 100
			}
			client.On("GetAffinityGroups", mock.Anything, ids[start:start+100]).Return(nil, pdStatusErr(http.StatusNotFound)).Once()
			client.On("GetAllAffinityGroups", mock.Anything).Return(tc.scan, tc.scanErr).Once()
			result, err := manager.GetAffinityGroups(context.Background(), ids)
			require.NoError(t, err)
			require.Equal(t, tc.expected, result)
			client.AssertExpectations(t)
		})
	}

	t.Run("non compatibility error on a later batch", func(t *testing.T) {
		client := &mockPDClient{}
		manager := &pdManager{Client: client}
		client.On("GetAffinityGroups", mock.Anything, ids[:100]).Return(filterAffinityGroups(allGroups, ids[:100]), nil).Once()
		client.On("GetAffinityGroups", mock.Anything, ids[100:200]).Return(nil, context.Canceled).Once()
		result, err := manager.GetAffinityGroups(context.Background(), ids)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, result)
		client.AssertExpectations(t)
		client.AssertNumberOfCalls(t, "GetAffinityGroups", 2)
		client.AssertNotCalled(t, "GetAllAffinityGroups", mock.Anything)
	})
}

func TestGetAffinityGroupsFallbackForHTTPServiceError(t *testing.T) {
	ctx := context.Background()
	client := &mockPDClient{}
	manager := &pdManager{Client: client}

	ids := []string{"g1"}
	client.On("GetAffinityGroups", mock.Anything, ids).Return(nil, pdHTTPServiceErr("mock service error")).Once()
	client.On("GetAllAffinityGroups", mock.Anything).Return(
		map[string]*pdhttp.AffinityGroupState{
			"g1": affinityGroupState("g1"),
			"g2": affinityGroupState("g2"),
		}, nil,
	).Once()

	result, err := manager.GetAffinityGroups(ctx, ids)
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Contains(t, result, "g1")
	require.NotContains(t, result, "g2")
	client.AssertExpectations(t)
}

func TestAffinityGroupIDsEscapedQueryLenBoundary(t *testing.T) {
	require.Equal(t, maxAffinityGroupIDsQueryLen, affinityGroupIDsEscapedQueryLen([]string{strings.Repeat("/", 1364)}))
	require.Equal(t, maxAffinityGroupIDsQueryLen+3, affinityGroupIDsEscapedQueryLen([]string{strings.Repeat("/", 1365)}))
}

func TestGetAffinityGroupsBatchQueryBoundary(t *testing.T) {
	client := &mockPDClient{}
	manager := &pdManager{Client: client}
	ids := []string{strings.Repeat("/", 1364), "other"}
	expected := map[string]*pdhttp.AffinityGroupState{
		ids[0]: affinityGroupState(ids[0]),
		ids[1]: affinityGroupState(ids[1]),
	}
	for _, id := range ids {
		client.On("GetAffinityGroups", mock.Anything, []string{id}).Return(
			map[string]*pdhttp.AffinityGroupState{id: expected[id]}, nil,
		).Once()
	}
	result, err := manager.GetAffinityGroups(context.Background(), ids)
	require.NoError(t, err)
	require.Equal(t, expected, result)
	client.AssertExpectations(t)
	client.AssertNotCalled(t, "GetAllAffinityGroups", mock.Anything)
}

func TestIsPDHTTPStatusErrorMatchByCodeOnly(t *testing.T) {
	err := fmt.Errorf("request pd http api failed with status: '400 UnknownText', body: 'test'")
	require.True(t, isPDHTTPStatusError(err, http.StatusBadRequest))
	require.False(t, isPDHTTPStatusError(err, http.StatusConflict))
}

func affinityGroupState(id string) *pdhttp.AffinityGroupState {
	return &pdhttp.AffinityGroupState{AffinityGroup: pdhttp.AffinityGroup{ID: id}, RangeCount: 1}
}

func pdStatusErr(statusCode int) error {
	return fmt.Errorf("request pd http api failed with status: '%d %s', body: 'test'", statusCode, http.StatusText(statusCode))
}

func pdHTTPServiceErr(msg string) error {
	return terror.ClassDomain.Synthesize(terror.ErrCode(errno.ErrHTTPServiceError), fmt.Sprintf("HTTP request failed with status %s", msg))
}

func TestCreateGroupsIfNotExistsNilContextForwardsNonNil(t *testing.T) {
	prevManager, prevClient := manager, pdClient
	t.Cleanup(func() {
		manager = prevManager
		pdClient = prevClient
	})
	client := &mockPDClient{}
	InitManager(client)

	groups := map[string][]pdhttp.AffinityGroupKeyRange{
		"g1": {{StartKey: []byte("a"), EndKey: []byte("b")}},
	}
	client.On("CreateAffinityGroups", mock.MatchedBy(func(ctx context.Context) bool {
		return ctx != nil
	}), groups, 1).Return(map[string]*pdhttp.AffinityGroupState{"g1": affinityGroupState("g1")}, nil).Once()

	//nolint:staticcheck // SA1012: intentionally test nil context normalization
	require.NoError(t, CreateGroupsIfNotExists(nil, groups))
	client.AssertExpectations(t)
}
