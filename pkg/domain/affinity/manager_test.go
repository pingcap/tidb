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
	client.On("CreateAffinityGroups", mock.Anything, groups, 1).Return(nil, nil).Once()

	require.NoError(t, manager.CreateAffinityGroupsIfNotExists(ctx, groups))
	client.AssertExpectations(t)
	client.AssertNotCalled(t, "GetAffinityGroups", mock.Anything, mock.Anything)
}

func TestCreateAffinityGroupsIfNotExistsFallbackWhenSkipExistRejected(t *testing.T) {
	ctx := context.Background()
	client := &mockPDClient{}
	manager := &pdManager{Client: client}

	groups := map[string][]pdhttp.AffinityGroupKeyRange{
		"g1": {{StartKey: []byte("a"), EndKey: []byte("b")}},
		"g2": {{StartKey: []byte("c"), EndKey: []byte("d")}},
	}
	client.On("CreateAffinityGroups", mock.Anything, groups, 1).Return(nil, pdStatusErr(http.StatusConflict)).Once()
	client.On("GetAffinityGroups", mock.Anything, []string{"g1", "g2"}).Return(
		map[string]*pdhttp.AffinityGroupState{"g1": affinityGroupState("g1")}, nil,
	).Once()
	client.On("CreateAffinityGroups", mock.Anything, map[string][]pdhttp.AffinityGroupKeyRange{
		"g2": groups["g2"],
	}, 0).Return(nil, nil).Once()

	require.NoError(t, manager.CreateAffinityGroupsIfNotExists(ctx, groups))
	client.AssertExpectations(t)
}

func TestCreateAffinityGroupsIfNotExistsFallbackForHTTPServiceError(t *testing.T) {
	ctx := context.Background()
	client := &mockPDClient{}
	manager := &pdManager{Client: client}

	groups := map[string][]pdhttp.AffinityGroupKeyRange{
		"g1": {{StartKey: []byte("a"), EndKey: []byte("b")}},
		"g2": {{StartKey: []byte("c"), EndKey: []byte("d")}},
	}
	client.On("CreateAffinityGroups", mock.Anything, groups, 1).Return(nil, pdHTTPServiceErr("mock service error")).Once()
	client.On("GetAffinityGroups", mock.Anything, []string{"g1", "g2"}).Return(
		map[string]*pdhttp.AffinityGroupState{"g1": affinityGroupState("g1")}, nil,
	).Once()
	client.On("CreateAffinityGroups", mock.Anything, map[string][]pdhttp.AffinityGroupKeyRange{
		"g2": groups["g2"],
	}, 0).Return(nil, nil).Once()

	require.NoError(t, manager.CreateAffinityGroupsIfNotExists(ctx, groups))
	client.AssertExpectations(t)
}

func TestCreateAffinityGroupsIfNotExistsDoNotFallbackForNonCompatibilityError(t *testing.T) {
	ctx := context.Background()
	client := &mockPDClient{}
	manager := &pdManager{Client: client}

	groups := map[string][]pdhttp.AffinityGroupKeyRange{
		"g1": {{StartKey: []byte("a"), EndKey: []byte("b")}},
	}
	testErr := pdStatusErr(http.StatusInternalServerError)
	client.On("CreateAffinityGroups", mock.Anything, groups, 1).Return(nil, testErr).Once()

	err := manager.CreateAffinityGroupsIfNotExists(ctx, groups)
	require.ErrorIs(t, err, testErr)
	client.AssertExpectations(t)
	client.AssertNotCalled(t, "GetAffinityGroups", mock.Anything, mock.Anything)
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
	ids := make([]string, 250)
	allGroups := make(map[string]*pdhttp.AffinityGroupState, len(ids)+1)
	for i := range ids {
		ids[i] = fmt.Sprintf("g%03d", i)
		allGroups[ids[i]] = affinityGroupState(ids[i])
	}
	allGroups["unrequested"] = affinityGroupState("unrequested")
	expected := filterAffinityGroups(allGroups, ids)

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
	return &pdhttp.AffinityGroupState{AffinityGroup: pdhttp.AffinityGroup{ID: id}}
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
