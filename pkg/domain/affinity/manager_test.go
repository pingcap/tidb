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
	ids := []string{id}
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

func TestAffinityGroupIDsEscapedQueryLenBoundary(t *testing.T) {
	require.Equal(t, maxAffinityGroupIDsQueryLen, affinityGroupIDsEscapedQueryLen([]string{strings.Repeat("/", 1364)}))
	require.Equal(t, maxAffinityGroupIDsQueryLen+3, affinityGroupIDsEscapedQueryLen([]string{strings.Repeat("/", 1365)}))
}

func TestShouldUseGetAllAffinityGroupsByIDCount(t *testing.T) {
	ids := make([]string, maxAffinityGroupIDsCount+1)
	for i := range ids {
		ids[i] = fmt.Sprintf("g%d", i)
	}
	require.True(t, shouldUseGetAllAffinityGroups(ids))
}

func affinityGroupState(id string) *pdhttp.AffinityGroupState {
	return &pdhttp.AffinityGroupState{AffinityGroup: pdhttp.AffinityGroup{ID: id}}
}

func pdStatusErr(statusCode int) error {
	return fmt.Errorf("request pd http api failed with status: '%d %s', body: 'test'", statusCode, http.StatusText(statusCode))
}
