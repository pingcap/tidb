// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metaservice_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/metaservice"
	"github.com/stretchr/testify/require"
)

// TestGetGroup tests the GetGroup function.
func TestGetGroup(t *testing.T) {
	globalMetaAddrs := []string{"127.0.0.1:2379"}
	expectedAddrsStr := "127.0.0.1:2388,127.0.0.1:2389"
	expectedAddrs := strings.Split(expectedAddrsStr, ",")

	// Test case where keyspaceMeta is nil
	keyspaceMetaServiceGroup, err := metaservice.GetGroup(nil, globalMetaAddrs)
	require.Nil(t, keyspaceMetaServiceGroup)
	require.Error(t, err)
	require.True(t, errors.Is(err, metaservice.ErrNilKeyspaceMeta))

	// Test case with a valid group ID and addresses
	keyspaceMeta := &keyspacepb.KeyspaceMeta{
		Config: map[string]string{
			metaservice.GroupIDKey:    "group1",
			metaservice.GroupAddrsKey: expectedAddrsStr,
		},
	}

	keyspaceMetaServiceGroup, err = metaservice.GetGroup(keyspaceMeta, globalMetaAddrs)
	require.NoError(t, err)
	require.Equal(t, "group1", keyspaceMetaServiceGroup.GroupID)

	require.ElementsMatch(t, expectedAddrs, keyspaceMetaServiceGroup.Addrs)

	// Test case with blank entries in addresses
	keyspaceMeta.Config[metaservice.GroupAddrsKey] = " 127.0.0.1:2388, ,127.0.0.1:2389,  "
	keyspaceMetaServiceGroup, err = metaservice.GetGroup(keyspaceMeta, globalMetaAddrs)
	require.NoError(t, err)
	require.Equal(t, "group1", keyspaceMetaServiceGroup.GroupID)
	require.ElementsMatch(t, expectedAddrs, keyspaceMetaServiceGroup.Addrs)

	// Test case where all addresses are blank
	keyspaceMeta.Config[metaservice.GroupAddrsKey] = " , \t,  "
	keyspaceMetaServiceGroup, err = metaservice.GetGroup(keyspaceMeta, globalMetaAddrs)
	require.Nil(t, keyspaceMetaServiceGroup)
	require.Error(t, err)
	require.True(t, errors.Is(err, metaservice.ErrGroupNotMatch))

	// Test case where the group ID exists but addresses do not
	delete(keyspaceMeta.Config, metaservice.GroupAddrsKey)
	keyspaceMetaServiceGroup, err = metaservice.GetGroup(keyspaceMeta, globalMetaAddrs)
	require.Error(t, err)
	require.True(t, errors.Is(err, metaservice.ErrGroupNotMatch))

	// Test case where the group ID does not exist
	delete(keyspaceMeta.Config, metaservice.GroupIDKey)
	keyspaceMetaServiceGroup, err = metaservice.GetGroup(keyspaceMeta, globalMetaAddrs)
	require.NoError(t, err)
	require.Equal(t, metaservice.GlobalGroupID, keyspaceMetaServiceGroup.GroupID)
	require.ElementsMatch(t, globalMetaAddrs, keyspaceMetaServiceGroup.Addrs)
}

func TestGetGroupRejectsInvalidGroupID(t *testing.T) {
	globalMetaAddrs := []string{"127.0.0.1:2379"}
	testCases := []struct {
		name    string
		groupID string
	}{
		{
			name:    "numeric only",
			groupID: "1",
		},
		{
			name:    "contains space",
			groupID: "group 1",
		},
		{
			name:    "contains dot",
			groupID: "group.1",
		},
		{
			name:    "contains only separators and digits",
			groupID: "1-2_3",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			keyspaceMeta := &keyspacepb.KeyspaceMeta{
				Config: map[string]string{
					metaservice.GroupIDKey:    tc.groupID,
					metaservice.GroupAddrsKey: "127.0.0.1:2388,127.0.0.1:2389",
				},
			}

			group, err := metaservice.GetGroup(keyspaceMeta, globalMetaAddrs)
			require.Nil(t, group)
			require.ErrorIs(t, err, metaservice.ErrInvalidGroupID)
			require.ErrorContains(t, err, "invalid meta service group id")
		})
	}
}

// TestGetInfo tests the GetInfo function.
func TestGetInfo(t *testing.T) {
	expectPDAddrs := []string{"127.0.0.1:2380"}
	globalMetaAddrs := []string{"127.0.0.1:2379"}

	// Test case where keyspaceMeta is nil
	metaInfo, err := metaservice.GetInfo(nil, globalMetaAddrs, expectPDAddrs)
	require.NoError(t, err)
	require.NotNil(t, metaInfo)
	require.Equal(t, globalMetaAddrs[0], metaInfo.GlobalAddrs[0])
	require.Equal(t, "0", metaInfo.Group.GroupID)
	require.Equal(t, expectPDAddrs, metaInfo.PDAddrs)

	// Test case with a valid keyspaceMeta
	keyspaceMeta := &keyspacepb.KeyspaceMeta{
		Config: map[string]string{
			metaservice.GroupIDKey:    "group2",
			metaservice.GroupAddrsKey: "127.0.0.1:2388,127.0.0.1:2389",
		},
	}

	metaInfo, err = metaservice.GetInfo(keyspaceMeta, globalMetaAddrs, expectPDAddrs)
	require.NoError(t, err)
	require.NotNil(t, metaInfo)
	require.Equal(t, globalMetaAddrs[0], metaInfo.GlobalAddrs[0])
	require.Equal(t, "group2", metaInfo.Group.GroupID)
	require.Equal(t, expectPDAddrs, metaInfo.PDAddrs)
	expectedAddrs := []string{"127.0.0.1:2388", "127.0.0.1:2389"}
	require.ElementsMatch(t, expectedAddrs, metaInfo.Group.Addrs)
}
