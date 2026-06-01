// Copyright 2025 PingCAP, Inc.
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

// TestGetKeyspaceMetaServiceGroup tests the GetKeyspaceMetaServiceGroup function.
func TestGetKeyspaceMetaServiceGroup(t *testing.T) {
	globalMetaAddrs := []string{"127.0.0.1:2379"}
	expectedAddrsStr := "127.0.0.1:2388,127.0.0.1:2389"
	expectedAddrs := strings.Split(expectedAddrsStr, ",")

	// Test case where keyspaceMeta is nil
	keyspaceMetaServiceGroup, err := metaservice.GetKeyspaceMetaServiceGroup(nil, globalMetaAddrs)
	require.Nil(t, keyspaceMetaServiceGroup)
	require.Error(t, err)
	require.True(t, errors.Is(err, metaservice.ErrNilKeyspaceMeta))

	// Test case with a valid group ID and addresses
	keyspaceMeta := &keyspacepb.KeyspaceMeta{
		Config: map[string]string{
			metaservice.KeyspaceMetaGroupIDKey:    "1", // Valid numeric string
			metaservice.KeyspaceMetaGroupAddrsKey: expectedAddrsStr,
		},
	}

	keyspaceMetaServiceGroup, err = metaservice.GetKeyspaceMetaServiceGroup(keyspaceMeta, globalMetaAddrs)
	require.NoError(t, err)
	require.Equal(t, "1", keyspaceMetaServiceGroup.GroupID)

	require.ElementsMatch(t, expectedAddrs, keyspaceMetaServiceGroup.KeyspaceMetaServiceAddrs)

	// Test case with blank entries in addresses
	keyspaceMeta.Config[metaservice.KeyspaceMetaGroupAddrsKey] = " 127.0.0.1:2388, ,127.0.0.1:2389,  "
	keyspaceMetaServiceGroup, err = metaservice.GetKeyspaceMetaServiceGroup(keyspaceMeta, globalMetaAddrs)
	require.NoError(t, err)
	require.Equal(t, "1", keyspaceMetaServiceGroup.GroupID)
	require.ElementsMatch(t, expectedAddrs, keyspaceMetaServiceGroup.KeyspaceMetaServiceAddrs)

	// Test case where all addresses are blank
	keyspaceMeta.Config[metaservice.KeyspaceMetaGroupAddrsKey] = " , \t,  "
	keyspaceMetaServiceGroup, err = metaservice.GetKeyspaceMetaServiceGroup(keyspaceMeta, globalMetaAddrs)
	require.Nil(t, keyspaceMetaServiceGroup)
	require.Error(t, err)
	require.True(t, errors.Is(err, metaservice.ErrGroupNotMatch))

	// Test case where the group ID exists but addresses do not
	delete(keyspaceMeta.Config, metaservice.KeyspaceMetaGroupAddrsKey)
	keyspaceMetaServiceGroup, err = metaservice.GetKeyspaceMetaServiceGroup(keyspaceMeta, globalMetaAddrs)
	require.Error(t, err)
	require.True(t, errors.Is(err, metaservice.ErrGroupNotMatch))

	// Test case where the group ID does not exist
	delete(keyspaceMeta.Config, metaservice.KeyspaceMetaGroupIDKey)
	keyspaceMetaServiceGroup, err = metaservice.GetKeyspaceMetaServiceGroup(keyspaceMeta, globalMetaAddrs)
	require.NoError(t, err)
	require.Equal(t, metaservice.GlobalGroupID, keyspaceMetaServiceGroup.GroupID)
	require.ElementsMatch(t, globalMetaAddrs, keyspaceMetaServiceGroup.KeyspaceMetaServiceAddrs)
}

// TestGetMetaServiceInfo tests the GetMetaServiceInfo function.
func TestGetMetaServiceInfo(t *testing.T) {
	expectPDAddrs := []string{"127.0.0.1:2380"}
	globalMetaAddrs := []string{"127.0.0.1:2379"}

	// Test case where keyspaceMeta is nil
	metaInfo, err := metaservice.GetMetaServiceInfo(nil, globalMetaAddrs, expectPDAddrs)
	require.NoError(t, err)
	require.NotNil(t, metaInfo)
	require.Equal(t, globalMetaAddrs[0], metaInfo.GlobalMetaServiceAddrs[0])
	require.Equal(t, "0", metaInfo.KeyspaceMetaGroup.GroupID)
	require.Equal(t, expectPDAddrs, metaInfo.PDAddrs)

	// Test case with a valid keyspaceMeta
	keyspaceMeta := &keyspacepb.KeyspaceMeta{
		Config: map[string]string{
			metaservice.KeyspaceMetaGroupIDKey:    "2", // Valid numeric string
			metaservice.KeyspaceMetaGroupAddrsKey: "127.0.0.1:2388,127.0.0.1:2389",
		},
	}

	metaInfo, err = metaservice.GetMetaServiceInfo(keyspaceMeta, globalMetaAddrs, expectPDAddrs)
	require.NoError(t, err)
	require.NotNil(t, metaInfo)
	require.Equal(t, globalMetaAddrs[0], metaInfo.GlobalMetaServiceAddrs[0])
	require.Equal(t, "2", metaInfo.KeyspaceMetaGroup.GroupID)
	require.Equal(t, expectPDAddrs, metaInfo.PDAddrs)
	expectedAddrs := []string{"127.0.0.1:2388", "127.0.0.1:2389"}
	require.ElementsMatch(t, expectedAddrs, metaInfo.KeyspaceMetaGroup.KeyspaceMetaServiceAddrs)
}
