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

package unistore

import (
	"context"
	"testing"

	"github.com/asaskevich/govalidator"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/constants"
)

type GlobalConfigTestSuite struct {
	rpc     *RPCClient
	cluster *Cluster
	client  pd.Client
}

func SetUpSuite() *GlobalConfigTestSuite {
	s := &GlobalConfigTestSuite{}
	s.rpc, s.client, s.cluster, _ = New("", nil, constants.NullKeyspaceID, nil)
	return s
}

func TestLoad(t *testing.T) {
	s := SetUpSuite()
	err := s.client.StoreGlobalConfig(context.Background(), "", []pd.GlobalConfigItem{{Name: "LoadOkGlobalConfig", Value: "ok"}})
	require.Equal(t, nil, err)
	res, _, err := s.client.LoadGlobalConfig(context.Background(), []string{"LoadOkGlobalConfig", "LoadErrGlobalConfig"}, "")
	require.Equal(t, err, nil)
	for _, j := range res {
		println(j.Name)
		switch j.Name {
		case "/global/config/LoadOkGlobalConfig":
			require.Equal(t, "ok", j.Value)
		case "/global/config/LoadErrGlobalConfig":
			require.Equal(t, "", j.Value)
		default:
			require.Equal(t, true, false)
		}
	}
	s.TearDownSuite()
}

func TestStore(t *testing.T) {
	s := SetUpSuite()

	res, _, err := s.client.LoadGlobalConfig(context.Background(), []string{"NewObject"}, "")
	require.Equal(t, err, nil)
	require.Equal(t, res[0].Value, "")

	err = s.client.StoreGlobalConfig(context.Background(), "", []pd.GlobalConfigItem{{Name: "NewObject", Value: "ok"}})
	require.Equal(t, err, nil)

	res, _, err = s.client.LoadGlobalConfig(context.Background(), []string{"NewObject"}, "")
	require.Equal(t, err, nil)
	require.Equal(t, res[0].Value, "ok")

	s.TearDownSuite()
}

func TestWatch(t *testing.T) {
	s := SetUpSuite()
	err := s.client.StoreGlobalConfig(context.Background(), "/global/config", []pd.GlobalConfigItem{{Name: "NewObject", Value: "ok"}})
	require.Equal(t, err, nil)

	ch, err := s.client.WatchGlobalConfig(context.Background(), "/global/config", 0)
	require.Equal(t, err, nil)

	for range 10 {
		res := <-ch
		require.NotEqual(t, res[0].Value, "")
	}
	close(ch)

	s.TearDownSuite()
}

func (s *GlobalConfigTestSuite) TearDownSuite() {
	s.client.Close()
	s.rpc.Close()
	s.cluster.Close()
}

func TestMockPDServiceDiscovery(t *testing.T) {
	re := require.New(t)
	pdAddrs := []string{"invalid_pd_address", "127.0.0.1:2379", "http://172.32.21.32:2379"}
	for i, addr := range pdAddrs {
		check := govalidator.IsURL(addr)
		if i > 0 {
			re.True(check)
		} else {
			re.False(check)
		}
	}
	sd := NewMockPDServiceDiscovery(pdAddrs)
	clis := sd.GetAllServiceClients()
	re.Len(clis, 2)
	re.Equal(clis[0].GetURL(), "http://127.0.0.1:2379")
	re.Equal(clis[1].GetURL(), "http://172.32.21.32:2379")
}

func TestMockKeyspaceManager(t *testing.T) {
	re := require.New(t)

	checkElements := func(m *mockKeyspaceManager, ids []uint32, names []string) {
		re.NotNil(m)
		re.Equal(len(ids), len(names))
		re.Equalf(len(ids), len(m.keyspaces), "amount of keyspace meta mismatches, meta: %+v, expected IDs: %v", m.keyspaces, ids)
		re.Equalf(len(ids), len(m.keyspaceNamesMap), "amount of keyspace name map entries mismatches, keyspace name map: %+v, expected IDs: %v", m.keyspaceNamesMap, ids)
		for i, keyspace := range m.keyspaces {
			// The array should be sorted by ID, and the ID is distinct.
			if i > 0 {
				re.Greater(keyspace.Id, m.keyspaces[i-1].Id)
			}
			re.Equal(ids[i], keyspace.Id)
			re.Equal(names[i], keyspace.Name)
			nameMapEntry, exists := m.keyspaceNamesMap[keyspace.Name]
			re.True(exists)
			re.Equal(keyspace.Id, nameMapEntry)
		}
	}

	mustLoadKeyspace := func(m *mockKeyspaceManager, name string, expectedExists bool, expectedID uint32) {
		meta, err := m.LoadKeyspace(context.Background(), name)
		if expectedExists {
			re.NoError(err)
			re.Equal(name, meta.Name)
			re.Equal(expectedID, meta.Id)
		} else {
			re.Error(err)
			re.Contains(err.Error(), pdpb.ErrorType_ENTRY_NOT_FOUND.String())
		}
	}

	mustListKeyspaces := func(m *mockKeyspaceManager, startID uint32, limit int, expectedIDs []uint32, expectedNames []string) {
		re.Equal(len(expectedIDs), len(expectedNames))
		keyspaces, err := m.GetAllKeyspaces(context.Background(), startID, uint32(limit))
		re.NoError(err)
		re.Len(keyspaces, len(expectedIDs))
		for i, keyspace := range keyspaces {
			re.Equal(expectedIDs[i], keyspace.Id)
			re.Equal(expectedNames[i], keyspace.Name)
		}
	}

	m, err := newMockKeyspaceManager(nil)
	re.NoError(err)
	checkElements(m, []uint32{}, []string{})
	mustLoadKeyspace(m, "DEFAULT", false, 0)
	mustListKeyspaces(m, 0, 0, []uint32{}, []string{})

	m, err = newMockKeyspaceManager([]*keyspacepb.KeyspaceMeta{{
		Id:   0,
		Name: "DEFAULT",
	}})
	re.NoError(err)
	checkElements(m, []uint32{0}, []string{"DEFAULT"})
	mustLoadKeyspace(m, "DEFAULT", true, 0)
	mustLoadKeyspace(m, "ks1", false, 0)
	mustListKeyspaces(m, 0, 0, []uint32{0}, []string{"DEFAULT"})
	mustListKeyspaces(m, 1, 0, []uint32{}, []string{})

	m, err = newMockKeyspaceManager([]*keyspacepb.KeyspaceMeta{
		{Id: 1, Name: "ks1"},
		{Id: 4, Name: "ks4"},
		{Id: 2, Name: "ks2"},
		{Id: 5, Name: "ks5"},
		{Id: 3, Name: "ks3"},
	})
	re.NoError(err)
	checkElements(m, []uint32{1, 2, 3, 4, 5}, []string{"ks1", "ks2", "ks3", "ks4", "ks5"})
	for i, name := range []string{"ks1", "ks2", "ks3", "ks4", "ks5"} {
		mustLoadKeyspace(m, name, true, uint32(i+1))
	}
	mustLoadKeyspace(m, "ks6", false, 0)
	mustListKeyspaces(m, 0, 0, []uint32{1, 2, 3, 4, 5}, []string{"ks1", "ks2", "ks3", "ks4", "ks5"})
	mustListKeyspaces(m, 0, 3, []uint32{1, 2, 3}, []string{"ks1", "ks2", "ks3"})
	mustListKeyspaces(m, 1, 0, []uint32{1, 2, 3, 4, 5}, []string{"ks1", "ks2", "ks3", "ks4", "ks5"})
	mustListKeyspaces(m, 3, 0, []uint32{3, 4, 5}, []string{"ks3", "ks4", "ks5"})
	mustListKeyspaces(m, 3, 2, []uint32{3, 4}, []string{"ks3", "ks4"})
	mustListKeyspaces(m, 5, 0, []uint32{5}, []string{"ks5"})

	m, err = newMockKeyspaceManager([]*keyspacepb.KeyspaceMeta{
		{Id: 100, Name: "ks100"},
		{Id: 1, Name: "ks1"},
		{Id: constants.MaxKeyspaceID, Name: "lastks"},
		{Id: 10, Name: "ks10"},
	})
	re.NoError(err)
	checkElements(m, []uint32{1, 10, 100, constants.MaxKeyspaceID}, []string{"ks1", "ks10", "ks100", "lastks"})
	mustListKeyspaces(m, 0, 0, []uint32{1, 10, 100, constants.MaxKeyspaceID}, []string{"ks1", "ks10", "ks100", "lastks"})
	mustListKeyspaces(m, 5, 0, []uint32{10, 100, constants.MaxKeyspaceID}, []string{"ks10", "ks100", "lastks"})
	mustListKeyspaces(m, 5, 1, []uint32{10}, []string{"ks10"})
	mustListKeyspaces(m, 10, 0, []uint32{10, 100, constants.MaxKeyspaceID}, []string{"ks10", "ks100", "lastks"})
	mustListKeyspaces(m, 11, 0, []uint32{100, constants.MaxKeyspaceID}, []string{"ks100", "lastks"})
	mustListKeyspaces(m, 99, 0, []uint32{100, constants.MaxKeyspaceID}, []string{"ks100", "lastks"})
	mustListKeyspaces(m, 101, 0, []uint32{constants.MaxKeyspaceID}, []string{"lastks"})
	mustListKeyspaces(m, constants.MaxKeyspaceID, 0, []uint32{constants.MaxKeyspaceID}, []string{"lastks"})

	// Rejects duplicated ID.
	_, err = newMockKeyspaceManager([]*keyspacepb.KeyspaceMeta{
		{Id: 1, Name: "ks1"},
		{Id: 2, Name: "ks2"},
		{Id: 3, Name: "ks3"},
		{Id: 1, Name: "ks4"},
	})
	re.Error(err)

	// Rejects duplicated name.
	_, err = newMockKeyspaceManager([]*keyspacepb.KeyspaceMeta{
		{Id: 1, Name: "ks1"},
		{Id: 2, Name: "ks2"},
		{Id: 3, Name: "ks3"},
		{Id: 4, Name: "ks1"},
	})
	re.Error(err)

	_, err = newMockKeyspaceManager([]*keyspacepb.KeyspaceMeta{
		// Exceeds the current value of max allowed keyspace id
		{Id: 0x1000000, Name: "illegal"},
	})
	re.Error(err)

	_, err = newMockKeyspaceManager([]*keyspacepb.KeyspaceMeta{
		// Null keyspace id is not allowed
		{Id: constants.NullKeyspaceID, Name: ""},
	})
	re.Error(err)
}
