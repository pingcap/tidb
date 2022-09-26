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

	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
)

type GlobalConfigTestSuite struct {
	rpc     *RPCClient
	cluster *Cluster
	client  pd.Client
}

func SetUpSuite() *GlobalConfigTestSuite {
	s := &GlobalConfigTestSuite{}
	s.rpc, s.client, s.cluster, _ = New("")
	return s
}

func TestLoad(t *testing.T) {
	s := SetUpSuite()

	s.client.StoreGlobalConfig(context.Background(), []pd.GlobalConfigItem{{Name: "LoadOkGlobalConfig", Value: "ok"}})
	res, err := s.client.LoadGlobalConfig(context.Background(), []string{"LoadOkGlobalConfig", "LoadErrGlobalConfig"})
	require.Equal(t, err, nil)
	for _, j := range res {
		switch j.Name {
		case "/global/config/LoadOkGlobalConfig":
			require.Equal(t, j.Value, "ok")

		case "/global/config/LoadErrGlobalConfig":
			require.Equal(t, j.Value, "")
			require.EqualError(t, j.Error, "not found")
		default:
			require.Equal(t, true, false)
		}
	}
	s.TearDownSuite()
}

func TestStore(t *testing.T) {
	s := SetUpSuite()

	res, err := s.client.LoadGlobalConfig(context.Background(), []string{"NewObject"})
	require.Equal(t, err, nil)
	require.EqualError(t, res[0].Error, "not found")

	err = s.client.StoreGlobalConfig(context.Background(), []pd.GlobalConfigItem{{Name: "NewObject", Value: "ok"}})
	require.Equal(t, err, nil)

	res, err = s.client.LoadGlobalConfig(context.Background(), []string{"NewObject"})
	require.Equal(t, err, nil)
	require.Equal(t, res[0].Error, nil)

	s.TearDownSuite()
}

func TestWatch(t *testing.T) {
	s := SetUpSuite()
	err := s.client.StoreGlobalConfig(context.Background(), []pd.GlobalConfigItem{{Name: "NewObject", Value: "ok"}})
	require.Equal(t, err, nil)

	ch, err := s.client.WatchGlobalConfig(context.Background())
	require.Equal(t, err, nil)

	for i := 0; i < 10; i++ {
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
