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
