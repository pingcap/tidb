// Copyright 2024 PingCAP, Inc.
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

package utiltest

import (
	"testing"

	gluemock "github.com/pingcap/tidb/br/pkg/gluetidb/mock"
	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/stretchr/testify/require"
)

type TestRestoreSchemaSuite struct {
	Mock     *mock.Cluster
	MockGlue *gluemock.MockGlue
	Storage  storage.ExternalStorage
}

func CreateRestoreSchemaSuite(t *testing.T) *TestRestoreSchemaSuite {
	var err error
	s := new(TestRestoreSchemaSuite)
	s.MockGlue = &gluemock.MockGlue{}
	s.Mock, err = mock.NewCluster()
	require.NoError(t, err)
	base := t.TempDir()
	s.Storage, err = storage.NewLocalStorage(base)
	require.NoError(t, err)
	require.NoError(t, s.Mock.Start())
	t.Cleanup(func() {
		s.Mock.Stop()
	})
	return s
}
