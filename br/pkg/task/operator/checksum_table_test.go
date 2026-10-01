// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package operator

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/br/pkg/conn"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

type pitrIDMapTestGlue struct {
	glue.Glue
	se *pitrIDMapTestSession
}

func (g *pitrIDMapTestGlue) CreateSession(kv.Storage) (glue.Session, error) {
	return g.se, nil
}

type pitrIDMapTestSession struct {
	glue.Session
	ctx    sessionctx.Context
	closed bool
}

func (s *pitrIDMapTestSession) GetSessionCtx() sessionctx.Context { return s.ctx }
func (s *pitrIDMapTestSession) Close()                            { s.closed = true }

func TestLoadPitrIDMapQueryError(t *testing.T) {
	_, dom := testkit.CreateMockStoreAndDomain(t)
	// The mock context returns "Not Supported" from ExecRestrictedSQL, after
	// CreateSession has succeeded. That query error must not become (nil, nil).
	se := &pitrIDMapTestSession{ctx: mock.NewContext()}
	c := &checksumTableCtx{dom: dom, mgr: &conn.Mgr{}}
	maps, err := c.loadPitrIdMap(context.Background(), &pitrIDMapTestGlue{se: se}, 123, 456)
	require.True(t, se.closed)
	require.Nil(t, maps)
	require.ErrorContains(t, err, "failed to get pitr id map from mysql.tidb_pitr_id_map")
	require.ErrorContains(t, err, "Not Supported")
}
