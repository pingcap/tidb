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

package ddl

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func TestCreateExistingReadOnlySchemaWithMixedVersions(t *testing.T) {
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/domain/infosync/mockGetAllServerInfo", "return(`{\"old\":{}}`)")
	cache := infoschema.NewCache(nil, 1)
	cache.Insert(infoschema.MockInfoSchema(nil), 0)
	job := &model.Job{Version: model.GetJobVerInUse(), Type: model.ActionCreateSchema, SchemaID: 42}
	job.FillArgs(&model.CreateSchemaArgs{DBInfo: &model.DBInfo{Name: pmodel.NewCIStr("test"), ReadOnly: true}})
	_, err := job.Encode(true)
	require.NoError(t, err)
	_, err = onCreateSchema(&jobContext{ctx: context.Background(), infoCache: cache}, job)
	require.True(t, infoschema.ErrDatabaseExists.Equal(err), "%v", err)
	require.Equal(t, model.JobStateCancelled, job.State)
}
