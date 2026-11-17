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

package taskkey

import (
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

func TestTaskKeys(t *testing.T) {
	original := config.GetGlobalConfig()
	config.UpdateGlobal(func(c *config.Config) { c.KeyspaceName = "tenant" })
	t.Cleanup(func() { config.StoreGlobalConfig(original) })
	prefix := ""
	explicitPrefix := ""
	if kerneltype.IsNextGen() {
		prefix = "tenant/"
		explicitPrefix = "other/"
	}
	require.Equal(t, prefix+"ImportInto/42", ForImportJob(42))
	require.Equal(t, explicitPrefix+"ImportInto/42", ForImportJobInKeyspace("other", 42))
	require.Equal(t, prefix+"ddl/backfill/42", ForBackfillJob(42, false))
	require.Equal(t, prefix+"ddl/backfill/42/merge", ForBackfillJob(42, true))
	builder := NewBackfillKeyBuilder()
	require.Equal(t, prefix+"ddl/backfill/42", builder.SetMultiSchema(nil).Build(42))
	require.Equal(t, prefix+"ddl/backfill/42/0", builder.SetMultiSchema(&model.MultiSchemaInfo{Seq: 0}).Build(42))
	require.Equal(t, prefix+"ddl/backfill/42/3/merge",
		builder.SetMultiSchema(&model.MultiSchemaInfo{Seq: 3}).SetMergeTempIndex(true).Build(42))
	require.Equal(t, prefix+"ddl/backfill/42/3", builder.SetMultiSchema(nil).SetMergeTempIndex(false).Build(42))
}
