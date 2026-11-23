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

package export

import (
	"context"
	"os"
	"testing"

	"github.com/pingcap/tidb/dumpling/internal/dumpservicetest"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/testkit"
	tf "github.com/pingcap/tidb/pkg/util/table-filter"
	"github.com/stretchr/testify/require"
)

func TestKVMetadataSnapshotFailureAbortsExport(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database metadata")
	tk.MustExec("create table metadata.t (id int)")
	// The service sends valid metadata, then reports that the scan failed.
	service := dumpservicetest.NewMetadataService(t, store.GetSnapshot(kv.MaxVersion), "snapshot unavailable")
	conf := DefaultConfig()
	conf.DumpService = service.Address
	conf.OutputDirPath = t.TempDir()
	conf.StatusAddr = ""
	conf.NoData = true
	conf.TableFilter = tf.NewSchemasFilter("metadata")
	dumper, err := NewDumper(context.Background(), conf)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dumper.Close()) })
	require.ErrorContains(t, dumper.Dump(), "snapshot unavailable")
	entries, err := os.ReadDir(conf.OutputDirPath)
	require.NoError(t, err)
	require.Empty(t, entries, "an incomplete metadata stream must not produce schema files")
	require.Equal(t, int64(1), service.Discoveries.Load())
	require.Equal(t, int64(1), service.Scans.Load())
}
