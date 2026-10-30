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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/pingcap/tidb/dumpling/dumpservice"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/testkit"
	tf "github.com/pingcap/tidb/pkg/util/table-filter"
	"github.com/stretchr/testify/require"
)

func TestKVMetadataSnapshotMatchesSQLMetadata(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@sql_mode = default, @@time_zone = '+00:00'")
	tk.MustExec("create placement policy metadata_policy followers=2")
	tk.MustExec("create database metadata charset utf8mb4 collate utf8mb4_bin placement policy=metadata_policy")
	tk.MustExec("create table metadata.shared (id bigint primary key auto_increment) auto_id_cache=1000 auto_increment=201 placement policy=metadata_policy")
	tk.MustExec("create table metadata.separate (id bigint unsigned primary key auto_increment) auto_id_cache=1 auto_increment=701")
	tk.MustExec("create table metadata.random (id bigint primary key auto_random(5)) auto_random_base=901")
	tk.MustExec(`create table metadata.partitioned (id int primary key)
		partition by range (id) (
			partition p0 values less than (10) placement policy metadata_policy,
			partition p1 values less than (maxvalue))`)

	tk.MustExec("create database metadata_reference")
	tk.MustExec("create table metadata_reference.parent (id bigint primary key)")
	// Create defaults in another time zone, then compare the UTC SHOW output.
	tk.MustExec("set @@time_zone = '+08:00'")
	tk.MustExec(`create table metadata.referencing (
		id bigint primary key,
		local_id bigint,
		remote_id bigint,
		created timestamp(6) default '2026-01-02 03:04:05.123456',
		updated timestamp(3) default current_timestamp(3) on update current_timestamp(3),
		constraint local_fk foreign key (local_id) references metadata.shared (id),
		constraint remote_fk foreign key (remote_id) references metadata_reference.parent (id))`)
	tk.MustExec("set @@time_zone = '+00:00'")

	wantTables := make(map[string]string)
	for _, name := range []string{"shared", "separate", "random", "partitioned", "referencing"} {
		wantTables[name] = tk.MustQuery("show create table metadata." + name).Rows()[0][1].(string)
	}
	wantPolicy := tk.MustQuery("show create placement policy metadata_policy").Rows()[0][1].(string)
	source := store.GetSnapshot(kv.MaxVersion)
	sourceReader := meta.NewReader(source)
	service := newMetadataSnapshotTestService(t, source, "")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	snapshot, err := newKVMetadataSnapshot(ctx, service.client)
	require.NoError(t, err)
	reader := meta.NewReader(snapshot)

	// Exercise the actual metadata consumers together: hash iteration, point
	// reads, versioned policy decoding, AutoID accessors, and schema rendering.
	checkMetadata := func() {
		t.Helper()
		databases, err := reader.ListDatabases()
		require.NoError(t, err)
		var databaseID int64
		for _, database := range databases {
			if database.Name.O == "metadata" {
				databaseID = database.ID
				break
			}
		}
		require.NotZero(t, databaseID)
		database, err := reader.GetDatabase(databaseID)
		require.NoError(t, err)
		require.NotNil(t, database.PlacementPolicyRef)
		require.Equal(t, "metadata_policy", database.PlacementPolicyRef.Name.O)
		tables, err := reader.ListTables(context.Background(), databaseID)
		require.NoError(t, err)
		require.Len(t, tables, len(wantTables))
		for _, listed := range tables {
			table, err := reader.GetTable(databaseID, listed.ID)
			require.NoError(t, err)
			createSQL, err := kvCreateTableSQL(reader, database, table)
			require.NoError(t, err)
			want := wantTables[table.Name.O]
			if table.SepAutoInc() {
				// SQL reads the live AutoID service's base; a snapshot export
				// restores from the persisted reservation end instead.
				base, err := sourceReader.GetAutoIDAccessors(databaseID, table.ID).IncrementID(table.Version).Get()
				require.NoError(t, err)
				require.Contains(t, want, " AUTO_INCREMENT=701")
				want = strings.Replace(want, " AUTO_INCREMENT=701", fmt.Sprintf(" AUTO_INCREMENT=%d", base+1), 1)
			}
			require.Equal(t, want, createSQL, table.Name.O)
		}
		policies, err := reader.ListPolicies()
		require.NoError(t, err)
		require.Len(t, policies, 1)
		require.Equal(t, wantPolicy, executor.ConstructResultOfShowCreatePlacementPolicy(policies[0]))
	}
	checkMetadata()
	// Repeating the complete consumer workflow uses the loaded view even after
	// its scan context is canceled; it must neither rescan nor change its SQL.
	cancel()
	checkMetadata()
	require.Equal(t, int64(1), service.discoveries.Load())
	require.Equal(t, int64(1), service.scans.Load())
}

func TestKVMetadataSnapshotFailureAbortsExport(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database metadata")
	tk.MustExec("create table metadata.t (id int)")
	// The service sends valid metadata, then reports that the scan failed.
	service := newMetadataSnapshotTestService(t, store.GetSnapshot(kv.MaxVersion), "snapshot unavailable")
	conf := DefaultConfig()
	conf.DumpService = service.address
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
	require.Equal(t, int64(1), service.discoveries.Load())
	require.Equal(t, int64(1), service.scans.Load())
}

type metadataSnapshotTestService struct {
	client      *dumpservice.Client
	address     string
	discoveries atomic.Int64
	scans       atomic.Int64
}

// Serve one whole metadata shard in reverse order. This makes every metadata
// consumer share the same expensive scan and exercises stream buffer ownership.
func newMetadataSnapshotTestService(t *testing.T, source kv.Snapshot, completionError string) *metadataSnapshotTestService {
	t.Helper()
	service := &metadataSnapshotTestService{}
	socketPath := startTestDumpService(t, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		start, end, err := decodeKVTestRange(request)
		if err != nil || string(start) != "m" || string(end) != "n" {
			http.Error(writer, "expected the whole metadata shard [m, n)", http.StatusBadRequest)
			return
		}
		if request.URL.Path == "/shards" {
			service.discoveries.Add(1)
			writer.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(writer).Encode(testShardsResponse{Ranges: []testHexRange{{
				StartKeyHex: hex.EncodeToString(start), EndKeyHex: hex.EncodeToString(end),
			}}}); err != nil {
				http.Error(writer, err.Error(), http.StatusInternalServerError)
			}
			return
		}
		if request.URL.Path != "/data" {
			http.NotFound(writer, request)
			return
		}
		service.scans.Add(1)
		iter, err := source.IterReverse(kv.Key(end), kv.Key(start))
		if err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			return
		}
		defer iter.Close()
		writer.Header().Add("Trailer", "x-dumper-scan-status")
		writer.Header().Add("Trailer", "x-dumper-scan-error")
		for iter.Valid() {
			if err := writeKVTestRow(writer, iter.Key(), iter.Value()); err != nil {
				return
			}
			if err := iter.Next(); err != nil {
				writer.Header().Set("x-dumper-scan-status", "failed")
				writer.Header().Set("x-dumper-scan-error", url.QueryEscape(err.Error()))
				return
			}
		}
		if completionError != "" {
			writer.Header().Set("x-dumper-scan-status", "failed")
			writer.Header().Set("x-dumper-scan-error", url.QueryEscape(completionError))
			return
		}
		writer.Header().Set("x-dumper-scan-status", "complete")
	}))
	service.address = (&url.URL{Scheme: "unix", Path: socketPath}).String()
	client, err := dumpservice.NewClient(service.address)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	service.client = client
	return service
}
