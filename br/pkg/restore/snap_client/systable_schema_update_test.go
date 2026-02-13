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

package snapclient_test

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/gluetidb"
	snapclient "github.com/pingcap/tidb/br/pkg/restore/snap_client"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

const (
	CreateStatsMeta2SQL = `CREATE TABLE IF NOT EXISTS %s.stats_meta (
		version							BIGINT(64) UNSIGNED NOT NULL,
		table_id						BIGINT(64) NOT NULL,
		modify_count					BIGINT(64) NOT NULL DEFAULT 0,
		count							BIGINT(64) UNSIGNED NOT NULL DEFAULT 0,
		snapshot						BIGINT(64) UNSIGNED NOT NULL DEFAULT 0,
		last_stats_histograms_version	BIGINT(64) UNSIGNED DEFAULT NULL,
		INDEX idx_ver(version),
		UNIQUE INDEX tbl(table_id)
	);`
	CreateStatsMeta1SQL = `CREATE TABLE IF NOT EXISTS %s.stats_meta (
		version			BIGINT(64) UNSIGNED NOT NULL,
		table_id		BIGINT(64) NOT NULL,
		modify_count	BIGINT(64) NOT NULL DEFAULT 0,
		count			BIGINT(64) UNSIGNED NOT NULL DEFAULT 0,
		snapshot		BIGINT(64) UNSIGNED NOT NULL DEFAULT 0,
		INDEX idx_ver(version),
		UNIQUE INDEX tbl(table_id)
	);`
)

func TestGetSchemaVersionFromStatsMeta(t *testing.T) {
	cluster := mc
	ctx := context.Background()
	g := gluetidb.New()
	se, err := g.CreateSession(cluster.Storage)
	require.NoError(t, err)
	err = se.ExecuteInternal(ctx, "create database test_ver2")
	require.NoError(t, err)
	err = se.ExecuteInternal(ctx, "create database test_ver1")
	require.NoError(t, err)
	err = se.ExecuteInternal(ctx, "create database __TiDB_BR_Temporary_mysql")
	require.NoError(t, err)
	err = se.ExecuteInternal(ctx, fmt.Sprintf(CreateStatsMeta2SQL, "test_ver2"))
	require.NoError(t, err)
	err = se.ExecuteInternal(ctx, fmt.Sprintf(CreateStatsMeta1SQL, "test_ver1"))
	require.NoError(t, err)

	info, err := cluster.Domain.GetSnapshotInfoSchema(math.MaxUint64)
	require.NoError(t, err)
	downstreamStatsMeta2, err := info.TableInfoByName(ast.NewCIStr("test_ver2"), ast.NewCIStr("stats_meta"))
	require.NoError(t, err)
	downstreamStatsMeta1, err := info.TableInfoByName(ast.NewCIStr("test_ver1"), ast.NewCIStr("stats_meta"))
	require.NoError(t, err)

	// case 1: upstream version is Version2
	err = se.ExecuteInternal(ctx, fmt.Sprintf(CreateStatsMeta2SQL, "__TiDB_BR_Temporary_mysql"))
	require.NoError(t, err)
	info, err = cluster.Domain.GetSnapshotInfoSchema(math.MaxUint64)
	require.NoError(t, err)
	upstreamStatsMeta2, err := info.TableInfoByName(ast.NewCIStr("__TiDB_BR_Temporary_mysql"), ast.NewCIStr("stats_meta"))
	require.NoError(t, err)
	require.Equal(t, snapclient.Version2, snapclient.GetSchemaVersionFromStatsMeta(downstreamStatsMeta2))
	// case 1-1: downstream version is Version2, nothing happens
	err = snapclient.UpdateStatsMetaSchema(ctx, downstreamStatsMeta2, upstreamStatsMeta2, func(ctx context.Context, sql string) error {
		return errors.Errorf("should not execute any sql: %s", sql)
	})
	require.NoError(t, err)
	// case 2-1: downstream version is Version1, add last_stats_histograms_version column
	err = snapclient.UpdateStatsMetaSchema(ctx, downstreamStatsMeta1, upstreamStatsMeta2, func(ctx context.Context, sql string) error {
		return se.ExecuteInternal(ctx, sql)
	})
	require.NoError(t, err)
	info, err = cluster.Domain.GetSnapshotInfoSchema(math.MaxUint64)
	require.NoError(t, err)
	newUpstreamStatsMeta2, err := info.TableInfoByName(ast.NewCIStr("__TiDB_BR_Temporary_mysql"), ast.NewCIStr("stats_meta"))
	require.NoError(t, err)
	require.Equal(t, snapclient.Version1, snapclient.GetSchemaVersionFromStatsMeta(newUpstreamStatsMeta2))
	err = se.ExecuteInternal(ctx, "DROP TABLE __TiDB_BR_Temporary_mysql.stats_meta")
	require.NoError(t, err)

	// case 2: upstream version is Version1
	err = se.ExecuteInternal(ctx, fmt.Sprintf(CreateStatsMeta1SQL, "__TiDB_BR_Temporary_mysql"))
	require.NoError(t, err)
	info, err = cluster.Domain.GetSnapshotInfoSchema(math.MaxUint64)
	require.NoError(t, err)
	upstreamStatsMeta1, err := info.TableInfoByName(ast.NewCIStr("__TiDB_BR_Temporary_mysql"), ast.NewCIStr("stats_meta"))
	require.NoError(t, err)
	require.Equal(t, snapclient.Version1, snapclient.GetSchemaVersionFromStatsMeta(downstreamStatsMeta1))
	// case 2-1: downstream version is Version1, nothing happens
	err = snapclient.UpdateStatsMetaSchema(ctx, downstreamStatsMeta1, upstreamStatsMeta1, func(ctx context.Context, sql string) error {
		return errors.Errorf("should not execute any sql: %s", sql)
	})
	require.NoError(t, err)
	// case 2-2: downstream version is Version2, add last_stats_histograms_version column
	err = snapclient.UpdateStatsMetaSchema(ctx, downstreamStatsMeta2, upstreamStatsMeta1, func(ctx context.Context, sql string) error {
		return se.ExecuteInternal(ctx, sql)
	})
	require.NoError(t, err)
	info, err = cluster.Domain.GetSnapshotInfoSchema(math.MaxUint64)
	require.NoError(t, err)
	newUpstreamStatsMeta1, err := info.TableInfoByName(ast.NewCIStr("__TiDB_BR_Temporary_mysql"), ast.NewCIStr("stats_meta"))
	require.NoError(t, err)
	require.Equal(t, snapclient.Version2, snapclient.GetSchemaVersionFromStatsMeta(newUpstreamStatsMeta1))
	err = se.ExecuteInternal(ctx, "DROP TABLE __TiDB_BR_Temporary_mysql.stats_meta")
	require.NoError(t, err)
}
