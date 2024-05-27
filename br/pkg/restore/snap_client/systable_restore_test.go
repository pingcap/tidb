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

package snapclient_test

import (
	"math"
	"testing"

	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/gluetidb"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/restore"
	snapclient "github.com/pingcap/tidb/br/pkg/restore/snap_client"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/utiltest"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/stretchr/testify/require"
)

func TestCheckSysTableCompatibility(t *testing.T) {
	cluster := mc
	g := gluetidb.New()
	client := snapclient.NewRestoreClient(cluster.PDClient, cluster.PDHTTPCli, nil, utiltest.DefaultTestKeepaliveCfg)
	err := client.Init(g, cluster.Storage)
	require.NoError(t, err)

	info, err := cluster.Domain.GetSnapshotInfoSchema(math.MaxUint64)
	require.NoError(t, err)
	dbSchema, isExist := info.SchemaByName(model.NewCIStr(mysql.SystemDB))
	require.True(t, isExist)
	tmpSysDB := dbSchema.Clone()
	tmpSysDB.Name = utils.TemporaryDBName(mysql.SystemDB)
	sysDB := model.NewCIStr(mysql.SystemDB)
	userTI, err := restore.GetTableSchema(cluster.Domain, sysDB, model.NewCIStr("user"))
	require.NoError(t, err)

	// user table in cluster have more columns(success)
	mockedUserTI := userTI.Clone()
	userTI.Columns = append(userTI.Columns, &model.ColumnInfo{Name: model.NewCIStr("new-name")})
	err = snapclient.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedUserTI,
	}})
	require.NoError(t, err)
	userTI.Columns = userTI.Columns[:len(userTI.Columns)-1]

	// user table in cluster have less columns(failed)
	mockedUserTI = userTI.Clone()
	mockedUserTI.Columns = append(mockedUserTI.Columns, &model.ColumnInfo{Name: model.NewCIStr("new-name")})
	err = snapclient.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedUserTI,
	}})
	require.True(t, berrors.ErrRestoreIncompatibleSys.Equal(err))

	// column order mismatch(success)
	mockedUserTI = userTI.Clone()
	mockedUserTI.Columns[4], mockedUserTI.Columns[5] = mockedUserTI.Columns[5], mockedUserTI.Columns[4]
	err = snapclient.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedUserTI,
	}})
	require.NoError(t, err)

	// incompatible column type
	mockedUserTI = userTI.Clone()
	mockedUserTI.Columns[0].FieldType.SetFlen(2000) // Columns[0] is `Host` char(255)
	err = snapclient.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedUserTI,
	}})
	require.True(t, berrors.ErrRestoreIncompatibleSys.Equal(err))

	// compatible
	mockedUserTI = userTI.Clone()
	err = snapclient.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedUserTI,
	}})
	require.NoError(t, err)

	// use the mysql.db table to test for column count mismatch.
	dbTI, err := restore.GetTableSchema(cluster.Domain, sysDB, model.NewCIStr("db"))
	require.NoError(t, err)

	// other system tables in cluster have more columns(failed)
	mockedDBTI := dbTI.Clone()
	dbTI.Columns = append(dbTI.Columns, &model.ColumnInfo{Name: model.NewCIStr("new-name")})
	err = snapclient.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedDBTI,
	}})
	require.True(t, berrors.ErrRestoreIncompatibleSys.Equal(err))
}

// NOTICE: Once there is a new system table, BR needs to ensure that it is correctly classified:
//
// - IF it is an unrecoverable table, please add the table name into `unRecoverableTable`.
// - IF it is an system privilege table, please add the table name into `sysPrivilegeTableMap`.
// - IF it is an statistics table, please add the table name into `statsTables`.
//
// The above variables are in the file br/pkg/restore/systable_restore.go
func TestMonitorTheSystemTableIncremental(t *testing.T) {
	require.Equal(t, int64(198), session.CurrentBootstrapVersion)
}
