// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"strings"
	"testing"

	tcontext "github.com/pingcap/dumpling/v4/context"
	"github.com/pingcap/tidb-tools/pkg/filter"
	tf "github.com/pingcap/tidb-tools/pkg/table-filter"
	"github.com/stretchr/testify/require"
)

func TestFilterTables(t *testing.T) {
	t.Parallel()

	tctx := tcontext.Background().WithLogger(appLogger)
	dbTables := DatabaseTables{}
	expectedDBTables := DatabaseTables{}

	dbTables.AppendTables(filter.InformationSchemaName, []string{"xxx"}, []uint64{0})
	dbTables.AppendTables(strings.ToUpper(filter.PerformanceSchemaName), []string{"xxx"}, []uint64{0})
	dbTables.AppendTables("xxx", []string{"yyy"}, []uint64{0})
	expectedDBTables.AppendTables("xxx", []string{"yyy"}, []uint64{0})
	dbTables.AppendTables("yyy", []string{"xxx"}, []uint64{0})

	tableFilter, err := tf.Parse([]string{"*.*"})
	require.NoError(t, err)

	conf := &Config{
		ServerInfo: ServerInfo{
			ServerType: ServerTypeTiDB,
		},
		Tables:      dbTables,
		TableFilter: tableFilter,
	}
	databases := []string{filter.InformationSchemaName, filter.PerformanceSchemaName, "xxx", "yyy"}
	require.Equal(t, databases, filterDatabases(tctx, conf, databases))

	conf.TableFilter = tf.NewSchemasFilter("xxx")
	require.Equal(t, []string{"xxx"}, filterDatabases(tctx, conf, databases))

	filterTables(tcontext.Background(), conf)
	require.Len(t, conf.Tables, 1)
	require.Equal(t, expectedDBTables, conf.Tables)
}

func TestFilterDatabaseWithNoTable(t *testing.T) {
	t.Parallel()

	dbTables := DatabaseTables{}
	expectedDBTables := DatabaseTables{}

	dbTables["xxx"] = []*TableInfo{}
	conf := &Config{
		ServerInfo: ServerInfo{
			ServerType: ServerTypeTiDB,
		},
		Tables:            dbTables,
		TableFilter:       tf.NewSchemasFilter("yyy"),
		DumpEmptyDatabase: true,
	}
	filterTables(tcontext.Background(), conf)
	require.Len(t, conf.Tables, 0)

	dbTables["xxx"] = []*TableInfo{}
	expectedDBTables["xxx"] = []*TableInfo{}
	conf.Tables = dbTables
	conf.TableFilter = tf.NewSchemasFilter("xxx")
	filterTables(tcontext.Background(), conf)
	require.Len(t, conf.Tables, 1)
	require.Equal(t, expectedDBTables, conf.Tables)

	dbTables["xxx"] = []*TableInfo{}
	expectedDBTables = DatabaseTables{}
	conf.Tables = dbTables
	conf.DumpEmptyDatabase = false
	filterTables(tcontext.Background(), conf)
	require.Len(t, conf.Tables, 0)
	require.Equal(t, expectedDBTables, conf.Tables)
}
