package export

import (
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-tools/pkg/filter"
	tf "github.com/pingcap/tidb-tools/pkg/table-filter"
)

var _ = Suite(&testBWListSuite{})

type testBWListSuite struct{}

func (s *testBWListSuite) TestFilterTables(c *C) {
	dbTables := DatabaseTables{}
	expectedDBTables := DatabaseTables{}

	dbTables.AppendTables(filter.InformationSchemaName, []string{"xxx"}...)
	dbTables.AppendTables(strings.ToUpper(filter.PerformanceSchemaName), []string{"xxx"}...)
	dbTables.AppendTables("xxx", []string{"yyy"}...)
	expectedDBTables.AppendTables("xxx", []string{"yyy"}...)
	dbTables.AppendTables("yyy", []string{"xxx"}...)

	tableFilter, err := tf.Parse([]string{"*.*"})
	c.Assert(err, IsNil)
	conf := &Config{
		ServerInfo: ServerInfo{
			ServerType: ServerTypeTiDB,
		},
		Tables:      dbTables,
		TableFilter: tableFilter,
	}

	conf.TableFilter = tf.NewSchemasFilter("xxx")
	filterTables(conf)
	c.Assert(conf.Tables, HasLen, 1)
	c.Assert(conf.Tables, DeepEquals, expectedDBTables)
}

func (s *testBWListSuite) TestFilterDatabaseWithNoTable(c *C) {
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
	filterTables(conf)
	c.Assert(conf.Tables, HasLen, 0)

	dbTables["xxx"] = []*TableInfo{}
	expectedDBTables["xxx"] = []*TableInfo{}
	conf.Tables = dbTables
	conf.TableFilter = tf.NewSchemasFilter("xxx")
	filterTables(conf)
	c.Assert(conf.Tables, HasLen, 1)
	c.Assert(conf.Tables, DeepEquals, expectedDBTables)

	dbTables["xxx"] = []*TableInfo{}
	expectedDBTables = DatabaseTables{}
	conf.Tables = dbTables
	conf.DumpEmptyDatabase = false
	filterTables(conf)
	c.Assert(conf.Tables, HasLen, 0)
	c.Assert(conf.Tables, DeepEquals, expectedDBTables)
}
