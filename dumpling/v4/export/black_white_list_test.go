package export

import (
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"github.com/pingcap/tidb/util"
)

var _ = Suite(&testBWListSuite{})

type testBWListSuite struct{}

func (s *testBWListSuite) TestBWList(c *C) {
	nopeBWList, err := NewBWList(BWListConf{})
	c.Assert(err, IsNil)

	c.Assert(nopeBWList.Apply("nope", "nope"), IsTrue)

	mysqlReplicationBWList, err := NewBWList(BWListConf{
		Mode: MySQLReplicationMode,
		Rules: &MySQLReplicationConf{
			Rules: &filter.Rules{
				DoDBs: []string{"xxx"},
			},
		},
	})
	c.Assert(err, IsNil)

	c.Assert(mysqlReplicationBWList.Apply("xxx", "yyy"), IsTrue)
	c.Assert(mysqlReplicationBWList.Apply("yyy", "xxx"), IsFalse)

	_, err = NewBWList(BWListConf{
		Mode: MySQLReplicationMode,
		Rules: &MySQLReplicationConf{
			Rules: &filter.Rules{
				DoDBs: []string{""},
			},
		},
	})
	c.Assert(err, NotNil)
}

func (s *testBWListSuite) TestFilterTables(c *C) {
	dbTables := DatabaseTables{}
	expectedDBTables := DatabaseTables{}

	dbTables.AppendTables(util.InformationSchemaName.L, []string{"xxx"}...)
	dbTables.AppendTables(strings.ToUpper(util.PerformanceSchemaName.L), []string{"xxx"}...)
	dbTables.AppendTables("xxx", []string{"yyy"}...)
	expectedDBTables.AppendTables("xxx", []string{"yyy"}...)
	dbTables.AppendTables("yyy", []string{"xxx"}...)

	conf := &Config{
		ServerInfo: ServerInfo{
			ServerType: ServerTypeTiDB,
		},
		Tables: dbTables,
		BlackWhiteList: BWListConf{
			Mode: MySQLReplicationMode,
			Rules: &MySQLReplicationConf{
				Rules: &filter.Rules{
					DoDBs: []string{""},
				},
			},
		},
	}

	c.Assert(filterTables(conf), NotNil)
	conf.BlackWhiteList = BWListConf{
		Mode: MySQLReplicationMode,
		Rules: &MySQLReplicationConf{
			Rules: &filter.Rules{
				DoDBs: []string{"xxx"},
			},
		},
	}
	c.Assert(filterTables(conf), IsNil)
	c.Assert(conf.Tables, HasLen, 1)
	c.Assert(conf.Tables, DeepEquals, expectedDBTables)
}
