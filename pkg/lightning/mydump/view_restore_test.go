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

package mydump

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
	"github.com/stretchr/testify/require"
)

func TestParseViewSchemaSQL(t *testing.T) {
	p := parser.New()
	currentView := filter.Table{Schema: "test", Name: "v2"}
	sql := `
/*!40014 SET FOREIGN_KEY_CHECKS=0*/;
/*!40101 SET NAMES binary*/;
DROP TABLE IF EXISTS v2;
DROP VIEW IF EXISTS v2;
SET @PREV_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT;
SET @PREV_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS;
SET @PREV_COLLATION_CONNECTION=@@COLLATION_CONNECTION;
SET character_set_client = utf8mb4;
SET character_set_results = utf8mb4;
SET collation_connection = utf8mb4_0900_ai_ci;
CREATE ALGORITHM=UNDEFINED DEFINER=` + "`root`@`%`" + ` SQL SECURITY DEFINER VIEW v2 (` + "`id`" + `) AS
	SELECT ` + "`id`" + ` FROM ` + "`test`.`v1`" + `;
SET character_set_client = @PREV_CHARACTER_SET_CLIENT;
SET character_set_results = @PREV_CHARACTER_SET_RESULTS;
SET collation_connection = @PREV_COLLATION_CONNECTION;
`

	parsed, err := parseViewSchemaSQL(p, currentView, sql)
	require.NoError(t, err)
	require.Equal(t, currentView, parsed.key)
	require.Equal(t, []filter.Table{{Schema: "test", Name: "v1"}}, parsed.deps)
	require.NotContains(t, parsed.createSQL, "DROP TABLE")
	require.NotContains(t, parsed.createSQL, "DROP VIEW")
	require.Contains(t, parsed.createSQL, "SET NAMES 'binary'")
	require.Contains(t, parsed.createSQL, "VIEW `test`.`v2`")
}

func TestParseViewSchemaSQLDeduplicatesAndUsesCurrentSchema(t *testing.T) {
	p := parser.New()
	currentView := filter.Table{Schema: "test", Name: "v3"}
	sql := `
CREATE ALGORITHM=UNDEFINED DEFINER=` + "`root`@`%`" + ` SQL SECURITY DEFINER VIEW v3 AS
SELECT src.id
FROM (
	SELECT id FROM v1
	UNION
	SELECT id FROM test.v1
) AS src
JOIN v2 ON v2.id = src.id;
`

	parsed, err := parseViewSchemaSQL(p, currentView, sql)
	require.NoError(t, err)
	require.ElementsMatch(t,
		[]filter.Table{
			{Schema: "test", Name: "v1"},
			{Schema: "test", Name: "v2"},
		},
		parsed.deps,
	)
}

func TestParseViewSchemaSQLSupportsMultipleAndCrossSchemaDeps(t *testing.T) {
	p := parser.New()
	currentView := filter.Table{Schema: "db3", Name: "v4"}
	sql := `
CREATE ALGORITHM=UNDEFINED DEFINER=` + "`root`@`%`" + ` SQL SECURITY DEFINER VIEW v4 AS
SELECT src.id
FROM db1.v1 src
JOIN db2.v2 ON v2.id = src.id
JOIN t_local ON t_local.id = src.id
JOIN db2.base_table ON base_table.id = src.id;
`

	parsed, err := parseViewSchemaSQL(p, currentView, sql)
	require.NoError(t, err)
	require.ElementsMatch(t,
		[]filter.Table{
			{Schema: "db1", Name: "v1"},
			{Schema: "db2", Name: "v2"},
			{Schema: "db2", Name: "base_table"},
			{Schema: "db3", Name: "t_local"},
		},
		parsed.deps,
	)
}

func TestParseViewSchemaSQLIgnoresCTEDependencies(t *testing.T) {
	p := parser.New()
	currentView := filter.Table{Schema: "test", Name: "v_cte"}
	sql := `
CREATE VIEW v_cte AS
WITH cte AS (
	SELECT id FROM t1
)
SELECT cte.id FROM cte;
`

	parsed, err := parseViewSchemaSQL(p, currentView, sql)
	require.NoError(t, err)
	require.Equal(t, []filter.Table{{Schema: "test", Name: "t1"}}, parsed.deps)
}

func TestParseViewSchemaSQLRejectsMultipleCreateStatements(t *testing.T) {
	p := parser.New()
	currentView := filter.Table{Schema: "test", Name: "v_multi"}
	sql := `
CREATE VIEW v_multi AS SELECT 1;
CREATE VIEW v_multi AS SELECT 2;
`

	_, err := parseViewSchemaSQL(p, currentView, sql)
	require.ErrorContains(t, err, "multiple create view statements found")
}

func TestBuildViewRestorePlan(t *testing.T) {
	v1 := filter.Table{Schema: "test", Name: "v1"}
	v2 := filter.Table{Schema: "test", Name: "v2"}
	dumpTables := make(tableNameSet)
	dumpTables.add(filter.Table{Schema: "test", Name: "t"})

	plan, err := buildViewRestorePlan([]*parsedViewSchema{
		{
			key:       v1,
			deps:      []filter.Table{{Schema: "test", Name: "t"}},
			createSQL: "CREATE VIEW `test`.`v1` AS SELECT `id` FROM `test`.`t`;",
		},
		{
			key:       v2,
			deps:      []filter.Table{{Schema: "test", Name: "v1"}},
			createSQL: "CREATE VIEW `test`.`v2` AS SELECT `id` FROM `test`.`v1`;",
		},
	}, dumpTables)
	require.NoError(t, err)
	require.Len(t, plan.ordered, 2)
	require.Equal(t, v1, plan.ordered[0].key)
	require.Equal(t, v2, plan.ordered[1].key)
	require.Equal(t, 0, plan.nodes[v1].indegree)
	require.Equal(t, 1, plan.nodes[v2].indegree)
	require.Empty(t, plan.nodes[v1].externalDeps)
	require.Equal(t, []filter.Table{v2}, plan.nodes[v1].dependents)
}

func TestBuildViewRestorePlanSupportsMultipleAndCrossSchemaViewDeps(t *testing.T) {
	db1v1 := filter.Table{Schema: "db1", Name: "v1"}
	db2v2 := filter.Table{Schema: "db2", Name: "v2"}
	db2v3 := filter.Table{Schema: "db2", Name: "v3"}
	dumpTables := make(tableNameSet)
	dumpTables.add(filter.Table{Schema: "db1", Name: "t1"})
	dumpTables.add(filter.Table{Schema: "db2", Name: "t2"})
	dumpTables.add(filter.Table{Schema: "db2", Name: "t3"})

	plan, err := buildViewRestorePlan([]*parsedViewSchema{
		{
			key:       db1v1,
			deps:      []filter.Table{{Schema: "db1", Name: "t1"}, {Schema: "db2", Name: "t2"}},
			createSQL: "CREATE VIEW `db1`.`v1` AS SELECT * FROM `db1`.`t1` JOIN `db2`.`t2`;",
		},
		{
			key:       db2v3,
			deps:      []filter.Table{{Schema: "db2", Name: "t3"}},
			createSQL: "CREATE VIEW `db2`.`v3` AS SELECT * FROM `db2`.`t3`;",
		},
		{
			key:       db2v2,
			deps:      []filter.Table{{Schema: "db1", Name: "v1"}, {Schema: "db2", Name: "v3"}},
			createSQL: "CREATE VIEW `db2`.`v2` AS SELECT * FROM `db1`.`v1` JOIN `db2`.`v3`;",
		},
	}, dumpTables)
	require.NoError(t, err)
	require.Len(t, plan.ordered, 3)
	require.Equal(t, []filter.Table{db1v1, db2v3, db2v2}, []filter.Table{
		plan.ordered[0].key,
		plan.ordered[1].key,
		plan.ordered[2].key,
	})
	require.Equal(t, 0, plan.nodes[db1v1].indegree)
	require.Equal(t, 0, plan.nodes[db2v3].indegree)
	require.Equal(t, 2, plan.nodes[db2v2].indegree)
	require.Equal(t, []filter.Table{db2v2}, plan.nodes[db1v1].dependents)
	require.Equal(t, []filter.Table{db2v2}, plan.nodes[db2v3].dependents)
}

func TestBuildViewRestorePlanNormalizesCaseInsensitiveDeps(t *testing.T) {
	v1 := filter.Table{Schema: "test", Name: "v1"}
	v2 := filter.Table{Schema: "test", Name: "V2"}
	dumpTables := make(tableNameSet)
	dumpTables.add(filter.Table{Schema: "test", Name: "t"})

	plan, err := buildViewRestorePlan([]*parsedViewSchema{
		{
			key:       v1,
			deps:      []filter.Table{{Schema: "test", Name: "t"}},
			createSQL: "CREATE VIEW `test`.`v1` AS SELECT `id` FROM `test`.`t`;",
		},
		{
			key:       v2,
			deps:      []filter.Table{{Schema: "Test", Name: "V1"}},
			createSQL: "CREATE VIEW `test`.`V2` AS SELECT `id` FROM `test`.`v1`;",
		},
	}, dumpTables)
	require.NoError(t, err)
	require.Len(t, plan.ordered, 2)
	require.Equal(t, v1, plan.ordered[0].key)
	require.Equal(t, v2, plan.ordered[1].key)
	require.Empty(t, plan.nodes[normalizeTableName(v2.Schema, v2.Name)].externalDeps)
}

func TestBuildViewRestorePlanRejectsCaseInsensitiveDuplicates(t *testing.T) {
	_, err := buildViewRestorePlan([]*parsedViewSchema{
		{
			key:       filter.Table{Schema: "test", Name: "v1"},
			createSQL: "CREATE VIEW `test`.`v1` AS SELECT 1;",
		},
		{
			key:       filter.Table{Schema: "Test", Name: "V1"},
			createSQL: "CREATE VIEW `test`.`V1` AS SELECT 1;",
		},
	}, nil)
	require.ErrorContains(t, err, "duplicate view definition")
}

func TestValidateViewRestorePlanRejectsMissingExternalDependency(t *testing.T) {
	plan := &viewRestorePlan{
		ordered: []*viewNode{{
			key:          filter.Table{Schema: "test", Name: "v1"},
			externalDeps: []filter.Table{{Schema: "test", Name: "missing_tbl"}},
		}},
	}

	err := validateViewRestorePlan(plan, make(tableNameSet))
	require.ErrorContains(t, err, "missing dependency")
}

func TestBuildViewRestorePlanDetectsCycle(t *testing.T) {
	_, err := buildViewRestorePlan([]*parsedViewSchema{
		{
			key:       filter.Table{Schema: "test", Name: "v1"},
			deps:      []filter.Table{{Schema: "test", Name: "v2"}},
			createSQL: "CREATE VIEW `test`.`v1` AS SELECT `id` FROM `test`.`v2`;",
		},
		{
			key:       filter.Table{Schema: "test", Name: "v2"},
			deps:      []filter.Table{{Schema: "test", Name: "v1"}},
			createSQL: "CREATE VIEW `test`.`v2` AS SELECT `id` FROM `test`.`v1`;",
		},
	}, nil)
	require.ErrorContains(t, err, "cyclic")
	require.ErrorContains(t, err, "`test`.`v1`")
	require.ErrorContains(t, err, "`test`.`v2`")
}
