// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package sysschema

var sysSchemaTables = []string{
	viewUnusedIndexes,
	viewIndexUsage,
}

const viewUnusedIndexes = "CREATE VIEW sys." + viewNameUnusedIndexes +
	` AS SELECT DISTINCT i.table_schema AS table_schema, i.table_name AS table_name, i.key_name AS index_name
			FROM information_schema.tidb_indexes AS i
				LEFT JOIN information_schema.tables AS t
					ON t.table_schema = i.table_schema
						AND t.table_name = i.table_name
				LEFT JOIN mysql.schema_index_usage AS u
					ON u.table_id = t.tidb_table_id
						AND u.index_id = i.index_id
			WHERE (u.query_count = 0 OR u.query_count is null)
				AND i.key_name != 'PRIMARY'
				AND i.table_schema not in ('mysql', 'PERFORMANCE_SCHEMA', 'INFORMATION_SCHEMA');`

const viewIndexUsage = "CREATE VIEW sys." + viewNameIndexUsage +
	` AS SELECT DISTINCT i.table_schema AS table_schema,
						i.table_name AS table_name,
						i.key_name AS index_name,
						IFNULL(u.query_count, 0) AS query_count,
						IFNULL(u.rows_selected, 0) AS rows_selected,
						u.last_used_at AS last_used_at
			FROM information_schema.tidb_indexes AS i
				LEFT JOIN information_schema.tables AS t
					ON t.table_schema = i.table_schema
						AND t.table_name = i.table_name
				LEFT JOIN mysql.schema_index_usage AS u
					ON u.table_id = t.tidb_table_id
						AND u.index_id = i.index_id
			WHERE i.key_name != 'PRIMARY'
				AND i.table_schema not in ('mysql', 'PERFORMANCE_SCHEMA', 'INFORMATION_SCHEMA');`
