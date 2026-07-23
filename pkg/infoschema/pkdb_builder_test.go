package infoschema

import (
	"testing"

	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestBuildRoutineMetadataSQL(t *testing.T) {
	sql, args := BuildRoutineMetadataSQL(RoutineMetadataFilter{})
	require.Equal(t, `select route_schema,name,type,definition_utf8,parameter_str,is_deterministic,sql_data_access,security_type,definer,sql_mode,
	character_set_client,connection_collation,schema_collation,created,last_altered,comment,options,external_language from mysql.routines;`, sql)
	require.Empty(t, args)

	sql, args = BuildRoutineMetadataSQL(RoutineMetadataFilter{
		Schemas: []string{"test", "mysql"},
		Names:   []string{"proc_b", "proc_a"},
	})
	require.Equal(t, `select route_schema,name,type,definition_utf8,parameter_str,is_deterministic,sql_data_access,security_type,definer,sql_mode,
	character_set_client,connection_collation,schema_collation,created,last_altered,comment,options,external_language from mysql.routines where lower(route_schema) in (%?) and lower(name) in (%?);`, sql)
	require.Equal(t, []any{[]string{"mysql", "test"}, []string{"proc_a", "proc_b"}}, args)
}

func TestDecodeRoutineMetadataRow(t *testing.T) {
	options := "{\"sql_mode\":\"strict\"}"
	row := chunk.MutRowFromDatums([]types.Datum{
		types.NewStringDatum("test"),
		types.NewStringDatum("proc_meta"),
		types.NewMysqlEnumDatum(types.Enum{Name: "PROCEDURE", Value: 2}),
		types.NewStringDatum("BEGIN SELECT 1; END"),
		types.NewStringDatum("IN p_id INT"),
		types.NewIntDatum(1),
		types.NewMysqlEnumDatum(types.Enum{Name: "READS SQL DATA", Value: 3}),
		types.NewMysqlEnumDatum(types.Enum{Name: "DEFINER", Value: 3}),
		types.NewStringDatum("root@%"),
		types.NewMysqlSetDatum(types.Set{Name: "ONLY_FULL_GROUP_BY", Value: 32}, ""),
		types.NewStringDatum("utf8mb4"),
		types.NewStringDatum("utf8mb4_bin"),
		types.NewStringDatum("utf8mb4_bin"),
		types.NewTimeDatum(types.ZeroTimestamp),
		types.NewTimeDatum(types.ZeroTimestamp),
		types.NewStringDatum("comment"),
		types.NewStringDatum(options),
		types.NewStringDatum("SQL"),
	}).ToRow()

	routine, err := DecodeRoutineMetadataRow(row)
	require.NoError(t, err)
	require.Equal(t, "test", routine.Schema.O)
	require.Equal(t, "proc_meta", routine.Name.O)
	require.Equal(t, "PROCEDURE", routine.Type)
	require.Equal(t, "BEGIN SELECT 1; END", routine.DefinitionUTF8)
	require.Equal(t, "IN p_id INT", routine.ParameterStr)
	require.Equal(t, int64(1), routine.IsDeterministic)
	require.Equal(t, "READS SQL DATA", routine.SQLDataAccess)
	require.Equal(t, "DEFINER", routine.SecurityType)
	require.Equal(t, "root@%", routine.Definer)
	require.Equal(t, "ONLY_FULL_GROUP_BY", routine.SQLMode)
	require.Equal(t, "utf8mb4", routine.CharacterSetClient)
	require.Equal(t, "utf8mb4_bin", routine.CollationConnection)
	require.Equal(t, "utf8mb4_bin", routine.SchemaCollation)
	require.Equal(t, "comment", routine.Comment)
	require.NotNil(t, routine.Options)
	require.Equal(t, options, *routine.Options)
	require.Equal(t, "SQL", routine.ExternalLanguage)
}
