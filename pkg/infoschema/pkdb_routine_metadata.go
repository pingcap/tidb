package infoschema

import (
	"slices"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

const routineMetadataSelectSQL = `select route_schema,name,type,definition_utf8,parameter_str,is_deterministic,sql_data_access,security_type,definer,sql_mode,
	character_set_client,connection_collation,schema_collation,created,last_altered,comment,options,external_language from mysql.routines`

// RoutineMetadataFilter narrows the mysql.routines query by schema and routine name.
type RoutineMetadataFilter struct {
	Schemas []string
	Names   []string
}

// RoutineMetadata is the shared mysql.routines row shape used by executor-side infoschema readers.
type RoutineMetadata struct {
	*model.ProcedureInfo
	Created     types.Time
	LastAltered types.Time
}

// BuildRoutineMetadataSQL returns the shared mysql.routines query used by infoschema and executor.
func BuildRoutineMetadataSQL(filter RoutineMetadataFilter) (string, []any) {
	sql := routineMetadataSelectSQL
	clauses := make([]string, 0, 2)
	args := make([]any, 0, 2)
	if len(filter.Schemas) > 0 {
		schemas := append([]string(nil), filter.Schemas...)
		slices.Sort(schemas)
		clauses = append(clauses, "lower(route_schema) in (%?)")
		args = append(args, schemas)
	}
	if len(filter.Names) > 0 {
		names := append([]string(nil), filter.Names...)
		slices.Sort(names)
		clauses = append(clauses, "lower(name) in (%?)")
		args = append(args, names)
	}
	if len(clauses) > 0 {
		sql += " where " + strings.Join(clauses, " and ")
	}
	return sql + ";", args
}

// DecodeRoutineMetadataRow converts one mysql.routines row into RoutineMetadata.
func DecodeRoutineMetadataRow(row chunk.Row) (*RoutineMetadata, error) {
	if row.Len() != 18 {
		return nil, errors.Errorf("unexpected mysql.routines row length %d", row.Len())
	}

	routineType := row.GetEnum(2).String()
	switch routineType {
	case "FUNCTION", "PROCEDURE":
	default:
		return nil, errors.Errorf("unsupported routine type %q", routineType)
	}

	var options *string
	if !row.IsNull(16) {
		optionsStr := row.GetString(16)
		options = &optionsStr
	}

	definitionUTF8 := row.GetString(3)
	return &RoutineMetadata{
		ProcedureInfo: &model.ProcedureInfo{
			Schema: pmodel.NewCIStr(row.GetString(0)),
			Name:   pmodel.NewCIStr(row.GetString(1)),
			Type:   routineType,

			Definition:     definitionUTF8,
			DefinitionUTF8: definitionUTF8,
			ParameterStr:   row.GetString(4),

			IsDeterministic: row.GetInt64(5),
			SQLDataAccess:   row.GetEnum(6).String(),
			SecurityType:    row.GetEnum(7).String(),
			Definer:         row.GetString(8),
			SQLMode:         row.GetSet(9).String(),

			CharacterSetClient:  row.GetString(10),
			CollationConnection: row.GetString(11),
			SchemaCollation:     row.GetString(12),

			Comment:          row.GetString(15),
			Options:          options,
			ExternalLanguage: row.GetString(17),
		},
		Created:     row.GetTime(13),
		LastAltered: row.GetTime(14),
	}, nil
}
