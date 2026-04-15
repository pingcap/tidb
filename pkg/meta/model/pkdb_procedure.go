// Copyright 2026 PingCAP, Inc.

package model

import (
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	types "github.com/pingcap/tidb/pkg/parser/types"
)

// ProcedureInfo provides meta data describing a stored procedure/function.
type ProcedureInfo struct {
	Schema pmodel.CIStr `json:"schema"`
	Name   pmodel.CIStr `json:"name"`
	Type   string       `json:"type"` // "PROCEDURE" or "FUNCTION"

	Definition     string `json:"definition"`
	DefinitionUTF8 string `json:"definition_utf8"`
	ParameterStr   string `json:"parameter_str"`

	IsDeterministic int64  `json:"is_deterministic"`
	SQLDataAccess   string `json:"sql_data_access"`
	SecurityType    string `json:"security_type"`
	Definer         string `json:"definer"`
	SQLMode         string `json:"sql_mode"`

	CharacterSetClient  string `json:"character_set_client"`
	CollationConnection string `json:"collation_connection"`
	SchemaCollation     string `json:"schema_collation"`

	Comment          string  `json:"comment"`
	Options          *string `json:"options,omitempty"`
	ExternalLanguage string  `json:"external_language"`

	// above fields mirrors the mysql.routines table schema
	//

	// RetType is set when infoschema loads ProcedureInfo into memory. A stored
	// function may have not nil RetType. Nil RetType for stored function means error
	// happens in infoschema, it will be process again later.
	RetType *types.FieldType `json:"ret_type"`
	State   SchemaState      `json:"state"`
}

// LoadableFunctionInfo contains metadata for creating a loadable function.
type LoadableFunctionInfo struct {
	Name       pmodel.CIStr   `json:"name"`
	SoName     string         `json:"so_name"`
	ReturnType types.EvalType `json:"return_type"`
}
