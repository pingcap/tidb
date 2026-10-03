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

// Package udf provides shared types and utilities for User-Defined Functions.
// This package is independent of any specific UDF runtime (SQL, JavaScript, etc.)
// and can be imported by both the expression package and runtime-specific packages.
package udf

import (
	"strconv"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
)

// Definition represents a user-defined function definition.
// This is a runtime-agnostic representation that can be used by any UDF implementation.
type Definition struct {
	ID              int64
	Name            string
	SchemaName      string
	ParamNames      []string
	ParamTypes      []byte // TiDB type IDs
	ReturnType      byte
	Language        string // "sql", "javascript", etc.
	SourceCode      string
	IsDeterministic bool
	IsAggregate     bool

	// Aggregate-specific code
	InitCode     string
	UpdateCode   string
	FinalizeCode string

	// Metadata
	Definer     string
	SQLSecurity string // "DEFINER" or "INVOKER"
	Comment     string
	DataAccess  string // "CONTAINS SQL", "NO SQL", "READS SQL DATA", "MODIFIES SQL DATA"
	Version     uint64

	// Cached function ID to avoid repeated fmt.Sprintf allocations
	cachedFuncID string
}

// GetFunctionID returns a cached function ID.
// The ID is computed once and cached to avoid repeated allocations.
func (d *Definition) GetFunctionID() string {
	if d.cachedFuncID == "" {
		d.cachedFuncID = "udf_" + strconv.FormatInt(d.ID, 10) + "_" + strconv.FormatUint(d.Version, 10)
	}
	return d.cachedFuncID
}

// InvalidateFunctionID clears the cached function ID (call when Version changes).
func (d *Definition) InvalidateFunctionID() {
	d.cachedFuncID = ""
}

// TypeID constants for UDF parameter and return types.
// These are used for serialization between TiDB and UDF runtimes.
const (
	TypeIDNull     int32 = 0
	TypeIDInt      int32 = 1
	TypeIDUint     int32 = 2
	TypeIDFloat    int32 = 3
	TypeIDString   int32 = 4
	TypeIDDecimal  int32 = 5
	TypeIDDatetime int32 = 6
	TypeIDJSON     int32 = 7
)

// MySQLTypeToTypeID converts a MySQL type to a UDF type ID.
func MySQLTypeToTypeID(tp byte) int32 {
	switch tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		return TypeIDInt
	case mysql.TypeFloat, mysql.TypeDouble:
		return TypeIDFloat
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		return TypeIDString
	case mysql.TypeNewDecimal:
		return TypeIDDecimal
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		return TypeIDDatetime
	case mysql.TypeJSON:
		return TypeIDJSON
	default:
		return TypeIDString // Default to string
	}
}

// TypeIDToFieldType converts a UDF type ID to a TiDB field type.
func TypeIDToFieldType(typeID int32) *types.FieldType {
	switch typeID {
	case TypeIDNull:
		return types.NewFieldType(mysql.TypeNull)
	case TypeIDInt:
		return types.NewFieldType(mysql.TypeLonglong)
	case TypeIDUint:
		ft := types.NewFieldType(mysql.TypeLonglong)
		ft.AddFlag(mysql.UnsignedFlag)
		return ft
	case TypeIDFloat:
		return types.NewFieldType(mysql.TypeDouble)
	case TypeIDString:
		return types.NewFieldType(mysql.TypeVarString)
	case TypeIDDecimal:
		return types.NewFieldType(mysql.TypeNewDecimal)
	case TypeIDDatetime:
		return types.NewFieldType(mysql.TypeDatetime)
	case TypeIDJSON:
		return types.NewFieldType(mysql.TypeJSON)
	default:
		return types.NewFieldType(mysql.TypeVarString)
	}
}

// TypeIDToEvalType converts a UDF type ID to a TiDB EvalType.
func TypeIDToEvalType(typeID int32) types.EvalType {
	switch typeID {
	case TypeIDInt, TypeIDUint:
		return types.ETInt
	case TypeIDFloat:
		return types.ETReal
	case TypeIDString:
		return types.ETString
	case TypeIDDecimal:
		return types.ETDecimal
	case TypeIDDatetime:
		return types.ETDatetime
	case TypeIDJSON:
		return types.ETJson
	default:
		return types.ETString
	}
}

// ParseTypeSpec parses a SQL type specification to a type ID.
func ParseTypeSpec(typeSpec string) int32 {
	switch typeSpec {
	case "INT", "INTEGER", "BIGINT", "TINYINT", "SMALLINT", "MEDIUMINT":
		return TypeIDInt
	case "FLOAT", "DOUBLE", "REAL":
		return TypeIDFloat
	case "DECIMAL", "NUMERIC":
		return TypeIDDecimal
	case "VARCHAR", "CHAR", "TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT":
		return TypeIDString
	case "DATE", "DATETIME", "TIMESTAMP":
		return TypeIDDatetime
	case "JSON":
		return TypeIDJSON
	default:
		return TypeIDString
	}
}
