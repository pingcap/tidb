// Copyright 2019 PingCAP, Inc.
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

package helper

// TypeContext is the template context for each "github.com/pingcap/tidb/types".EvalType .
type TypeContext struct {
	// Describe the name of "github.com/pingcap/tidb/types".ET{{ .ETName }} .
	ETName string
	// Describe the name of "github.com/pingcap/tidb/expression".VecExpr.VecEval{{ .TypeName }} .
	TypeName string
	// Describe the name of "github.com/pingcap/tidb/util/chunk".*Column.Append{{ .TypeNameInColumn }},
	// Resize{{ .TypeNameInColumn }}, Reserve{{ .TypeNameInColumn }}, Get{{ .TypeNameInColumn }} and
	// {{ .TypeNameInColumn }}s.
	// If undefined, it's same as TypeName.
	TypeNameInColumn string
	// Same as "github.com/pingcap/tidb/util/chunk".getFixedLen() .
	Fixed bool
}

var (
	// TypeInt represents the template context of types.ETInt .
	TypeInt = TypeContext{ETName: "Int", TypeName: "Int", TypeNameInColumn: "Int64", Fixed: true}
	// TypeReal represents the template context of types.ETReal .
	TypeReal = TypeContext{ETName: "Real", TypeName: "Real", TypeNameInColumn: "Float64", Fixed: true}
	// TypeDecimal represents the template context of types.ETDecimal .
	TypeDecimal = TypeContext{ETName: "Decimal", TypeName: "Decimal", TypeNameInColumn: "Decimal", Fixed: true}
	// TypeString represents the template context of types.ETString .
	TypeString = TypeContext{ETName: "String", TypeName: "String", TypeNameInColumn: "String", Fixed: false}
	// TypeDatetime represents the template context of types.ETDatetime .
	TypeDatetime = TypeContext{ETName: "Datetime", TypeName: "Time", TypeNameInColumn: "Time", Fixed: true}
	// TypeDuration represents the template context of types.ETDuration .
	TypeDuration = TypeContext{ETName: "Duration", TypeName: "Duration", TypeNameInColumn: "GoDuration", Fixed: true}
	// TypeJSON represents the template context of types.ETJson .
	TypeJSON = TypeContext{ETName: "Json", TypeName: "JSON", TypeNameInColumn: "JSON", Fixed: false}
)
