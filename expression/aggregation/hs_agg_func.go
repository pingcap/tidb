// Copyright 2021 PingCAP, Inc.
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

// +build hyperscan

package aggregation

import (
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
)

func init() {
	extensionAggFuncs[ast.AggFuncHSBuildDB] = extensionAggFuncInfo{
		typeInfer:     typeInfer4HsBuildDb,
		removeNotNull: removeNotNull4HsBuildDb,
	}
}

func typeInfer4HsBuildDb(a *baseFuncDesc, ctx sessionctx.Context) {
	a.RetTp = types.NewFieldType(mysql.TypeString)
	a.RetTp.Charset, a.RetTp.Collate = charset.GetDefaultCharsetAndCollate()
	a.RetTp.Flen, a.RetTp.Decimal = mysql.MaxBlobWidth, 0
}

func removeNotNull4HsBuildDb(hasGroupBy, allAggsFirstRow bool) bool {
	return false
}
