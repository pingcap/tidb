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

package executor

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestImportIntoShouldHaveSameFlagsAsInsert(t *testing.T) {
	insertStmt := &ast.InsertStmt{}
	importStmt := &ast.ImportIntoStmt{}
	insertCtx := mock.NewContext()
	importCtx := mock.NewContext()
	domain.BindDomain(insertCtx, &domain.Domain{})
	domain.BindDomain(importCtx, &domain.Domain{})
	for _, modeStr := range []string{
		"",
		"IGNORE_SPACE",
		"STRICT_TRANS_TABLES",
		"STRICT_ALL_TABLES",
		"ALLOW_INVALID_DATES",
		"NO_ZERO_IN_DATE",
		"NO_ZERO_DATE",
		"NO_ZERO_IN_DATE,STRICT_ALL_TABLES",
		"NO_ZERO_DATE,STRICT_ALL_TABLES",
		"NO_ZERO_IN_DATE,NO_ZERO_DATE,STRICT_ALL_TABLES",
	} {
		t.Run(fmt.Sprintf("mode %s", modeStr), func(t *testing.T) {
			mode, err := mysql.GetSQLMode(modeStr)
			require.NoError(t, err)
			insertCtx.GetSessionVars().SQLMode = mode
			require.NoError(t, ResetContextOfStmt(insertCtx, insertStmt))
			importCtx.GetSessionVars().SQLMode = mode
			require.NoError(t, ResetContextOfStmt(importCtx, importStmt))

			insertTypeCtx := insertCtx.GetSessionVars().StmtCtx.TypeCtx()
			importTypeCtx := importCtx.GetSessionVars().StmtCtx.TypeCtx()
			require.EqualValues(t, insertTypeCtx.Flags(), importTypeCtx.Flags())
		})
	}
}
