// Copyright 2022 PingCAP, Inc.
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

package transformer

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
)

type TestCase struct {
	fail           bool
	origin, expect string
}

func TestParse(_t *testing.T) {
	stmt, warns, err := parser.New().Parse("SELECT MAX(c0) FROM (SELECT MAX(c) as c0 FROM t UNION ALL SELECT MAX(c) as c0 FROM t) as tmp", "", "")
	fmt.Printf("%#x", warns)
	if err != nil {
		_ = fmt.Errorf("error: %s", err.Error())
	}
	fmt.Printf("%#v", stmt[0].(*ast.SelectStmt).From.TableRefs.Left)
}
