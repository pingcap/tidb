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

package mutasql

import (
	"bytes"
	"testing"

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/types"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func parseNode(sql string) ast.Node {
	p := parser.New()
	stmtNodes, _, _ := p.Parse(sql, "", "")
	return stmtNodes[0]
}

func stringifyNode(node ast.Node) string {
	out := new(bytes.Buffer)
	err := node.Restore(format.NewRestoreCtx(format.RestoreStringDoubleQuotes, out))
	if err != nil {
		panic(zap.Error(err)) // should never get error
	}
	return out.String()
}

func TestReplaceTableNameInNode(t *testing.T) {
	node := parseNode("SELECT s.c1,t.c2 FROM s LEFT JOIN t ON t.c0=\"1995\" WHERE t.c1=s.c3")

	m := make(map[string]string)
	m["s"] = "p"
	m["t"] = "q"
	n := replaceTableNameInNode(node, m)

	assert.Equal(t, stringifyNode(n), "SELECT p.c1,q.c2 FROM p LEFT JOIN q ON q.c0=_utf8mb4\"1995\" WHERE q.c1=p.c3")
}

func TestReplaceTableName(t *testing.T) {
	node := parseNode("SELECT s.c1,t.c2 FROM s LEFT JOIN t ON t.c0=\"1995\" WHERE t.c1=s.c3")

	tc := TestCase{D: make([]*Dataset, 0), Q: node, Mutable: true}
	tc.D = append(tc.D, &Dataset{})
	tc.D[0].Table.Name = types.CIStr("t")
	tc.D[0].Table.Columns = append(tc.D[0].Table.Columns,
		types.Column{Name: types.CIStr("c0"), Table: types.CIStr("t"), Type: "TEXT", Length: 255, Null: false},
		types.Column{Name: types.CIStr("c1"), Table: types.CIStr("t"), Type: "TEXT", Length: 255, Null: true},
		types.Column{Name: types.CIStr("c2"), Table: types.CIStr("t"), Type: "TEXT", Length: 255, Null: true},
	)
	tc.D[0].After = append(tc.D[0].After, parseNode("INSERT INTO t VALUES(\"1995\", NULL, NULL)"))

	m := make(map[string]string)
	m["s"] = "p"
	m["t"] = "q"
	tc.ReplaceTableName(m)

	assert.Equal(t, stringifyNode(tc.Q), "SELECT p.c1,q.c2 FROM p LEFT JOIN q ON q.c0=_utf8mb4\"1995\" WHERE q.c1=p.c3")
	assert.Equal(t, tc.D[0].Table.Name.String(), "q")
	assert.Equal(t, tc.D[0].Table.Columns[0].Table.String(), "q")
	assert.Equal(t, stringifyNode(tc.D[0].After[0]), "INSERT INTO q VALUES (_utf8mb4\"1995\",NULL,NULL)")
}
