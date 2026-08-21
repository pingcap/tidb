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

package nameroute_test

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/br/pkg/restore/nameroute"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

func TestParseAndRoute(t *testing.T) {
	router, err := nameroute.Parse([]string{
		"src:dst",
		"src.orders:archive.orders_copy",
	})
	require.NoError(t, err)

	schema, table, matched := router.Route(ast.NewCIStr("SRC"), ast.NewCIStr("orders"))
	require.True(t, matched)
	require.Equal(t, "archive", schema.O)
	require.Equal(t, "orders_copy", table.O)

	schema, table, matched = router.Route(ast.NewCIStr("src"), ast.NewCIStr("customers"))
	require.True(t, matched)
	require.Equal(t, "dst", schema.O)
	require.Equal(t, "customers", table.O)

	schema, table, matched = router.Route(ast.NewCIStr("other"), ast.NewCIStr("orders"))
	require.False(t, matched)
	require.Equal(t, "other", schema.O)
	require.Equal(t, "orders", table.O)
}

func TestParseQuotedIdentifiers(t *testing.T) {
	router, err := nameroute.Parse([]string{
		"  `db.1`.`t:a``b` : `db:2`.`t.2``x`  ",
	})
	require.NoError(t, err)

	rules := router.Rules()
	require.Len(t, rules, 1)
	require.Equal(t, "db.1", rules[0].Source.Schema.O)
	require.Equal(t, "t:a`b", rules[0].Source.Table.O)
	require.Equal(t, "db:2", rules[0].Target.Schema.O)
	require.Equal(t, "t.2`x", rules[0].Target.Table.O)
	require.Equal(t,
		[]string{"`db.1`.`t:a``b`:`db:2`.`t.2``x`"},
		router.CanonicalRules())
}

func TestRuleOrderDoesNotAffectRoutingOrFingerprint(t *testing.T) {
	first, err := nameroute.Parse([]string{"a:x", "a.b:c.d", "c:y"})
	require.NoError(t, err)
	second, err := nameroute.Parse([]string{"c:y", "a.b:c.d", "a:x"})
	require.NoError(t, err)

	require.Equal(t, first.CanonicalRules(), second.CanonicalRules())
	require.Equal(t, first.Fingerprint(), second.Fingerprint())

	schema, table, matched := first.Route(ast.NewCIStr("a"), ast.NewCIStr("b"))
	require.True(t, matched)
	require.Equal(t, "c", schema.O)
	require.Equal(t, "d", table.O)
}

func TestParseRejectsInvalidRules(t *testing.T) {
	testCases := []struct {
		name    string
		spec    string
		message string
	}{
		{name: "empty", spec: "", message: "expected identifier"},
		{name: "missing separator", spec: "a.b", message: "expected ':'"},
		{name: "extra separator", spec: "a:b:c", message: "unexpected character"},
		{name: "too many components", spec: "a.b.c:d.e", message: "expected ':'"},
		{name: "level mismatch", spec: "a:b.t", message: "must both name"},
		{name: "unterminated quote", spec: "`a:b", message: "unterminated"},
		{name: "empty quoted identifier", spec: "``:b", message: "must not be empty"},
		{name: "unquoted punctuation", spec: "a-b:c", message: "inside backticks"},
		{name: "schema too long", spec: strings.Repeat("a", 65) + ":b", message: "maximum length"},
		{name: "table too long", spec: "a." + strings.Repeat("t", 65) + ":b.c", message: "maximum length"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := nameroute.Parse([]string{testCase.spec})
			require.ErrorContains(t, err, testCase.message)
		})
	}
}

func TestNewRejectsDuplicatesAndConflicts(t *testing.T) {
	testCases := []struct {
		name    string
		specs   []string
		message string
	}{
		{name: "duplicate source", specs: []string{"a:b", "A:c"}, message: "duplicate source"},
		{name: "duplicate target", specs: []string{"a:x", "b:X"}, message: "duplicate target"},
		{name: "identity", specs: []string{"a:A"}, message: "maps `a` to itself"},
		{
			name:    "table conflicts with schema output",
			specs:   []string{"a:x", "b.t:x.u"},
			message: "conflicts with schema rule",
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := nameroute.Parse(testCase.specs)
			require.ErrorContains(t, err, testCase.message)
		})
	}
}

func TestSchemaTableOverrideCanAvoidConflict(t *testing.T) {
	router, err := nameroute.Parse([]string{
		"a:x",
		"b.t:x.u",
		"a.u:y.u",
	})
	require.NoError(t, err)

	schema, table, matched := router.Route(ast.NewCIStr("a"), ast.NewCIStr("u"))
	require.True(t, matched)
	require.Equal(t, "y", schema.O)
	require.Equal(t, "u", table.O)
}

func TestValidateTargets(t *testing.T) {
	router, err := nameroute.Parse([]string{"a:b"})
	require.NoError(t, err)

	err = router.ValidateTargets([]nameroute.ObjectName{
		{Schema: ast.NewCIStr("a"), Table: ast.NewCIStr("t")},
		{Schema: ast.NewCIStr("b"), Table: ast.NewCIStr("t")},
	})
	require.ErrorContains(t, err, "conflict at target `b`.`t`")

	err = router.ValidateTargets([]nameroute.ObjectName{
		{Schema: ast.NewCIStr("a"), Table: ast.NewCIStr("t")},
		{Schema: ast.NewCIStr("b"), Table: ast.NewCIStr("other")},
	})
	require.NoError(t, err)
}

func TestFingerprintChangesWithRuleContent(t *testing.T) {
	first, err := nameroute.Parse([]string{"a:b"})
	require.NoError(t, err)
	second, err := nameroute.Parse([]string{"a:c"})
	require.NoError(t, err)
	require.NotEqual(t, first.Fingerprint(), second.Fingerprint())
}
