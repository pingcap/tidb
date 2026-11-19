// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package parser_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

func TestMFAUserStatements(t *testing.T) {
	p := parser.New()
	stmt, err := p.ParseOneStmt("CREATE USER 'u'@'%' IDENTIFIED BY 'first' AND IDENTIFIED WITH authentication_ldap_simple AS 'uid=second'", "", "")
	require.NoError(t, err)
	create := stmt.(*ast.CreateUserStmt)
	require.Len(t, create.Specs, 1)
	require.Len(t, create.Specs[0].AuthFactors, 1)
	require.Equal(t, "authentication_ldap_simple", create.Specs[0].AuthFactors[0].AuthPlugin)

	stmt, err = p.ParseOneStmt("ALTER USER 'u'@'%' MODIFY 3 FACTOR IDENTIFIED WITH authentication_ldap_simple AS 'uid=third' MODIFY 2 FACTOR IDENTIFIED WITH authentication_ldap_simple AS 'uid=second-new'", "", "")
	require.NoError(t, err)
	alter := stmt.(*ast.AlterUserStmt)
	require.Len(t, alter.Specs[0].AuthFactorOps, 2)
	require.Equal(t, 3, alter.Specs[0].AuthFactorOps[0].Factor)
	require.Equal(t, 2, alter.Specs[0].AuthFactorOps[1].Factor)

	_, err = p.ParseOneStmt("ALTER USER 'u'@'%' ADD 2 FACTOR IDENTIFIED WITH authentication_ldap_simple AS 'uid=second'", "", "")
	require.NoError(t, err)
}
