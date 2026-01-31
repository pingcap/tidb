package parser

import "github.com/pingcap/tidb/pkg/parser/ast"

type mviewCreateOptions struct {
	hasComment bool
	comment    string

	hasTiFlashReplicas bool
	tiflashReplicas    uint64

	hasRefresh bool
	refresh    *ast.MViewRefreshClause
}

type mlogCreateOptions struct {
	hasIncludingNewVals bool
	includingNewVals    bool

	hasPurge bool
	purge    *ast.MLogPurgeClause
}
