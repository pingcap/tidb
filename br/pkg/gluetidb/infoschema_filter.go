// Copyright 2025 PingCAP, Inc. Licensed under Apache-2.0.

package gluetidb

import (
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/infoschema/issyncer"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// brInfoSchemaFilter implements issyncer.Filter and encapsulates BR-specific
// filtering logic based on database names.
type brInfoSchemaFilter struct {
	allow func(ast.CIStr) bool
}

// NewInfoSchemaFilter builds a BR-specific filter from a DB-name predicate.
// It returns nil if the predicate is nil so the default loading behavior is used.
func NewInfoSchemaFilter(allow func(ast.CIStr) bool) issyncer.Filter {
	if allow == nil {
		return nil
	}
	return &brInfoSchemaFilter{allow: allow}
}

func (f *brInfoSchemaFilter) SkipLoadDiff(diff *model.SchemaDiff, latestIS infoschema.InfoSchema) bool {
	if f == nil || f.allow == nil {
		return false
	}
	// Always accept newly created schema as we cannot access its name in this context.
	// Always accept `CREATE PLACEMENT POLICY`: its `schemaID` is ID of this policy but not zero.
	if diff.Type == model.ActionCreateSchema || diff.Type == model.ActionCreatePlacementPolicy {
		return false
	}
	// Always accept db unrelated DDLs.
	if diff.SchemaID == 0 {
		return false
	}
	if latestIS == nil {
		return true
	}
	schema, ok := latestIS.SchemaByID(diff.SchemaID)
	selected := ok && f.allow(schema.Name)
	return !selected
}

func (f *brInfoSchemaFilter) SkipLoadSchema(dbInfo *model.DBInfo) bool {
	if f == nil || f.allow == nil || dbInfo == nil {
		return false
	}
	return !f.allow(dbInfo.Name)
}

func (f *brInfoSchemaFilter) SkipMDLCheck(tableIDs map[int64]struct{}, latestIS infoschema.InfoSchema) bool {
	if f == nil || f.allow == nil || latestIS == nil {
		return false
	}
	for id := range tableIDs {
		db, ok := latestIS.SchemaByID(id)
		isAllowed := ok && f.allow(db.Name)
		if !isAllowed {
			return false
		}
	}
	return true
}
