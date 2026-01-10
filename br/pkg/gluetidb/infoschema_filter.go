// Copyright 2025 PingCAP, Inc. Licensed under Apache-2.0.

package gluetidb

import (
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/infoschema/issyncer"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"go.uber.org/zap"
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

func (f *brInfoSchemaFilter) SkipLoadDiff(diff *model.SchemaDiff, latestIS infoschema.InfoSchema) (skip bool) {
	defer func() {
		if skip {
			log.Warn("skip load a schema diff due to configuration.", zap.Any("diff", diff), zap.Int64("version", diff.Version))
		}
	}()

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
