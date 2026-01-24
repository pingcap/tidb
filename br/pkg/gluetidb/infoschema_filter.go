// Copyright 2025 PingCAP, Inc. Licensed under Apache-2.0.

package gluetidb

import (
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/infoschema/issyncer"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"go.uber.org/zap"
)

// brInfoSchemaFilter implements issyncer.Filter and encapsulates BR-specific
// filtering logic based on database names.
type brInfoSchemaFilter struct {
	allow func(pmodel.CIStr) bool
}

// NewInfoSchemaFilter builds a BR-specific filter from a DB-name predicate.
// It returns nil if the predicate is nil so the default loading behavior is used.
func NewInfoSchemaFilter(allow func(pmodel.CIStr) bool) issyncer.Filter {
	if allow == nil {
		return nil
	}
	return &brInfoSchemaFilter{allow: allow}
}

func (f *brInfoSchemaFilter) SkipLoadDiff(diff *model.SchemaDiff, latestIS infoschema.InfoSchema) (skip bool) {
	defer func() {
		if skip {
			log.Info("skip load a schema diff due to configuration.",
				zap.Stringer("type", diff.Type),
				zap.Int64("schema-id", diff.SchemaID),
				zap.Int64("table-id", diff.TableID),
				zap.Int64("old-schema-id", diff.OldSchemaID),
				zap.Int64("version", diff.Version))
		}
	}()

	if f == nil || f.allow == nil {
		return false
	}
	// Always accept newly created schema as we cannot access its name in this context.
	switch diff.Type {
	case model.ActionCreateSchema,
		// Always accept `PLACEMENT POLICY` SQLs: its `schemaID` is ID of this policy but not zero.
		model.ActionCreatePlacementPolicy, model.ActionAlterPlacementPolicy, model.ActionDropPlacementPolicy,
		// Always accept resource group related SQLs: their `schemaID` is resource group ID.
		model.ActionCreateResourceGroup, model.ActionDropResourceGroup, model.ActionAlterResourceGroup:
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
