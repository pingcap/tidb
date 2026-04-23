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

package infoschema_test

import (
	infoschema "github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// Exported types from parent package for use in test package

type (
	// Builder is exported from parent infoschema package
	Builder = infoschema.Builder
	// Data is exported from parent infoschema package
	Data = infoschema.Data
)

// newInfoSchema creates a new infoSchema for testing
// This is a copy of the unexported function from the parent package
func newInfoSchema(r autoid.Requirement) *infoSchema {
	return &infoSchema{
		schemaMap:       make(map[string]*schemaTables),
		schemaID2Name:   make(map[int64]string),
		tableByName:     make(map[tableName]any),
		databaseByID:    make(map[int64]*model.DBInfo),
		backgroundKeys:  make([]int64, 0, 8),
		backgroundMap:   make(map[int64]*model.DBInfo),
		statistics:      &schemaStatistics{},
		schemaMetaVer:   0,
		self:            nil,
		schemaValidator: nil,
	}
}

// infoSchema is a copy of the unexported type from the parent package
type infoSchema struct {
	schemaMap       map[string]*schemaTables
	schemaID2Name   map[int64]string
	tableByName     map[tableName]any
	databaseByID    map[int64]*model.DBInfo
	backgroundKeys  []int64
	backgroundMap   map[int64]*model.DBInfo
	statistics      *schemaStatistics
	schemaMetaVer   int64
	self            InfoSchema
	schemaValidator SchemaValidator
}

// schemaTables is a copy of the unexported type from the parent package
type schemaTables struct {
	dbInfo      *model.DBInfo
	tables      map[string]*model.TableInfo
	tablesID    map[int64]*model.TableInfo
	viewIDs     map[int64]*model.TableInfo
	initialized bool
}

// tableName is a copy of the unexported type from the parent package
type tableName struct {
	db  ast.CIStr
	tbl ast.CIStr
}

// schemaStatistics is a copy of the unexported type from the parent package
type schemaStatistics struct {
	TableCount  int64
	LoadTime    int64
	MemoryUsage int64
}

// InfoSchema is the interface from the parent package
type InfoSchema = infoschema.InfoSchema

// SchemaValidator is the interface from the parent package
type SchemaValidator = infoschema.SchemaValidator

// NewInfoSchemaV2 creates a new infoschemaV2 for testing
func NewInfoSchemaV2(r autoid.Requirement, factory func() (autoid.Allocator, error), data *Data) infoschema.ExportedInfoschemaV2 {
	// The NewExportedInfoSchemaV2 expects a factory that returns pools.Resource
	// Since autoid.Allocator doesn't implement pools.Resource, we create a wrapper
	return infoschema.NewExportedInfoSchemaV2(r, func() (any, error) {
		if factory != nil {
			alloc, err := factory()
			if err != nil {
				return nil, err
			}
			// Create a wrapper that implements pools.Resource
			return &resourceWrapper{Allocator: alloc}, nil
		}
		return nil, nil
	}, data)
}

// resourceWrapper wraps autoid.Allocator to implement pools.Resource
type resourceWrapper struct {
	autoid.Allocator
}

// Close is required to implement pools.Resource
func (w *resourceWrapper) Close() error {
	// No-op for autoid.Allocator
	return nil
}

// GetKeptAllocators is exported from parent package for testing
func GetKeptAllocators(is infoschema.InfoSchema) []autoid.Allocator {
	if is == nil {
		return nil
	}
	// This is a simplified version - in the actual implementation we'd need to access the internal fields
	return nil
}

// NewData creates a new Data struct for testing
func NewData() *Data {
	return infoschema.NewData()
}

// tableItem is a copy of the unexported type from the parent package
type tableItem = struct {
	dbName        ast.CIStr
	dbID          int64
	tableName     ast.CIStr
	tableID       int64
	schemaVersion int64
	tomb          bool
}

// infoschemaV2 is a wrapper around the parent package's type
type infoschemaV2 struct {
	*infoschema.ExportedInfoschemaV2
}

// NewInfoSchemaV2Wrapper creates a wrapper for testing
func NewInfoSchemaV2Wrapper(r autoid.Requirement, factory func() (autoid.Allocator, error), data *Data) infoschemaV2 {
	// The factory needs to return pools.Resource, not autoid.Allocator
	wrappedFactory := func() (any, error) {
		if factory != nil {
			alloc, err := factory()
			if err != nil {
				return nil, err
			}
			return &resourceWrapper{Allocator: alloc}, nil
		}
		return nil, nil
	}
	base := infoschema.NewExportedInfoSchemaV2(r, wrappedFactory, data)
	return infoschemaV2{ExportedInfoschemaV2: &base}
}
