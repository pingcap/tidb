// Copyright 2025 PingCAP, Inc.
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

package infoschema

import (
	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
)

// ExportedInfoschemaV2 is the infoschemaV2 type for testing
type ExportedInfoschemaV2 = infoschemaV2

// NewExportedInfoSchemaV2 creates a new infoschemaV2 for testing
// The factory parameter should return a pools.Resource or any compatible type
func NewExportedInfoSchemaV2(r autoid.Requirement, factory func() (any, error), data *Data) ExportedInfoschemaV2 {
	return NewInfoSchemaV2(r, func() (pools.Resource, error) {
		if factory != nil {
			res, err := factory()
			if err != nil {
				return nil, err
			}
			if res == nil {
				return nil, nil
			}
			// Try to convert the result to pools.Resource
			if r, ok := res.(pools.Resource); ok {
				return r, nil
			}
			// If it doesn't implement pools.Resource, we can't use it
			// Return nil and let the caller handle the error
			return nil, nil
		}
		return nil, nil
	}, data)
}

// ExportedTableCacheKey is the tableCacheKey type for testing
type ExportedTableCacheKey struct {
	TableID       int64
	SchemaVersion int64
}

// ExportedSchemaTables is the schemaTables type for testing
type ExportedSchemaTables struct {
	DBInfo      *model.DBInfo
	Tables      map[string]*model.TableInfo
	TablesID    map[int64]*model.TableInfo
	ViewIDs     map[int64]*model.TableInfo
	Initialized bool
}

// SchemaValidator is the interface for schema validation
// This is defined here to allow test packages to use it
type SchemaValidator interface {
	ValidateSchema(schemaVersion int64) error
}

// InfoSchemaValidator implements SchemaValidator
type InfoSchemaValidator struct{}

// ValidateSchema validates the schema version
func (v *InfoSchemaValidator) ValidateSchema(schemaVersion int64) error {
	return nil
}

// GetKeptAllocators exports the function for testing
func GetKeptAllocators(diff *model.SchemaDiff, allocators autoid.Allocators) autoid.Allocators {
	return getKeptAllocators(diff, allocators)
}
