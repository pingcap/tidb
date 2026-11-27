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

package kvsource

import (
	"bytes"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor"
	tidbmeta "github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
)

// CreateDatabaseSQL renders a database schema with the offline export session settings.
func CreateDatabaseSQL(database *model.DBInfo) (string, error) {
	var output bytes.Buffer
	if err := executor.ConstructResultOfShowCreateDatabase(newSchemaContext(), database, true, &output); err != nil {
		return "", errors.Annotatef(err, "build schema for kv database %q", database.Name.O)
	}
	return output.String(), nil
}

// CreateTableSQL renders a table schema using persisted snapshot AutoID bases.
func CreateTableSQL(reader tidbmeta.Reader, database *model.DBInfo, table *model.TableInfo) (string, error) {
	allocators, err := metadataAllocators(reader, database.ID, table)
	if err != nil {
		return "", errors.Annotatef(err, "read auto ID bases for kv table %q", table.Name.O)
	}
	var output bytes.Buffer
	if err := executor.ConstructResultOfShowCreateTable(
		newSchemaContext(), database.Name, table, allocators, &output,
	); err != nil {
		return "", errors.Annotatef(err, "build schema for kv table %q", table.Name.O)
	}
	return output.String(), nil
}

// CreatePlacementPolicySQL renders a placement policy using TiDB SHOW CREATE semantics.
func CreatePlacementPolicySQL(policy *model.PolicyInfo) string {
	return executor.ConstructResultOfShowCreatePlacementPolicy(policy)
}
