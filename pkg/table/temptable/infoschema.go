// Copyright 2021 PingCAP, Inc.
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

package temptable

import (
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
)

// AttachLocalTemporaryTableInfoSchema attach local temporary table information schema to is
func AttachLocalTemporaryTableInfoSchema(sctx variable.SessionVarsProvider, is infoschema.InfoSchema) infoschema.InfoSchema {
	localTemporaryTables := getLocalTemporaryTables(sctx)
	if localTemporaryTables == nil {
		return is
	}
	if se, ok := is.(*infoschema.SessionExtendedInfoSchema); ok {
		se.LocalTemporaryTablesOnce.Do(func() {
			se.LocalTemporaryTables = localTemporaryTables
		})
		return is
	}

	return &infoschema.SessionExtendedInfoSchema{
		InfoSchema:           is,
		LocalTemporaryTables: localTemporaryTables,
	}
}

// DetachLocalTemporaryTableInfoSchema detach local temporary table information schema from is
func DetachLocalTemporaryTableInfoSchema(is infoschema.InfoSchema) infoschema.InfoSchema {
	if attachedInfoSchema, ok := is.(*infoschema.SessionExtendedInfoSchema); ok {
		return attachedInfoSchema.DetachTemporaryTableInfoSchema()
	}

	return is
}

func getLocalTemporaryTables(sctx variable.SessionVarsProvider) *infoschema.SessionTables {
	localTemporaryTables := sctx.GetSessionVars().LocalTemporaryTables
	if localTemporaryTables == nil {
		return nil
	}

	return localTemporaryTables.(*infoschema.SessionTables)
}

func ensureLocalTemporaryTables(sctx variable.SessionVarsProvider) *infoschema.SessionTables {
	sessVars := sctx.GetSessionVars()
	if sessVars.LocalTemporaryTables == nil {
		localTempTables := infoschema.NewSessionTables()
		sessVars.LocalTemporaryTables = localTempTables
		return localTempTables
	}

	return sessVars.LocalTemporaryTables.(*infoschema.SessionTables)
}
