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

package meta

import (
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/structure"
)

// Reader is the meta reader
type Reader interface {
	GetDatabase(dbID int64) (*model.DBInfo, error)
	ListDatabases() ([]*model.DBInfo, error)
	GetTable(dbID int64, tableID int64) (*model.TableInfo, error)
	ListTables(dbID int64) ([]*model.TableInfo, error)
	ListSimpleTables(dbID int64) ([]*model.TableNameInfo, error)
	IterTables(dbID int64, fn func(info *model.TableInfo) error) error
	GetAutoIDAccessors(dbID, tableID int64) AutoIDAccessors
	GetAllNameToIDAndTheMustLoadedTableInfo(dbID int64) (map[string]int64, []*model.TableInfo, error)

	GetMetadataLock() (enable bool, isNull bool, err error)
	GetHistoryDDLJob(id int64) (*model.Job, error)
	GetHistoryDDLCount() (uint64, error)
	GetLastHistoryDDLJobsIterator() (LastJobIterator, error)
	GetHistoryDDLJobsIterator(startJobID int64) (LastJobIterator, error)

	GetSchemaVersion() (int64, error)
	EncodeSchemaDiffKey(schemaVersion int64) kv.Key
	GetSchemaDiff(schemaVersion int64) (*model.SchemaDiff, error)
	GetSchemaVersionWithNonEmptyDiff() (int64, error)

	GetPolicyID() (int64, error)
	GetPolicy(policyID int64) (*model.PolicyInfo, error)
	ListPolicies() ([]*model.PolicyInfo, error)

	GetRUStats() (*RUStats, error)
	GetResourceGroup(groupID int64) (*model.ResourceGroupInfo, error)
	ListResourceGroups() ([]*model.ResourceGroupInfo, error)

	GetMetasByDBID(dbID int64) ([]structure.HashPair, error)
	GetGlobalID() (int64, error)
	GetBDRRole() (string, error)
	GetSystemDBID() (int64, error)
	GetSchemaCacheSize() (size uint64, isNull bool, err error)
	GetBootstrapVersion() (int64, error)
}

// NewReader creates a meta Reader in snapshot.
func NewReader(snapshot kv.Snapshot) Reader {
	snapshot.SetOption(kv.RequestSourceInternal, true)
	snapshot.SetOption(kv.RequestSourceType, kv.InternalTxnMeta)
	t := structure.NewStructure(snapshot, nil, mMetaPrefix)
	return &Mutator{txn: t}
}
