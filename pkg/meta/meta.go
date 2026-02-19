// Copyright 2015 PingCAP, Inc.
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
	"math"
	"strconv"
	"sync"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/resourcegroup"
	"github.com/pingcap/tidb/pkg/structure"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

var (
	globalIDMutex sync.Mutex
	policyIDMutex sync.Mutex
)

// Meta structure:
//	NextGlobalID -> int64
//	SchemaVersion -> int64
//	DBs -> {
//		DB:1 -> db meta data []byte
//		DB:2 -> db meta data []byte
//	}
//	DB:1 -> {
//		Table:1 -> table meta data []byte
//		Table:2 -> table meta data []byte
//		TID:1 -> int64
//		TID:2 -> int64
//	}
//
// DDL version 2
// Names -> {
//		Name:DBname\x00tablename -> tableID
// }

var (
	mMetaPrefix = []byte("m")
	// the value inside it is actually the max current used ID, not next id.
	mNextGlobalIDKey     = []byte("NextGlobalID")
	mSchemaVersionKey    = []byte("SchemaVersionKey")
	mDBs                 = []byte("DBs")
	mDBPrefix            = "DB"
	mTablePrefix         = "Table"
	mSequencePrefix      = "SID"
	mSeqCyclePrefix      = "SequenceCycle"
	mTableIDPrefix       = "TID"
	mIncIDPrefix         = "IID"
	mRandomIDPrefix      = "TARID"
	mBootstrapKey        = []byte("BootstrapKey")
	mSchemaDiffPrefix    = "Diff"
	mPolicies            = []byte("Policies")
	mPolicyPrefix        = "Policy"
	mResourceGroups      = []byte("ResourceGroups")
	mResourceGroupPrefix = "RG"
	mPolicyGlobalID      = []byte("PolicyGlobalID")
	mPolicyMagicByte     = CurrentMagicByteVer
	mDDLTableVersion     = []byte("DDLTableVersion")
	// the name doesn't contain nextgen, as we might impl the same logic in classic
	// kernel later, then we can reuse the same meta key.
	mBootTableVersion = []byte("BootTableVersion")
	mBDRRole          = []byte("BDRRole")
	mMetaDataLock     = []byte("metadataLock")
	mSchemaCacheSize  = []byte("SchemaCacheSize")
	mRequestUnitStats = []byte("RequestUnitStats")

	mIngestMaxBatchSplitRangesKey  = []byte("IngestMaxBatchSplitRanges")
	mIngestMaxSplitRangesPerSecKey = []byte("IngestMaxSplitRangesPerSec")
	mIngestMaxInflightKey          = []byte("IngestMaxInflight")
	mIngestMaxPerSecKey            = []byte("IngestMaxReqPerSec")
	mDXFScheduleTuneKey            = []byte("DXFScheduleTune")

	// the id for 'default' group, the internal ddl can ensure
	// user created resource group won't duplicate with this id.
	defaultGroupID = int64(1)
	// the default meta of the `default` group
	defaultRGroupMeta = &model.ResourceGroupInfo{
		ResourceGroupSettings: &model.ResourceGroupSettings{
			RURate:     math.MaxInt32,
			BurstLimit: -1,
			Priority:   ast.MediumPriorityValue,
		},
		ID:    defaultGroupID,
		Name:  ast.NewCIStr(resourcegroup.DefaultResourceGroupName),
		State: model.StatePublic,
	}
)

const (
	// CurrentMagicByteVer is the current magic byte version, used for future meta compatibility.
	CurrentMagicByteVer byte = 0x00
	// PolicyMagicByte handler
	// 0x00 - 0x3F: Json Handler
	// 0x40 - 0x7F: Reserved
	// 0x80 - 0xBF: Reserved
	// 0xC0 - 0xFF: Reserved

	// type means how to handle the serialized data.
	typeUnknown int = 0
	typeJSON    int = 1
	// todo: customized handler.
)

var (
	// ErrDBExists is the error for db exists.
	ErrDBExists = dbterror.ClassMeta.NewStd(mysql.ErrDBCreateExists)
	// ErrDBNotExists is the error for db not exists.
	ErrDBNotExists = dbterror.ClassMeta.NewStd(mysql.ErrBadDB)
	// ErrPolicyExists is the error for policy exists.
	ErrPolicyExists = dbterror.ClassMeta.NewStd(errno.ErrPlacementPolicyExists)
	// ErrPolicyNotExists is the error for policy not exists.
	ErrPolicyNotExists = dbterror.ClassMeta.NewStd(errno.ErrPlacementPolicyNotExists)
	// ErrResourceGroupExists is the error for resource group exists.
	ErrResourceGroupExists = dbterror.ClassMeta.NewStd(errno.ErrResourceGroupExists)
	// ErrResourceGroupNotExists is the error for resource group not exists.
	ErrResourceGroupNotExists = dbterror.ClassMeta.NewStd(errno.ErrResourceGroupNotExists)
	// ErrTableExists is the error for table exists.
	ErrTableExists = dbterror.ClassMeta.NewStd(mysql.ErrTableExists)
	// ErrTableNotExists is the error for table not exists.
	ErrTableNotExists = dbterror.ClassMeta.NewStd(mysql.ErrNoSuchTable)
	// ErrDDLReorgElementNotExist is the error for reorg element not exists.
	ErrDDLReorgElementNotExist = dbterror.ClassMeta.NewStd(errno.ErrDDLReorgElementNotExist)
	// ErrInvalidString is the error for invalid string to parse
	ErrInvalidString = dbterror.ClassMeta.NewStd(errno.ErrInvalidCharacterString)
)

// NextGenBootTableVersion is the version of nextgen bootstrapping.
// it serves the same purpose as DDLTableVersion, to avoid the same table created
// twice, as we are creating those tables in meta kv directly, without going
// through DDL.
type NextGenBootTableVersion int

const (
	// InitNextGenBootTableVersion means it's a fresh cluster, we haven't bootstrapped yet.
	InitNextGenBootTableVersion NextGenBootTableVersion = 0
	// BaseNextGenBootTableVersion is the first version of nextgen bootstrapping, we
	// will create 52 physical tables.
	// Note: DDL related tables are created separately, see DDLTableVersion.
	BaseNextGenBootTableVersion NextGenBootTableVersion = 1
)

// DDLTableVersion is to display ddl related table versions
type DDLTableVersion int

const (
	// InitDDLTableVersion is the original version.
	InitDDLTableVersion DDLTableVersion = 0
	// BaseDDLTableVersion is for support concurrent DDL, it added tidb_ddl_job, tidb_ddl_reorg and tidb_ddl_history.
	BaseDDLTableVersion DDLTableVersion = 1
	// MDLTableVersion is for support MDL tables.
	MDLTableVersion DDLTableVersion = 2
	// BackfillTableVersion is for support distributed reorg stage, it added tidb_background_subtask, tidb_background_subtask_history.
	BackfillTableVersion DDLTableVersion = 3
	// DDLNotifierTableVersion is for support ddl notifier, it added tidb_ddl_notifier.
	DDLNotifierTableVersion DDLTableVersion = 4
)

func encodeIntVal(i int) []byte {
	return []byte(strconv.Itoa(i))
}

// Option is for Mutator option.
type Option func(m *Mutator)

// Mutator is for handling meta information in a transaction.
type Mutator struct {
	txn     *structure.TxStructure
	StartTS uint64 // StartTS is the txn's start TS.
}

var _ Reader = (*Mutator)(nil)

// NewMutator creates a meta Mutator in transaction txn.
// If the current Mutator needs to handle a job, jobListKey is the type of the job's list.
func NewMutator(txn kv.Transaction, options ...Option) *Mutator {
	txn.SetOption(kv.Priority, kv.PriorityHigh)
	txn.SetDiskFullOpt(kvrpcpb.DiskFullOpt_AllowedOnAlmostFull)
	t := structure.NewStructure(txn, txn, mMetaPrefix)
	m := &Mutator{txn: t,
		StartTS: txn.StartTS(),
	}
	for _, opt := range options {
		opt(m)
	}
	return m
}
