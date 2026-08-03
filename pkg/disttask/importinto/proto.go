// Copyright 2023 PingCAP, Inc.
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

package importinto

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/lightning/verification"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"go.uber.org/zap"
)

// TaskMeta is the task of IMPORT INTO.
// All the field should be serializable.
type TaskMeta struct {
	// IMPORT INTO job id, see mysql.tidb_import_jobs.
	JobID  int64
	Plan   importer.Plan
	Stmt   string
	Result Result
	// eligible instances to run this task, we run on all instances if it's empty.
	// we only need this when run IMPORT INTO without distributed option now, i.e.
	// running on the instance that initiate the IMPORT INTO.
	EligibleInstances []*infosync.ServerInfo
	// the file chunks to import, when import from server file, we need to pass those
	// files to the framework scheduler which might run on another instance.
	// we use a map from engine ID to chunks since we need support split_file for CSV,
	// so need to split them into engines before passing to scheduler.
	ChunkMap map[int32][]Chunk
}

// ImportStepMeta is the meta of import step.
// Scheduler will split the task into subtasks(FileInfos -> Chunks)
// All the field should be serializable.
type ImportStepMeta struct {
	external.BaseExternalMeta
	// this is the engine ID, not the id in tidb_background_subtask table.
	ID       int32
	Chunks   []Chunk            `external:"true"`
	Checksum map[int64]Checksum // see KVGroupChecksum for definition of map key.
	Result   Result
	// MaxIDs stores the max id that have been used during encoding for each allocator type.
	// the max id is same among all allocator types for now, since we're using same base, see
	// NewPanickingAllocators for more info.
	MaxIDs map[autoid.AllocatorType]int64

	SortedDataMeta *external.SortedKVMeta `external:"true"`
	// SortedIndexMetas is a map from index id to its sorted kv meta.
	SortedIndexMetas map[int64]*external.SortedKVMeta `external:"true"`
	// it's the sum of all conflict KVs in all SortedKVMeta, we keep it here to
	// avoid get the external meta when no conflict KVs.
	RecordedConflictKVCount uint64
}

// Marshal marshals the import step meta to JSON.
func (m *ImportStepMeta) Marshal() ([]byte, error) {
	return m.BaseExternalMeta.Marshal(m)
}

const (
	// dataKVGroup is the group name of the sorted kv for data.
	// index kv will be stored in a group named as index-id.
	dataKVGroup = "data"
)

// MergeSortStepMeta is the meta of merge sort step.
type MergeSortStepMeta struct {
	external.BaseExternalMeta
	// KVGroup is the group name of the sorted kv, either dataKVGroup or index-id.
	KVGroup                 string   `json:"kv-group"`
	DataFiles               []string `json:"data-files" external:"true"`
	external.SortedKVMeta   `external:"true"`
	RecordedConflictKVCount uint64 `json:"recorded-conflict-kv-count,omitempty"`
}

// Marshal the merge sort step meta to JSON.
func (m *MergeSortStepMeta) Marshal() ([]byte, error) {
	return m.BaseExternalMeta.Marshal(m)
}

// KVGroupConflictInfos is the conflict infos of a kv group.
type KVGroupConflictInfos struct {
	ConflictInfos map[string]*common.ConflictInfo `json:"conflict-infos,omitempty"`
}

func (gci *KVGroupConflictInfos) addDataConflictInfo(other *common.ConflictInfo) {
	gci.addConflictInfo(dataKVGroup, other)
}

func (gci *KVGroupConflictInfos) addIndexConflictInfo(indexID int64, other *common.ConflictInfo) {
	kvGroup := IndexID2KVGroup(indexID)
	gci.addConflictInfo(kvGroup, other)
}

func (gci *KVGroupConflictInfos) addConflictInfo(kvGroup string, other *common.ConflictInfo) {
	if other.Count == 0 {
		return
	}
	if gci.ConflictInfos == nil {
		gci.ConflictInfos = make(map[string]*common.ConflictInfo, 1)
	}
	ci, ok := gci.ConflictInfos[kvGroup]
	if !ok {
		ci = &common.ConflictInfo{}
		gci.ConflictInfos[kvGroup] = ci
	}
	ci.Merge(other)
}

// CollectConflictsStepMeta is the meta of collect conflicts step.
type CollectConflictsStepMeta struct {
	external.BaseExternalMeta
	Infos                   KVGroupConflictInfos `json:"infos" external:"true"`
	RecordedDataKVConflicts int64                `json:"recorded-data-kv-conflicts,omitempty"`
	// Checksum is the checksum of all conflicts rows.
	Checksum *Checksum `json:"checksum,omitempty"`
	// ConflictedRowCount is the count of all conflicted rows.
	ConflictedRowCount int64 `json:"conflicted-row-count,omitempty"`
	// ConflictedRowFilenames is the filenames of all conflicted rows.
	// Note: this file is for user to resolve conflicts manually.
	ConflictedRowFilenames []string `json:"conflicted-row-filenames,omitempty"`
	// TooManyConflictsFromIndex is true if there are too many conflicts from index.
	// if true, we will skip checksum.
	TooManyConflictsFromIndex bool `json:"too-many-conflicts-from-index,omitempty"`
}

// Marshal marshals the collect conflicts step meta to JSON.
func (m *CollectConflictsStepMeta) Marshal() ([]byte, error) {
	return m.BaseExternalMeta.Marshal(m)
}

// ConflictResolutionStepMeta is the meta of conflict resolution step.
type ConflictResolutionStepMeta struct {
	external.BaseExternalMeta
	Infos KVGroupConflictInfos `json:"infos" external:"true"`
}

// Marshal marshals the conflict resolution step meta to JSON.
func (m *ConflictResolutionStepMeta) Marshal() ([]byte, error) {
	return m.BaseExternalMeta.Marshal(m)
}

// WriteIngestStepMeta is the meta of write and ingest step.
// only used when global sort is enabled.
type WriteIngestStepMeta struct {
	external.BaseExternalMeta
	KVGroup                 string `json:"kv-group"`
	external.SortedKVMeta   `json:"sorted-kv-meta" external:"true"`
	RecordedConflictKVCount uint64   `json:"recorded-conflict-kv-count,omitempty"`
	DataFiles               []string `json:"data-files" external:"true"`
	StatFiles               []string `json:"stat-files" external:"true"`
	RangeJobKeys            [][]byte `json:"range-job-keys" external:"true"`
	RangeSplitKeys          [][]byte `json:"range-split-keys" external:"true"`
	TS                      uint64   `json:"ts"`

	Result Result
}

// Marshal marshals the write ingest step meta to JSON.
func (m *WriteIngestStepMeta) Marshal() ([]byte, error) {
	return m.BaseExternalMeta.Marshal(m)
}

// PostProcessStepMeta is the meta of post process step.
type PostProcessStepMeta struct {
	// accumulated checksum of all subtasks in encode step. See KVGroupChecksum
	// for definition of map key.
	Checksum map[int64]Checksum
	// DeletedRowsChecksum is the checksum of all deleted rows due to conflicts.
	DeletedRowsChecksum Checksum
	// TooManyConflictsFromIndex is true if there are too many conflicts from index.
	// if true, the DeletedRowsChecksum might not be accurate, we will skip checksum.
	TooManyConflictsFromIndex bool `json:"too-many-conflicts-from-index,omitempty"`
	// MaxIDs of max all max-ids of subtasks in import step.
	MaxIDs map[autoid.AllocatorType]int64
}

// SharedVars is the shared variables of all minimal tasks in a subtask.
// This is because subtasks cannot directly obtain the results of the minimal subtask.
// All the fields should be concurrent safe.
type SharedVars struct {
	TableImporter *importer.TableImporter
	DataEngine    *backend.OpenedEngine
	IndexEngine   *backend.OpenedEngine

	mu       sync.Mutex
	Checksum *verification.KVGroupChecksum

	SortedDataMeta *external.SortedKVMeta
	// SortedIndexMetas is a map from index id to its sorted kv meta.
	SortedIndexMetas        map[int64]*external.SortedKVMeta
	RecordedConflictKVCount uint64
	ShareMu                 sync.Mutex
}

func (sv *SharedVars) mergeDataSummary(summary *external.WriterSummary) {
	sv.mu.Lock()
	defer sv.mu.Unlock()
	sv.SortedDataMeta.MergeSummary(summary)
	sv.RecordedConflictKVCount += summary.ConflictInfo.Count
}

func (sv *SharedVars) mergeIndexSummary(indexID int64, summary *external.WriterSummary) {
	sv.mu.Lock()
	defer sv.mu.Unlock()
	sv.RecordedConflictKVCount += summary.ConflictInfo.Count

	meta, ok := sv.SortedIndexMetas[indexID]
	if !ok {
		meta = external.NewSortedKVMeta(summary)
		sv.SortedIndexMetas[indexID] = meta
		return
	}
	meta.MergeSummary(summary)
}

// importStepMinimalTask is the minimal task of IMPORT INTO.
// TaskExecutor will split the subtask into minimal tasks(Chunks -> Chunk)
type importStepMinimalTask struct {
	Plan       importer.Plan
	Chunk      Chunk
	SharedVars *SharedVars
	logger     *zap.Logger
}

// RecoverArgs implements workerpool.TaskMayPanic interface.
func (*importStepMinimalTask) RecoverArgs() (metricsLabel string, funcInfo string, err error) {
	return proto.ImportInto.String(), "importStepMininalTask", errors.Errorf("panic occurred during import, please check log")
}

func (t *importStepMinimalTask) String() string {
	return fmt.Sprintf("chunk:%s:%d", t.Chunk.Path, t.Chunk.Offset)
}

// Chunk records the chunk information.
type Chunk struct {
	Path         string
	FileSize     int64
	Offset       int64
	EndOffset    int64
	PrevRowIDMax int64
	RowIDMax     int64
	Type         mydump.SourceType
	Compression  mydump.Compression
	Timestamp    int64
}

// Checksum records the checksum information.
type Checksum struct {
	Sum  uint64
	KVs  uint64
	Size uint64
}

func newFromKVChecksum(sum *verification.KVChecksum) *Checksum {
	return &Checksum{
		Sum:  sum.Sum(),
		KVs:  sum.SumKVS(),
		Size: sum.SumSize(),
	}
}

// ToKVChecksum converts the Checksum to verification.KVChecksum.
func (c *Checksum) ToKVChecksum() *verification.KVChecksum {
	sum := verification.MakeKVChecksum(c.Size, c.KVs, c.Sum)
	return &sum
}

// Result records the metrics information.
// This portion of the code may be implemented uniformly in the framework in the future.
type Result struct {
	LoadedRowCnt     uint64
	ConflictedRowCnt uint64
}

// IndexID2KVGroup converts index id to kv group name.
// exported for test.
func IndexID2KVGroup(indexID int64) string {
	return fmt.Sprintf("%d", indexID)
}

func kvGroup2IndexID(kvGroup string) (int64, error) {
	return strconv.ParseInt(kvGroup, 10, 64)
}
