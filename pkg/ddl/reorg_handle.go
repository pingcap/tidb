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

package ddl

import (
	"bytes"
	"context"
	"encoding/hex"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"go.uber.org/zap"
)

func getSplitKeysForTempIndexRanges(pid int64, elements []*meta.Element) []kv.Key {
	splitKeys := make([]kv.Key, 0, len(elements))
	for _, e := range elements {
		if !bytes.Equal(e.TypeKey, meta.IndexElementKey) {
			continue
		}
		tempIdxID := tablecodec.TempIndexPrefix | e.ID
		splitKey := tablecodec.EncodeIndexSeekKey(pid, tempIdxID, nil)
		splitKeys = append(splitKeys, splitKey)
	}
	return splitKeys
}

func encodeTempIndexRange(physicalID, firstIdxID, lastIdxID int64) (start kv.Key, end kv.Key) {
	firstElemTempID := tablecodec.TempIndexPrefix | firstIdxID
	lastElemTempID := tablecodec.TempIndexPrefix | lastIdxID
	start = tablecodec.EncodeIndexSeekKey(physicalID, firstElemTempID, nil)
	end = tablecodec.EncodeIndexSeekKey(physicalID, lastElemTempID, []byte{255})
	return start, end
}

func getReorgInfoFromPartitions(ctx *ReorgContext, jobCtx *jobContext, rh *reorgHandler, job *model.Job, dbInfo *model.DBInfo, tbl table.PartitionedTable, partitionIDs []int64, elements []*meta.Element) (*reorgInfo, error) {
	var (
		element *meta.Element
		start   kv.Key
		end     kv.Key
		pid     int64
		info    reorgInfo
	)
	if job.SnapshotVer == 0 {
		info.first = true
		delayForAsyncCommit()
		ver, err := getValidCurrentVersion(jobCtx.store)
		if err != nil {
			return nil, errors.Trace(err)
		}
		pid = partitionIDs[0]
		physTbl := tbl.GetPartition(pid)

		start, end, err = getTableRange(ctx, jobCtx.store, physTbl, ver.Ver, job.Priority)
		if err != nil {
			return nil, errors.Trace(err)
		}
		logutil.DDLLogger().Info("job get table range",
			zap.Int64("job ID", job.ID), zap.Int64("physical table ID", pid),
			zap.String("start key", hex.EncodeToString(start)),
			zap.String("end key", hex.EncodeToString(end)))

		err = rh.InitDDLReorgHandle(job, start, end, pid, elements[0])
		if err != nil {
			return &info, errors.Trace(err)
		}
		// Update info should after data persistent.
		job.SnapshotVer = ver.Ver
		element = elements[0]
	} else {
		var err error
		element, start, end, pid, err = rh.GetDDLReorgHandle(job)
		if err != nil {
			// If the reorg element doesn't exist, this reorg info should be saved by the older TiDB versions.
			// It's compatible with the older TiDB versions.
			// We'll try to remove it in the next major TiDB version.
			if meta.ErrDDLReorgElementNotExist.Equal(err) {
				job.SnapshotVer = 0
				logutil.DDLLogger().Warn("get reorg info, the element does not exist", zap.Stringer("job", job))
			}
			return &info, errors.Trace(err)
		}
	}
	info.Job = job
	info.jobCtx = jobCtx
	info.StartKey = start
	info.EndKey = end
	info.PhysicalTableID = pid
	info.currElement = element
	info.elements = elements
	info.dbInfo = dbInfo

	return &info, nil
}

// UpdateReorgMeta creates a new transaction and updates tidb_ddl_reorg table,
// so the reorg can restart in case of issues.
func (r *reorgInfo) UpdateReorgMeta(startKey kv.Key, pool *sess.Pool) (err error) {
	if startKey == nil && r.EndKey == nil {
		return nil
	}
	sctx, err := pool.Get()
	if err != nil {
		return
	}
	defer pool.Put(sctx)

	se := sess.NewSession(sctx)
	err = se.Begin(context.Background())
	if err != nil {
		return
	}
	rh := newReorgHandler(se)
	err = updateDDLReorgHandle(rh.s, r.Job.ID, startKey, r.EndKey, r.PhysicalTableID, r.currElement)
	err1 := se.Commit(context.Background())
	if err == nil {
		err = err1
	}
	return errors.Trace(err)
}

// reorgHandler is used to handle the reorg information duration reorganization DDL job.
type reorgHandler struct {
	s *sess.Session
}

// NewReorgHandlerForTest creates a new reorgHandler, only used in test.
func NewReorgHandlerForTest(se sessionctx.Context) *reorgHandler {
	return newReorgHandler(sess.NewSession(se))
}

func newReorgHandler(sess *sess.Session) *reorgHandler {
	return &reorgHandler{s: sess}
}

// InitDDLReorgHandle initializes the job reorganization information.
func (r *reorgHandler) InitDDLReorgHandle(job *model.Job, startKey, endKey kv.Key, physicalTableID int64, element *meta.Element) error {
	return initDDLReorgHandle(r.s, job.ID, startKey, endKey, physicalTableID, element)
}

// RemoveReorgElementFailPoint removes the element of the reorganization information.
func (r *reorgHandler) RemoveReorgElementFailPoint(job *model.Job) error {
	return removeReorgElement(r.s, job)
}

// RemoveDDLReorgHandle removes the job reorganization related handles.
func (r *reorgHandler) RemoveDDLReorgHandle(job *model.Job, elements []*meta.Element) error {
	return removeDDLReorgHandle(r.s, job, elements)
}

// cleanupDDLReorgHandles removes the job reorganization related handles.
func cleanupDDLReorgHandles(job *model.Job, s *sess.Session) {
	if job != nil && !job.IsFinished() && !job.IsSynced() {
		// Job is given, but it is neither finished nor synced; do nothing
		return
	}

	err := cleanDDLReorgHandles(s, job)
	if err != nil {
		// ignore error, cleanup is not that critical
		logutil.DDLLogger().Warn("Failed removing the DDL reorg entry in tidb_ddl_reorg", zap.Stringer("job", job), zap.Error(err))
	}
}

// GetDDLReorgHandle gets the latest processed DDL reorganize position.
func (r *reorgHandler) GetDDLReorgHandle(job *model.Job) (element *meta.Element, startKey, endKey kv.Key, physicalTableID int64, err error) {
	element, startKey, endKey, physicalTableID, err = getDDLReorgHandle(r.s, job)
	if err != nil {
		return element, startKey, endKey, physicalTableID, err
	}
	adjustedEndKey := adjustEndKeyAcrossVersion(job, endKey)
	return element, startKey, adjustedEndKey, physicalTableID, nil
}

// #46306 changes the table range from [start_key, end_key] to [start_key, end_key.next).
// For old version TiDB, the semantic is still [start_key, end_key], we need to adjust it in new version TiDB.
func adjustEndKeyAcrossVersion(job *model.Job, endKey kv.Key) kv.Key {
	if job.ReorgMeta != nil && job.ReorgMeta.Version == model.ReorgMetaVersion0 {
		logutil.DDLLogger().Info("adjust range end key for old version ReorgMetas",
			zap.Int64("jobID", job.ID),
			zap.Int64("reorgMetaVersion", job.ReorgMeta.Version),
			zap.String("endKey", hex.EncodeToString(endKey)))
		return endKey.Next()
	}
	return endKey
}
