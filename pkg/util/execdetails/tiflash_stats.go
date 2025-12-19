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

package execdetails

import (
	"bytes"
	"fmt"
	"maps"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/tikv/client-go/v2/util"
)

// TiflashStats contains tiflash execution stats.
type TiflashStats struct {
	scanContext    TiFlashScanContext
	waitSummary    TiFlashWaitSummary
	networkSummary TiFlashNetworkTrafficSummary
}

// TiFlashScanContext is used to express the table scan information in tiflash
type TiFlashScanContext struct {
	dmfileDataScannedRows     uint64
	dmfileDataSkippedRows     uint64
	dmfileMvccScannedRows     uint64
	dmfileMvccSkippedRows     uint64
	dmfileLmFilterScannedRows uint64
	dmfileLmFilterSkippedRows uint64
	totalDmfileRsCheckMs      uint64
	totalDmfileReadMs         uint64
	totalBuildSnapshotMs      uint64
	localRegions              uint64
	remoteRegions             uint64
	totalLearnerReadMs        uint64
	disaggReadCacheHitBytes   uint64
	disaggReadCacheMissBytes  uint64
	segments                  uint64
	readTasks                 uint64
	deltaRows                 uint64
	deltaBytes                uint64
	mvccInputRows             uint64
	mvccInputBytes            uint64
	mvccOutputRows            uint64
	totalBuildBitmapMs        uint64
	totalBuildInputStreamMs   uint64
	staleReadRegions          uint64
	minLocalStreamMs          uint64
	maxLocalStreamMs          uint64
	minRemoteStreamMs         uint64
	maxRemoteStreamMs         uint64
	regionsOfInstance         map[string]uint64

	// vector index related

	vectorIdxLoadFromS3           uint64
	vectorIdxLoadFromDisk         uint64
	vectorIdxLoadFromCache        uint64
	vectorIdxLoadTimeMs           uint64
	vectorIdxSearchTimeMs         uint64
	vectorIdxSearchVisitedNodes   uint64
	vectorIdxSearchDiscardedNodes uint64
	vectorIdxReadVecTimeMs        uint64
	vectorIdxReadOthersTimeMs     uint64

	// fts related

	ftsNFromInmemoryNoindex     uint32
	ftsNFromTinyIndex           uint32
	ftsNFromTinyNoindex         uint32
	ftsNFromDmfIndex            uint32
	ftsNFromDmfNoindex          uint32
	ftsRowsFromInmemoryNoindex  uint64
	ftsRowsFromTinyIndex        uint64
	ftsRowsFromTinyNoindex      uint64
	ftsRowsFromDmfIndex         uint64
	ftsRowsFromDmfNoindex       uint64
	ftsIdxLoadTotalMs           uint64
	ftsIdxLoadFromCache         uint32
	ftsIdxLoadFromColumnFile    uint32
	ftsIdxLoadFromStableS3      uint32
	ftsIdxLoadFromStableDisk    uint32
	ftsIdxSearchN               uint32
	ftsIdxSearchTotalMs         uint64
	ftsIdxDmSearchRows          uint64
	ftsIdxDmTotalReadFtsMs      uint64
	ftsIdxDmTotalReadOthersMs   uint64
	ftsIdxTinySearchRows        uint64
	ftsIdxTinyTotalReadFtsMs    uint64
	ftsIdxTinyTotalReadOthersMs uint64
	ftsBruteTotalReadMs         uint64
	ftsBruteTotalSearchMs       uint64

	// inverted index related

	invertedIdxLoadFromS3         uint32
	invertedIdxLoadFromDisk       uint32
	invertedIdxLoadFromCache      uint32
	invertedIdxLoadTimeMs         uint64
	invertedIdxSearchTimeMs       uint64
	invertedIdxSearchSkippedPacks uint32
	invertedIdxIndexedRows        uint64
	invertedIdxSearchSelectedRows uint64
}

// Clone implements the deep copy of * TiFlashshScanContext
func (context *TiFlashScanContext) Clone() TiFlashScanContext {
	newContext := TiFlashScanContext{
		dmfileDataScannedRows:     context.dmfileDataScannedRows,
		dmfileDataSkippedRows:     context.dmfileDataSkippedRows,
		dmfileMvccScannedRows:     context.dmfileMvccScannedRows,
		dmfileMvccSkippedRows:     context.dmfileMvccSkippedRows,
		dmfileLmFilterScannedRows: context.dmfileLmFilterScannedRows,
		dmfileLmFilterSkippedRows: context.dmfileLmFilterSkippedRows,
		totalDmfileRsCheckMs:      context.totalDmfileRsCheckMs,
		totalDmfileReadMs:         context.totalDmfileReadMs,
		totalBuildSnapshotMs:      context.totalBuildSnapshotMs,
		localRegions:              context.localRegions,
		remoteRegions:             context.remoteRegions,
		totalLearnerReadMs:        context.totalLearnerReadMs,
		disaggReadCacheHitBytes:   context.disaggReadCacheHitBytes,
		disaggReadCacheMissBytes:  context.disaggReadCacheMissBytes,
		segments:                  context.segments,
		readTasks:                 context.readTasks,
		deltaRows:                 context.deltaRows,
		deltaBytes:                context.deltaBytes,
		mvccInputRows:             context.mvccInputRows,
		mvccInputBytes:            context.mvccInputBytes,
		mvccOutputRows:            context.mvccOutputRows,
		totalBuildBitmapMs:        context.totalBuildBitmapMs,
		totalBuildInputStreamMs:   context.totalBuildInputStreamMs,
		staleReadRegions:          context.staleReadRegions,
		minLocalStreamMs:          context.minLocalStreamMs,
		maxLocalStreamMs:          context.maxLocalStreamMs,
		minRemoteStreamMs:         context.minRemoteStreamMs,
		maxRemoteStreamMs:         context.maxRemoteStreamMs,
		regionsOfInstance:         make(map[string]uint64),

		vectorIdxLoadFromS3:           context.vectorIdxLoadFromS3,
		vectorIdxLoadFromDisk:         context.vectorIdxLoadFromDisk,
		vectorIdxLoadFromCache:        context.vectorIdxLoadFromCache,
		vectorIdxLoadTimeMs:           context.vectorIdxLoadTimeMs,
		vectorIdxSearchTimeMs:         context.vectorIdxSearchTimeMs,
		vectorIdxSearchVisitedNodes:   context.vectorIdxSearchVisitedNodes,
		vectorIdxSearchDiscardedNodes: context.vectorIdxSearchDiscardedNodes,
		vectorIdxReadVecTimeMs:        context.vectorIdxReadVecTimeMs,
		vectorIdxReadOthersTimeMs:     context.vectorIdxReadOthersTimeMs,

		ftsNFromInmemoryNoindex:     context.ftsNFromInmemoryNoindex,
		ftsNFromTinyIndex:           context.ftsNFromTinyIndex,
		ftsNFromTinyNoindex:         context.ftsNFromTinyNoindex,
		ftsNFromDmfIndex:            context.ftsNFromDmfIndex,
		ftsNFromDmfNoindex:          context.ftsNFromDmfNoindex,
		ftsRowsFromInmemoryNoindex:  context.ftsRowsFromInmemoryNoindex,
		ftsRowsFromTinyIndex:        context.ftsRowsFromTinyIndex,
		ftsRowsFromTinyNoindex:      context.ftsRowsFromTinyNoindex,
		ftsRowsFromDmfIndex:         context.ftsRowsFromDmfIndex,
		ftsRowsFromDmfNoindex:       context.ftsRowsFromDmfNoindex,
		ftsIdxLoadTotalMs:           context.ftsIdxLoadTotalMs,
		ftsIdxLoadFromCache:         context.ftsIdxLoadFromCache,
		ftsIdxLoadFromColumnFile:    context.ftsIdxLoadFromColumnFile,
		ftsIdxLoadFromStableS3:      context.ftsIdxLoadFromStableS3,
		ftsIdxLoadFromStableDisk:    context.ftsIdxLoadFromStableDisk,
		ftsIdxSearchN:               context.ftsIdxSearchN,
		ftsIdxSearchTotalMs:         context.ftsIdxSearchTotalMs,
		ftsIdxDmSearchRows:          context.ftsIdxDmSearchRows,
		ftsIdxDmTotalReadFtsMs:      context.ftsIdxDmTotalReadFtsMs,
		ftsIdxDmTotalReadOthersMs:   context.ftsIdxDmTotalReadOthersMs,
		ftsIdxTinySearchRows:        context.ftsIdxTinySearchRows,
		ftsIdxTinyTotalReadFtsMs:    context.ftsIdxTinyTotalReadFtsMs,
		ftsIdxTinyTotalReadOthersMs: context.ftsIdxTinyTotalReadOthersMs,
		ftsBruteTotalReadMs:         context.ftsBruteTotalReadMs,
		ftsBruteTotalSearchMs:       context.ftsBruteTotalSearchMs,

		invertedIdxLoadFromS3:         context.invertedIdxLoadFromS3,
		invertedIdxLoadFromDisk:       context.invertedIdxLoadFromDisk,
		invertedIdxLoadFromCache:      context.invertedIdxLoadFromCache,
		invertedIdxLoadTimeMs:         context.invertedIdxLoadTimeMs,
		invertedIdxSearchTimeMs:       context.invertedIdxSearchTimeMs,
		invertedIdxSearchSkippedPacks: context.invertedIdxSearchSkippedPacks,
		invertedIdxIndexedRows:        context.invertedIdxIndexedRows,
		invertedIdxSearchSelectedRows: context.invertedIdxSearchSelectedRows,
	}
	maps.Copy(newContext.regionsOfInstance, context.regionsOfInstance)
	return newContext
}

func (context *TiFlashScanContext) String() string {
	var output []string
	if context.vectorIdxLoadFromS3+context.vectorIdxLoadFromDisk+context.vectorIdxLoadFromCache > 0 {
		var items []string
		items = append(items, fmt.Sprintf("load:{total:%dms,from_s3:%d,from_disk:%d,from_cache:%d}", context.vectorIdxLoadTimeMs, context.vectorIdxLoadFromS3, context.vectorIdxLoadFromDisk, context.vectorIdxLoadFromCache))
		items = append(items, fmt.Sprintf("search:{total:%dms,visited_nodes:%d,discarded_nodes:%d}", context.vectorIdxSearchTimeMs, context.vectorIdxSearchVisitedNodes, context.vectorIdxSearchDiscardedNodes))
		items = append(items, fmt.Sprintf("read:{vec_total:%dms,others_total:%dms}", context.vectorIdxReadVecTimeMs, context.vectorIdxReadOthersTimeMs))
		output = append(output, "vector_idx:{"+strings.Join(items, ",")+"}")
	}
	if context.invertedIdxLoadFromS3+context.invertedIdxLoadFromDisk+context.invertedIdxLoadFromCache > 0 {
		var items []string
		items = append(items, fmt.Sprintf("load:{total:%dms,from_s3:%d,from_disk:%d,from_cache:%d}", context.invertedIdxLoadTimeMs, context.invertedIdxLoadFromS3, context.invertedIdxLoadFromDisk, context.invertedIdxLoadFromCache))
		items = append(items, fmt.Sprintf("search:{total:%dms,skipped_packs:%d,indexed_rows:%d,selected_rows:%d}", context.invertedIdxSearchTimeMs, context.invertedIdxSearchSkippedPacks, context.invertedIdxIndexedRows, context.invertedIdxSearchSelectedRows))
		output = append(output, "inverted_idx:{"+strings.Join(items, ",")+"}")
	}
	if context.ftsNFromInmemoryNoindex+context.ftsNFromTinyIndex+context.ftsNFromTinyNoindex+context.ftsNFromDmfIndex+context.ftsNFromDmfNoindex > 0 {
		var items []string
		items = append(items, fmt.Sprintf("hit_rows:{delta:%d,dmf:%d}", context.ftsRowsFromTinyIndex, context.ftsRowsFromDmfIndex))
		items = append(items, fmt.Sprintf("miss_rows:{mem:%d,delta:%d,dmf:%d}", context.ftsRowsFromInmemoryNoindex, context.ftsRowsFromTinyNoindex, context.ftsRowsFromDmfNoindex))
		items = append(items, fmt.Sprintf("idx_load:{total:%dms,from:{s3:%d,disk:%d,cache:%d}}", context.ftsIdxLoadTotalMs, context.ftsIdxLoadFromStableS3, context.ftsIdxLoadFromStableDisk+context.ftsIdxLoadFromColumnFile, context.ftsIdxLoadFromCache))
		avg := uint64(0)
		if context.ftsIdxSearchN > 0 {
			avg = context.ftsIdxSearchTotalMs / uint64(context.ftsIdxSearchN)
		}
		items = append(items, fmt.Sprintf("idx_search:{total:%dms,avg:%dms}", context.ftsIdxSearchTotalMs, avg))
		items = append(items, fmt.Sprintf("idx_read:{rows:%d,fts_total:%dms,others_total:%dms}", context.ftsIdxDmSearchRows+context.ftsIdxTinySearchRows, context.ftsIdxDmTotalReadFtsMs+context.ftsIdxTinyTotalReadFtsMs, context.ftsIdxDmTotalReadOthersMs+context.ftsIdxTinyTotalReadOthersMs))
		items = append(items, fmt.Sprintf("miss:{read:%dms,search:%dms}", context.ftsBruteTotalReadMs, context.ftsBruteTotalSearchMs))
		output = append(output, "fts:{"+strings.Join(items, ",")+"}")
	}

	regionBalanceInfo := "none"
	if len(context.regionsOfInstance) > 0 {
		maxNum := uint64(0)
		minNum := uint64(math.MaxUint64)
		for _, v := range context.regionsOfInstance {
			if v > maxNum {
				maxNum = v
			}
			if v > 0 && v < minNum {
				minNum = v
			}
		}
		regionBalanceInfo = fmt.Sprintf("{instance_num: %d, max/min: %d/%d=%f}",
			len(context.regionsOfInstance),
			maxNum,
			minNum,
			float64(maxNum)/float64(minNum))
	}
	dmfileDisaggInfo := ""
	if context.disaggReadCacheHitBytes != 0 || context.disaggReadCacheMissBytes != 0 {
		dmfileDisaggInfo = fmt.Sprintf(", disagg_cache_hit_bytes: %d, disagg_cache_miss_bytes: %d",
			context.disaggReadCacheHitBytes,
			context.disaggReadCacheMissBytes)
	}
	remoteStreamInfo := ""
	if context.minRemoteStreamMs != 0 || context.maxRemoteStreamMs != 0 {
		remoteStreamInfo = fmt.Sprintf("min_remote_stream:%dms, max_remote_stream:%dms, ", context.minRemoteStreamMs, context.maxRemoteStreamMs)
	}

	// note: "tot" is short for "total"
	output = append(output, fmt.Sprintf("tiflash_scan:{"+
		"mvcc_input_rows:%d, "+
		"mvcc_input_bytes:%d, "+
		"mvcc_output_rows:%d, "+
		"local_regions:%d, "+
		"remote_regions:%d, "+
		"tot_learner_read:%dms, "+
		"region_balance:%s, "+
		"delta_rows:%d, "+
		"delta_bytes:%d, "+
		"segments:%d, "+
		"stale_read_regions:%d, "+
		"tot_build_snapshot:%dms, "+
		"tot_build_bitmap:%dms, "+
		"tot_build_inputstream:%dms, "+
		"min_local_stream:%dms, "+
		"max_local_stream:%dms, "+
		"%s"+ // remote stream info
		"dtfile:{"+
		"data_scanned_rows:%d, "+
		"data_skipped_rows:%d, "+
		"mvcc_scanned_rows:%d, "+
		"mvcc_skipped_rows:%d, "+
		"lm_filter_scanned_rows:%d, "+
		"lm_filter_skipped_rows:%d, "+
		"tot_rs_index_check:%dms, "+
		"tot_read:%dms"+
		"%s}"+ // Disagg cache info of DMFile
		"}",
		context.mvccInputRows,
		context.mvccInputBytes,
		context.mvccOutputRows,
		context.localRegions,
		context.remoteRegions,
		context.totalLearnerReadMs,
		regionBalanceInfo,
		context.deltaRows,
		context.deltaBytes,
		context.segments,
		context.staleReadRegions,
		context.totalBuildSnapshotMs,
		context.totalBuildBitmapMs,
		context.totalBuildInputStreamMs,
		context.minLocalStreamMs,
		context.maxLocalStreamMs,
		remoteStreamInfo,
		context.dmfileDataScannedRows,
		context.dmfileDataSkippedRows,
		context.dmfileMvccScannedRows,
		context.dmfileMvccSkippedRows,
		context.dmfileLmFilterScannedRows,
		context.dmfileLmFilterSkippedRows,
		context.totalDmfileRsCheckMs,
		context.totalDmfileReadMs,
		dmfileDisaggInfo,
	))

	return strings.Join(output, ", ")
}

// Merge make sum to merge the information in TiFlashScanContext
func (context *TiFlashScanContext) Merge(other TiFlashScanContext) {
	context.dmfileDataScannedRows += other.dmfileDataScannedRows
	context.dmfileDataSkippedRows += other.dmfileDataSkippedRows
	context.dmfileMvccScannedRows += other.dmfileMvccScannedRows
	context.dmfileMvccSkippedRows += other.dmfileMvccSkippedRows
	context.dmfileLmFilterScannedRows += other.dmfileLmFilterScannedRows
	context.dmfileLmFilterSkippedRows += other.dmfileLmFilterSkippedRows
	context.totalDmfileRsCheckMs += other.totalDmfileRsCheckMs
	context.totalDmfileReadMs += other.totalDmfileReadMs
	context.totalBuildSnapshotMs += other.totalBuildSnapshotMs
	context.localRegions += other.localRegions
	context.remoteRegions += other.remoteRegions
	context.totalLearnerReadMs += other.totalLearnerReadMs
	context.disaggReadCacheHitBytes += other.disaggReadCacheHitBytes
	context.disaggReadCacheMissBytes += other.disaggReadCacheMissBytes
	context.segments += other.segments
	context.readTasks += other.readTasks
	context.deltaRows += other.deltaRows
	context.deltaBytes += other.deltaBytes
	context.mvccInputRows += other.mvccInputRows
	context.mvccInputBytes += other.mvccInputBytes
	context.mvccOutputRows += other.mvccOutputRows
	context.totalBuildBitmapMs += other.totalBuildBitmapMs
	context.totalBuildInputStreamMs += other.totalBuildInputStreamMs
	context.staleReadRegions += other.staleReadRegions

	context.vectorIdxLoadFromS3 += other.vectorIdxLoadFromS3
	context.vectorIdxLoadFromDisk += other.vectorIdxLoadFromDisk
	context.vectorIdxLoadFromCache += other.vectorIdxLoadFromCache
	context.vectorIdxLoadTimeMs += other.vectorIdxLoadTimeMs
	context.vectorIdxSearchTimeMs += other.vectorIdxSearchTimeMs
	context.vectorIdxSearchVisitedNodes += other.vectorIdxSearchVisitedNodes
	context.vectorIdxSearchDiscardedNodes += other.vectorIdxSearchDiscardedNodes
	context.vectorIdxReadVecTimeMs += other.vectorIdxReadVecTimeMs
	context.vectorIdxReadOthersTimeMs += other.vectorIdxReadOthersTimeMs

	context.ftsNFromInmemoryNoindex += other.ftsNFromInmemoryNoindex
	context.ftsNFromTinyIndex += other.ftsNFromTinyIndex
	context.ftsNFromTinyNoindex += other.ftsNFromTinyNoindex
	context.ftsNFromDmfIndex += other.ftsNFromDmfIndex
	context.ftsNFromDmfNoindex += other.ftsNFromDmfNoindex
	context.ftsRowsFromInmemoryNoindex += other.ftsRowsFromInmemoryNoindex
	context.ftsRowsFromTinyIndex += other.ftsRowsFromTinyIndex
	context.ftsRowsFromTinyNoindex += other.ftsRowsFromTinyNoindex
	context.ftsRowsFromDmfIndex += other.ftsRowsFromDmfIndex
	context.ftsRowsFromDmfNoindex += other.ftsRowsFromDmfNoindex
	context.ftsIdxLoadTotalMs += other.ftsIdxLoadTotalMs
	context.ftsIdxLoadFromCache += other.ftsIdxLoadFromCache
	context.ftsIdxLoadFromColumnFile += other.ftsIdxLoadFromColumnFile
	context.ftsIdxLoadFromStableS3 += other.ftsIdxLoadFromStableS3
	context.ftsIdxLoadFromStableDisk += other.ftsIdxLoadFromStableDisk
	context.ftsIdxSearchN += other.ftsIdxSearchN
	context.ftsIdxSearchTotalMs += other.ftsIdxSearchTotalMs
	context.ftsIdxDmSearchRows += other.ftsIdxDmSearchRows
	context.ftsIdxDmTotalReadFtsMs += other.ftsIdxDmTotalReadFtsMs
	context.ftsIdxDmTotalReadOthersMs += other.ftsIdxDmTotalReadOthersMs
	context.ftsIdxTinySearchRows += other.ftsIdxTinySearchRows
	context.ftsIdxTinyTotalReadFtsMs += other.ftsIdxTinyTotalReadFtsMs
	context.ftsIdxTinyTotalReadOthersMs += other.ftsIdxTinyTotalReadOthersMs
	context.ftsBruteTotalReadMs += other.ftsBruteTotalReadMs
	context.ftsBruteTotalSearchMs += other.ftsBruteTotalSearchMs

	context.invertedIdxLoadFromS3 += other.invertedIdxLoadFromS3
	context.invertedIdxLoadFromDisk += other.invertedIdxLoadFromDisk
	context.invertedIdxLoadFromCache += other.invertedIdxLoadFromCache
	context.invertedIdxLoadTimeMs += other.invertedIdxLoadTimeMs
	context.invertedIdxSearchTimeMs += other.invertedIdxSearchTimeMs
	context.invertedIdxSearchSkippedPacks += other.invertedIdxSearchSkippedPacks
	context.invertedIdxIndexedRows += other.invertedIdxIndexedRows
	context.invertedIdxSearchSelectedRows += other.invertedIdxSearchSelectedRows

	if context.minLocalStreamMs == 0 || other.minLocalStreamMs < context.minLocalStreamMs {
		context.minLocalStreamMs = other.minLocalStreamMs
	}
	if other.maxLocalStreamMs > context.maxLocalStreamMs {
		context.maxLocalStreamMs = other.maxLocalStreamMs
	}
	if context.minRemoteStreamMs == 0 || other.minRemoteStreamMs < context.minRemoteStreamMs {
		context.minRemoteStreamMs = other.minRemoteStreamMs
	}
	if other.maxRemoteStreamMs > context.maxRemoteStreamMs {
		context.maxRemoteStreamMs = other.maxRemoteStreamMs
	}

	if context.regionsOfInstance == nil {
		context.regionsOfInstance = make(map[string]uint64)
	}
	for k, v := range other.regionsOfInstance {
		context.regionsOfInstance[k] += v
	}
}

func (context *TiFlashScanContext) mergeExecSummary(summary *tipb.TiFlashScanContext) {
	if summary == nil {
		return
	}
	context.dmfileDataScannedRows += summary.GetDmfileDataScannedRows()
	context.dmfileDataSkippedRows += summary.GetDmfileDataSkippedRows()
	context.dmfileMvccScannedRows += summary.GetDmfileMvccScannedRows()
	context.dmfileMvccSkippedRows += summary.GetDmfileMvccSkippedRows()
	context.dmfileLmFilterScannedRows += summary.GetDmfileLmFilterScannedRows()
	context.dmfileLmFilterSkippedRows += summary.GetDmfileLmFilterSkippedRows()
	context.totalDmfileRsCheckMs += summary.GetTotalDmfileRsCheckMs()
	context.totalDmfileReadMs += summary.GetTotalDmfileReadMs()
	context.totalBuildSnapshotMs += summary.GetTotalBuildSnapshotMs()
	context.localRegions += summary.GetLocalRegions()
	context.remoteRegions += summary.GetRemoteRegions()
	context.totalLearnerReadMs += summary.GetTotalLearnerReadMs()
	context.disaggReadCacheHitBytes += summary.GetDisaggReadCacheHitBytes()
	context.disaggReadCacheMissBytes += summary.GetDisaggReadCacheMissBytes()
	context.segments += summary.GetSegments()
	context.readTasks += summary.GetReadTasks()
	context.deltaRows += summary.GetDeltaRows()
	context.deltaBytes += summary.GetDeltaBytes()
	context.mvccInputRows += summary.GetMvccInputRows()
	context.mvccInputBytes += summary.GetMvccInputBytes()
	context.mvccOutputRows += summary.GetMvccOutputRows()
	context.totalBuildBitmapMs += summary.GetTotalBuildBitmapMs()
	context.totalBuildInputStreamMs += summary.GetTotalBuildInputstreamMs()
	context.staleReadRegions += summary.GetStaleReadRegions()

	context.vectorIdxLoadFromS3 += summary.GetVectorIdxLoadFromS3()
	context.vectorIdxLoadFromDisk += summary.GetVectorIdxLoadFromDisk()
	context.vectorIdxLoadFromCache += summary.GetVectorIdxLoadFromCache()
	context.vectorIdxLoadTimeMs += summary.GetVectorIdxLoadTimeMs()
	context.vectorIdxSearchTimeMs += summary.GetVectorIdxSearchTimeMs()
	context.vectorIdxSearchVisitedNodes += summary.GetVectorIdxSearchVisitedNodes()
	context.vectorIdxSearchDiscardedNodes += summary.GetVectorIdxSearchDiscardedNodes()
	context.vectorIdxReadVecTimeMs += summary.GetVectorIdxReadVecTimeMs()
	context.vectorIdxReadOthersTimeMs += summary.GetVectorIdxReadOthersTimeMs()

	context.ftsNFromInmemoryNoindex += summary.GetFtsNFromInmemoryNoindex()
	context.ftsNFromTinyIndex += summary.GetFtsNFromTinyIndex()
	context.ftsNFromTinyNoindex += summary.GetFtsNFromTinyNoindex()
	context.ftsNFromDmfIndex += summary.GetFtsNFromDmfIndex()
	context.ftsNFromDmfNoindex += summary.GetFtsNFromDmfNoindex()
	context.ftsRowsFromInmemoryNoindex += summary.GetFtsRowsFromInmemoryNoindex()
	context.ftsRowsFromTinyIndex += summary.GetFtsRowsFromTinyIndex()
	context.ftsRowsFromTinyNoindex += summary.GetFtsRowsFromTinyNoindex()
	context.ftsRowsFromDmfIndex += summary.GetFtsRowsFromDmfIndex()
	context.ftsRowsFromDmfNoindex += summary.GetFtsRowsFromDmfNoindex()
	context.ftsIdxLoadTotalMs += summary.GetFtsIdxLoadTotalMs()
	context.ftsIdxLoadFromCache += summary.GetFtsIdxLoadFromCache()
	context.ftsIdxLoadFromColumnFile += summary.GetFtsIdxLoadFromColumnFile()
	context.ftsIdxLoadFromStableS3 += summary.GetFtsIdxLoadFromStableS3()
	context.ftsIdxLoadFromStableDisk += summary.GetFtsIdxLoadFromStableDisk()
	context.ftsIdxSearchN += summary.GetFtsIdxSearchN()
	context.ftsIdxSearchTotalMs += summary.GetFtsIdxSearchTotalMs()
	context.ftsIdxDmSearchRows += summary.GetFtsIdxDmSearchRows()
	context.ftsIdxDmTotalReadFtsMs += summary.GetFtsIdxDmTotalReadFtsMs()
	context.ftsIdxDmTotalReadOthersMs += summary.GetFtsIdxDmTotalReadOthersMs()
	context.ftsIdxTinySearchRows += summary.GetFtsIdxTinySearchRows()
	context.ftsIdxTinyTotalReadFtsMs += summary.GetFtsIdxTinyTotalReadFtsMs()
	context.ftsIdxTinyTotalReadOthersMs += summary.GetFtsIdxTinyTotalReadOthersMs()
	context.ftsBruteTotalReadMs += summary.GetFtsBruteTotalReadMs()
	context.ftsBruteTotalSearchMs += summary.GetFtsBruteTotalSearchMs()

	context.invertedIdxLoadFromS3 += summary.GetInvertedIdxLoadFromS3()
	context.invertedIdxLoadFromDisk += summary.GetInvertedIdxLoadFromDisk()
	context.invertedIdxLoadFromCache += summary.GetInvertedIdxLoadFromCache()
	context.invertedIdxLoadTimeMs += summary.GetInvertedIdxLoadTimeMs()
	context.invertedIdxSearchTimeMs += summary.GetInvertedIdxSearchTimeMs()
	context.invertedIdxSearchSkippedPacks += summary.GetInvertedIdxSearchSkippedPacks()
	context.invertedIdxIndexedRows += summary.GetInvertedIdxIndexedRows()
	context.invertedIdxSearchSelectedRows += summary.GetInvertedIdxSearchSelectedRows()

	if context.minLocalStreamMs == 0 || summary.GetMinLocalStreamMs() < context.minLocalStreamMs {
		context.minLocalStreamMs = summary.GetMinLocalStreamMs()
	}
	if summary.GetMaxLocalStreamMs() > context.maxLocalStreamMs {
		context.maxLocalStreamMs = summary.GetMaxLocalStreamMs()
	}
	if context.minRemoteStreamMs == 0 || summary.GetMinRemoteStreamMs() < context.minRemoteStreamMs {
		context.minRemoteStreamMs = summary.GetMinRemoteStreamMs()
	}
	if summary.GetMaxRemoteStreamMs() > context.maxRemoteStreamMs {
		context.maxRemoteStreamMs = summary.GetMaxRemoteStreamMs()
	}

	if context.regionsOfInstance == nil {
		context.regionsOfInstance = make(map[string]uint64, len(summary.GetRegionsOfInstance()))
	}
	for _, instance := range summary.GetRegionsOfInstance() {
		context.regionsOfInstance[instance.GetInstanceId()] += instance.GetRegionNum()
	}
}

// Empty check whether TiFlashScanContext is Empty, if scan no pack and skip no pack, we regard it as empty
func (context *TiFlashScanContext) Empty() bool {
	res := context.dmfileDataScannedRows == 0 &&
		context.dmfileDataSkippedRows == 0 &&
		context.dmfileMvccScannedRows == 0 &&
		context.dmfileMvccSkippedRows == 0 &&
		context.dmfileLmFilterScannedRows == 0 &&
		context.dmfileLmFilterSkippedRows == 0 &&
		context.localRegions == 0 &&
		context.remoteRegions == 0 &&
		context.vectorIdxLoadFromDisk == 0 &&
		context.vectorIdxLoadFromCache == 0 &&
		context.vectorIdxLoadFromS3 == 0 &&
		context.invertedIdxLoadFromDisk == 0 &&
		context.invertedIdxLoadFromCache == 0 &&
		context.invertedIdxLoadFromS3 == 0 &&
		context.ftsNFromInmemoryNoindex == 0 &&
		context.ftsNFromTinyIndex == 0 &&
		context.ftsNFromTinyNoindex == 0 &&
		context.ftsNFromDmfIndex == 0 &&
		context.ftsNFromDmfNoindex == 0
	return res
}

// TiFlashWaitSummary is used to express all kinds of wait information in tiflash
type TiFlashWaitSummary struct {
	// keep execution time to do merge work, always record the wait time with largest execution time
	executionTime           uint64
	minTSOWaitTime          uint64
	pipelineBreakerWaitTime uint64
	pipelineQueueWaitTime   uint64
}

// Clone implements the deep copy of * TiFlashWaitSummary
func (waitSummary *TiFlashWaitSummary) Clone() TiFlashWaitSummary {
	newSummary := TiFlashWaitSummary{
		executionTime:           waitSummary.executionTime,
		minTSOWaitTime:          waitSummary.minTSOWaitTime,
		pipelineBreakerWaitTime: waitSummary.pipelineBreakerWaitTime,
		pipelineQueueWaitTime:   waitSummary.pipelineQueueWaitTime,
	}
	return newSummary
}

// String dumps TiFlashWaitSummary info as string
func (waitSummary *TiFlashWaitSummary) String() string {
	if waitSummary.CanBeIgnored() {
		return ""
	}
	buf := bytes.NewBuffer(make([]byte, 0, 32))
	buf.WriteString("tiflash_wait: {")
	empty := true
	if waitSummary.minTSOWaitTime >= uint64(time.Millisecond) {
		buf.WriteString("minTSO_wait: ")
		buf.WriteString(strconv.FormatInt(time.Duration(waitSummary.minTSOWaitTime).Milliseconds(), 10))
		buf.WriteString("ms")
		empty = false
	}
	if waitSummary.pipelineBreakerWaitTime >= uint64(time.Millisecond) {
		if !empty {
			buf.WriteString(", ")
		}
		buf.WriteString("pipeline_breaker_wait: ")
		buf.WriteString(strconv.FormatInt(time.Duration(waitSummary.pipelineBreakerWaitTime).Milliseconds(), 10))
		buf.WriteString("ms")
		empty = false
	}
	if waitSummary.pipelineQueueWaitTime >= uint64(time.Millisecond) {
		if !empty {
			buf.WriteString(", ")
		}
		buf.WriteString("pipeline_queue_wait: ")
		buf.WriteString(strconv.FormatInt(time.Duration(waitSummary.pipelineQueueWaitTime).Milliseconds(), 10))
		buf.WriteString("ms")
	}
	buf.WriteString("}")
	return buf.String()
}

// Merge make sum to merge the information in TiFlashWaitSummary
func (waitSummary *TiFlashWaitSummary) Merge(other TiFlashWaitSummary) {
	if waitSummary.executionTime < other.executionTime {
		waitSummary.executionTime = other.executionTime
		waitSummary.minTSOWaitTime = other.minTSOWaitTime
		waitSummary.pipelineBreakerWaitTime = other.pipelineBreakerWaitTime
		waitSummary.pipelineQueueWaitTime = other.pipelineQueueWaitTime
	}
}

func (waitSummary *TiFlashWaitSummary) mergeExecSummary(summary *tipb.TiFlashWaitSummary, executionTime uint64) {
	if summary == nil {
		return
	}
	if waitSummary.executionTime < executionTime {
		waitSummary.executionTime = executionTime
		waitSummary.minTSOWaitTime = summary.GetMinTSOWaitNs()
		waitSummary.pipelineBreakerWaitTime = summary.GetPipelineBreakerWaitNs()
		waitSummary.pipelineQueueWaitTime = summary.GetPipelineQueueWaitNs()
	}
}

// CanBeIgnored check whether TiFlashWaitSummary can be ignored, not all tidb executors have significant tiflash wait summary
func (waitSummary *TiFlashWaitSummary) CanBeIgnored() bool {
	res := waitSummary.minTSOWaitTime < uint64(time.Millisecond) &&
		waitSummary.pipelineBreakerWaitTime < uint64(time.Millisecond) &&
		waitSummary.pipelineQueueWaitTime < uint64(time.Millisecond)
	return res
}

// TiFlashNetworkTrafficSummary is used to express network traffic in tiflash
type TiFlashNetworkTrafficSummary struct {
	innerZoneSendBytes    uint64
	interZoneSendBytes    uint64
	innerZoneReceiveBytes uint64
	interZoneReceiveBytes uint64
}

// UpdateTiKVExecDetails update tikvDetails with TiFlashNetworkTrafficSummary's values
func (networkTraffic *TiFlashNetworkTrafficSummary) UpdateTiKVExecDetails(tikvDetails *util.ExecDetails) {
	if tikvDetails == nil {
		return
	}
	atomic.AddInt64(&tikvDetails.UnpackedBytesSentMPPCrossZone, int64(networkTraffic.interZoneSendBytes))
	atomic.AddInt64(&tikvDetails.UnpackedBytesSentMPPTotal, int64(networkTraffic.interZoneSendBytes))
	atomic.AddInt64(&tikvDetails.UnpackedBytesSentMPPTotal, int64(networkTraffic.innerZoneSendBytes))

	atomic.AddInt64(&tikvDetails.UnpackedBytesReceivedMPPCrossZone, int64(networkTraffic.interZoneReceiveBytes))
	atomic.AddInt64(&tikvDetails.UnpackedBytesReceivedMPPTotal, int64(networkTraffic.interZoneReceiveBytes))
	atomic.AddInt64(&tikvDetails.UnpackedBytesReceivedMPPTotal, int64(networkTraffic.innerZoneReceiveBytes))
}

// Clone implements the deep copy of * TiFlashNetworkTrafficSummary
func (networkTraffic *TiFlashNetworkTrafficSummary) Clone() TiFlashNetworkTrafficSummary {
	newSummary := TiFlashNetworkTrafficSummary{
		innerZoneSendBytes:    networkTraffic.innerZoneSendBytes,
		interZoneSendBytes:    networkTraffic.interZoneSendBytes,
		innerZoneReceiveBytes: networkTraffic.innerZoneReceiveBytes,
		interZoneReceiveBytes: networkTraffic.interZoneReceiveBytes,
	}
	return newSummary
}

// Empty check whether TiFlashNetworkTrafficSummary is Empty, if no any network traffic, we regard it as empty
func (networkTraffic *TiFlashNetworkTrafficSummary) Empty() bool {
	res := networkTraffic.innerZoneSendBytes == 0 &&
		networkTraffic.interZoneSendBytes == 0 &&
		networkTraffic.innerZoneReceiveBytes == 0 &&
		networkTraffic.interZoneReceiveBytes == 0
	return res
}

// String dumps TiFlashNetworkTrafficSummary info as string
func (networkTraffic *TiFlashNetworkTrafficSummary) String() string {
	buf := bytes.NewBuffer(make([]byte, 0, 32))
	buf.WriteString("tiflash_network: {")
	empty := true
	if networkTraffic.innerZoneSendBytes != 0 {
		buf.WriteString("inner_zone_send_bytes: ")
		buf.WriteString(strconv.FormatInt(int64(networkTraffic.innerZoneSendBytes), 10))
		empty = false
	}
	if networkTraffic.interZoneSendBytes != 0 {
		if !empty {
			buf.WriteString(", ")
		}
		buf.WriteString("inter_zone_send_bytes: ")
		buf.WriteString(strconv.FormatInt(int64(networkTraffic.interZoneSendBytes), 10))
		empty = false
	}
	if networkTraffic.innerZoneReceiveBytes != 0 {
		if !empty {
			buf.WriteString(", ")
		}
		buf.WriteString("inner_zone_receive_bytes: ")
		buf.WriteString(strconv.FormatInt(int64(networkTraffic.innerZoneReceiveBytes), 10))
		empty = false
	}
	if networkTraffic.interZoneReceiveBytes != 0 {
		if !empty {
			buf.WriteString(", ")
		}
		buf.WriteString("inter_zone_receive_bytes: ")
		buf.WriteString(strconv.FormatInt(int64(networkTraffic.interZoneReceiveBytes), 10))
	}
	buf.WriteString("}")
	return buf.String()
}

// Merge make sum to merge the information in TiFlashNetworkTrafficSummary
func (networkTraffic *TiFlashNetworkTrafficSummary) Merge(other TiFlashNetworkTrafficSummary) {
	networkTraffic.innerZoneSendBytes += other.innerZoneSendBytes
	networkTraffic.interZoneSendBytes += other.interZoneSendBytes
	networkTraffic.innerZoneReceiveBytes += other.innerZoneReceiveBytes
	networkTraffic.interZoneReceiveBytes += other.interZoneReceiveBytes
}

func (networkTraffic *TiFlashNetworkTrafficSummary) mergeExecSummary(summary *tipb.TiFlashNetWorkSummary) {
	if summary == nil {
		return
	}
	networkTraffic.innerZoneSendBytes += *summary.InnerZoneSendBytes
	networkTraffic.interZoneSendBytes += *summary.InterZoneSendBytes
	networkTraffic.innerZoneReceiveBytes += *summary.InnerZoneReceiveBytes
	networkTraffic.interZoneReceiveBytes += *summary.InterZoneReceiveBytes
}

// GetInterZoneTrafficBytes returns the inter zone network traffic bytes involved
// between tiflash instances.
func (networkTraffic *TiFlashNetworkTrafficSummary) GetInterZoneTrafficBytes() uint64 {
	// NOTE: we only count the inter zone sent bytes here because tiflash count the traffic bytes
	// of all sub request. For each sub request, both side with count the send and recv traffic.
	// So here, we only use the send bytes as the overall traffic to avoid count the traffic twice.
	// While this statistics logic seems a bit weird to me, but this is the tiflash side desicion.
	return networkTraffic.interZoneSendBytes
}

// MergeTiFlashRUConsumption merge execution summaries from selectResponse into ruDetails.
func MergeTiFlashRUConsumption(executionSummaries []*tipb.ExecutorExecutionSummary, ruDetails *util.RUDetails) error {
	newRUDetails := util.NewRUDetails()
	for _, summary := range executionSummaries {
		if summary != nil && summary.GetRuConsumption() != nil {
			tiflashRU := new(resource_manager.Consumption)
			if err := tiflashRU.Unmarshal(summary.GetRuConsumption()); err != nil {
				return err
			}
			newRUDetails.Update(tiflashRU, 0)
		}
	}
	ruDetails.Merge(newRUDetails)
	return nil
}
