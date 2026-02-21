// Copyright 2019 PingCAP, Inc.
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

package executor

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/diagnosticspb"
	"github.com/pingcap/sysutil"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	pd "github.com/tikv/pd/client/http"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type clusterLogRetriever struct {
	isDrained  bool
	retrieving bool
	heap       *logResponseHeap
	extractor  *plannercore.ClusterLogTableExtractor
	cancel     context.CancelFunc
}

type logStreamResult struct {
	// Read the next stream result while current messages is drained
	next chan logStreamResult

	addr     string
	typ      string
	messages []*diagnosticspb.LogMessage
	err      error
}

type logResponseHeap []logStreamResult

func (h logResponseHeap) Len() int {
	return len(h)
}

func (h logResponseHeap) Less(i, j int) bool {
	if lhs, rhs := h[i].messages[0].Time, h[j].messages[0].Time; lhs != rhs {
		return lhs < rhs
	}
	return h[i].typ < h[j].typ
}

func (h logResponseHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *logResponseHeap) Push(x any) {
	*h = append(*h, x.(logStreamResult))
}

func (h *logResponseHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (e *clusterLogRetriever) initialize(ctx context.Context, sctx sessionctx.Context) ([]chan logStreamResult, error) {
	if !hasPriv(sctx, mysql.ProcessPriv) {
		return nil, plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("PROCESS")
	}
	serversInfo, err := infoschema.GetClusterServerInfo(sctx)
	failpoint.Inject("mockClusterLogServerInfo", func(val failpoint.Value) {
		// erase the error
		err = nil
		if s := val.(string); len(s) > 0 {
			serversInfo = parseFailpointServerInfo(s)
		}
	})
	if err != nil {
		return nil, err
	}

	instances := e.extractor.Instances
	nodeTypes := e.extractor.NodeTypes
	serversInfo = infoschema.FilterClusterServerInfo(serversInfo, nodeTypes, instances)

	var levels = make([]diagnosticspb.LogLevel, 0, len(e.extractor.LogLevels))
	for l := range e.extractor.LogLevels {
		levels = append(levels, sysutil.ParseLogLevel(l))
	}

	// To avoid search log interface overload, the user should specify the time range, and at least one pattern
	// in normally SQL.
	if e.extractor.StartTime == 0 {
		return nil, errors.New("denied to scan logs, please specified the start time, such as `time > '2020-01-01 00:00:00'`")
	}
	if e.extractor.EndTime == 0 {
		return nil, errors.New("denied to scan logs, please specified the end time, such as `time < '2020-01-01 00:00:00'`")
	}
	patterns := e.extractor.Patterns
	if len(patterns) == 0 && len(levels) == 0 && len(instances) == 0 && len(nodeTypes) == 0 {
		return nil, errors.New("denied to scan full logs (use `SELECT * FROM cluster_log WHERE message LIKE '%'` explicitly if intentionally)")
	}

	req := &diagnosticspb.SearchLogRequest{
		StartTime: e.extractor.StartTime,
		EndTime:   e.extractor.EndTime,
		Levels:    levels,
		Patterns:  patterns,
	}

	return e.startRetrieving(ctx, sctx, serversInfo, req)
}

func (e *clusterLogRetriever) startRetrieving(
	ctx context.Context,
	sctx sessionctx.Context,
	serversInfo []infoschema.ServerInfo,
	req *diagnosticspb.SearchLogRequest) ([]chan logStreamResult, error) {
	// gRPC options
	opt := grpc.WithTransportCredentials(insecure.NewCredentials())
	security := config.GetGlobalConfig().Security
	if len(security.ClusterSSLCA) != 0 {
		clusterSecurity := security.ClusterSecurity()
		tlsConfig, err := clusterSecurity.ToTLSConfig()
		if err != nil {
			return nil, errors.Trace(err)
		}
		opt = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	}

	// The retrieve progress may be abort
	ctx, e.cancel = context.WithCancel(ctx)

	var results []chan logStreamResult //nolint: prealloc
	for _, srv := range serversInfo {
		typ := srv.ServerType
		address := srv.Address
		statusAddr := srv.StatusAddr
		if len(statusAddr) == 0 {
			sctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("%s node %s does not contain status address", typ, address))
			continue
		}
		ch := make(chan logStreamResult)
		results = append(results, ch)

		go func(ch chan logStreamResult, serverType, address, statusAddr string) {
			util.WithRecovery(func() {
				defer close(ch)

				// TiDB and TiProxy provide diagnostics service via status address
				remote := address
				if serverType == "tidb" || serverType == "tiproxy" {
					remote = statusAddr
				}
				conn, err := grpc.Dial(remote, opt)
				if err != nil {
					ch <- logStreamResult{addr: address, typ: serverType, err: err}
					return
				}
				defer terror.Call(conn.Close)

				cli := diagnosticspb.NewDiagnosticsClient(conn)
				stream, err := cli.SearchLog(ctx, req)
				if err != nil {
					ch <- logStreamResult{addr: address, typ: serverType, err: err}
					return
				}

				for {
					res, err := stream.Recv()
					if err != nil && err == io.EOF {
						return
					}
					if err != nil {
						select {
						case ch <- logStreamResult{addr: address, typ: serverType, err: err}:
						case <-ctx.Done():
						}
						return
					}

					result := logStreamResult{next: ch, addr: address, typ: serverType, messages: res.Messages}
					select {
					case ch <- result:
					case <-ctx.Done():
						return
					}
				}
			}, nil)
		}(ch, typ, address, statusAddr)
	}

	return results, nil
}

func (e *clusterLogRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if e.extractor.SkipRequest || e.isDrained {
		return nil, nil
	}

	if !e.retrieving {
		e.retrieving = true
		results, err := e.initialize(ctx, sctx)
		if err != nil {
			e.isDrained = true
			return nil, err
		}

		// initialize the heap
		e.heap = &logResponseHeap{}
		for _, ch := range results {
			result := <-ch
			if result.err != nil || len(result.messages) == 0 {
				if result.err != nil {
					sctx.GetSessionVars().StmtCtx.AppendWarning(result.err)
				}
				continue
			}
			*e.heap = append(*e.heap, result)
		}
		heap.Init(e.heap)
	}

	// Merge the results
	var finalRows [][]types.Datum
	for e.heap.Len() > 0 && len(finalRows) < clusterLogBatchSize {
		minTimeItem := heap.Pop(e.heap).(logStreamResult)
		headMessage := minTimeItem.messages[0]
		loggingTime := time.UnixMilli(headMessage.Time)
		finalRows = append(finalRows, types.MakeDatums(
			loggingTime.Format("2006/01/02 15:04:05.000"),
			minTimeItem.typ,
			minTimeItem.addr,
			strings.ToUpper(headMessage.Level.String()),
			headMessage.Message,
		))
		minTimeItem.messages = minTimeItem.messages[1:]
		// Current streaming result is drained, read the next to supply.
		if len(minTimeItem.messages) == 0 {
			result := <-minTimeItem.next
			if result.err != nil {
				sctx.GetSessionVars().StmtCtx.AppendWarning(result.err)
				continue
			}
			if len(result.messages) > 0 {
				heap.Push(e.heap, result)
			}
		} else {
			heap.Push(e.heap, minTimeItem)
		}
	}

	// All streams are drained
	e.isDrained = e.heap.Len() == 0

	return finalRows, nil
}

func (e *clusterLogRetriever) close() error {
	if e.cancel != nil {
		e.cancel()
	}
	return nil
}

func (*clusterLogRetriever) getRuntimeStats() execdetails.RuntimeStats {
	return nil
}

type hotRegionsResult struct {
	addr     string
	messages *HistoryHotRegions
	err      error
}

type hotRegionsResponseHeap []hotRegionsResult

func (h hotRegionsResponseHeap) Len() int {
	return len(h)
}

func (h hotRegionsResponseHeap) Less(i, j int) bool {
	lhs, rhs := h[i].messages.HistoryHotRegion[0], h[j].messages.HistoryHotRegion[0]
	if lhs.UpdateTime != rhs.UpdateTime {
		return lhs.UpdateTime < rhs.UpdateTime
	}
	return lhs.HotDegree < rhs.HotDegree
}

func (h hotRegionsResponseHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *hotRegionsResponseHeap) Push(x any) {
	*h = append(*h, x.(hotRegionsResult))
}

func (h *hotRegionsResponseHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type hotRegionsHistoryRetriver struct {
	dummyCloser
	isDrained  bool
	retrieving bool
	heap       *hotRegionsResponseHeap
	extractor  *plannercore.HotRegionsHistoryTableExtractor
}

// HistoryHotRegionsRequest wrap conditions push down to PD.
type HistoryHotRegionsRequest struct {
	StartTime      int64    `json:"start_time,omitempty"`
	EndTime        int64    `json:"end_time,omitempty"`
	RegionIDs      []uint64 `json:"region_ids,omitempty"`
	StoreIDs       []uint64 `json:"store_ids,omitempty"`
	PeerIDs        []uint64 `json:"peer_ids,omitempty"`
	IsLearners     []bool   `json:"is_learners,omitempty"`
	IsLeaders      []bool   `json:"is_leaders,omitempty"`
	HotRegionTypes []string `json:"hot_region_type,omitempty"`
}

// HistoryHotRegions records filtered hot regions stored in each PD.
// it's the response of PD.
type HistoryHotRegions struct {
	HistoryHotRegion []*HistoryHotRegion `json:"history_hot_region"`
}

// HistoryHotRegion records each hot region's statistics.
// it's the response of PD.
type HistoryHotRegion struct {
	UpdateTime    int64   `json:"update_time"`
	RegionID      uint64  `json:"region_id"`
	StoreID       uint64  `json:"store_id"`
	PeerID        uint64  `json:"peer_id"`
	IsLearner     bool    `json:"is_learner"`
	IsLeader      bool    `json:"is_leader"`
	HotRegionType string  `json:"hot_region_type"`
	HotDegree     int64   `json:"hot_degree"`
	FlowBytes     float64 `json:"flow_bytes"`
	KeyRate       float64 `json:"key_rate"`
	QueryRate     float64 `json:"query_rate"`
	StartKey      string  `json:"start_key"`
	EndKey        string  `json:"end_key"`
}

func (e *hotRegionsHistoryRetriver) initialize(_ context.Context, sctx sessionctx.Context) ([]chan hotRegionsResult, error) {
	if !hasPriv(sctx, mysql.ProcessPriv) {
		return nil, plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("PROCESS")
	}
	pdServers, err := infoschema.GetPDServerInfo(sctx)
	if err != nil {
		return nil, err
	}

	// To avoid search hot regions interface overload, the user should specify the time range in normally SQL.
	if e.extractor.StartTime == 0 {
		return nil, errors.New("denied to scan hot regions, please specified the start time, such as `update_time > '2020-01-01 00:00:00'`")
	}
	if e.extractor.EndTime == 0 {
		return nil, errors.New("denied to scan hot regions, please specified the end time, such as `update_time < '2020-01-01 00:00:00'`")
	}

	historyHotRegionsRequest := &HistoryHotRegionsRequest{
		StartTime:  e.extractor.StartTime,
		EndTime:    e.extractor.EndTime,
		RegionIDs:  e.extractor.RegionIDs,
		StoreIDs:   e.extractor.StoreIDs,
		PeerIDs:    e.extractor.PeerIDs,
		IsLearners: e.extractor.IsLearners,
		IsLeaders:  e.extractor.IsLeaders,
	}

	return e.startRetrieving(pdServers, historyHotRegionsRequest)
}

func (e *hotRegionsHistoryRetriver) startRetrieving(
	pdServers []infoschema.ServerInfo,
	req *HistoryHotRegionsRequest,
) ([]chan hotRegionsResult, error) {
	var results []chan hotRegionsResult
	for _, srv := range pdServers {
		for typ := range e.extractor.HotRegionTypes {
			req.HotRegionTypes = []string{typ}
			jsonBody, err := json.Marshal(req)
			if err != nil {
				return nil, err
			}
			body := bytes.NewBuffer(jsonBody)
			ch := make(chan hotRegionsResult)
			results = append(results, ch)
			go func(ch chan hotRegionsResult, address string, body *bytes.Buffer) {
				util.WithRecovery(func() {
					defer close(ch)
					url := fmt.Sprintf("%s://%s%s", util.InternalHTTPSchema(), address, pd.HotHistory)
					req, err := http.NewRequest(http.MethodGet, url, body)
					if err != nil {
						ch <- hotRegionsResult{err: errors.Trace(err)}
						return
					}
					req.Header.Add("PD-Allow-follower-handle", "true")
					resp, err := util.InternalHTTPClient().Do(req)
					if err != nil {
						ch <- hotRegionsResult{err: errors.Trace(err)}
						return
					}
					defer func() {
						terror.Log(resp.Body.Close())
					}()
					if resp.StatusCode != http.StatusOK {
						ch <- hotRegionsResult{err: errors.Errorf("request %s failed: %s", url, resp.Status)}
						return
					}
					var historyHotRegions HistoryHotRegions
					if err = json.NewDecoder(resp.Body).Decode(&historyHotRegions); err != nil {
						ch <- hotRegionsResult{err: errors.Trace(err)}
						return
					}
					ch <- hotRegionsResult{addr: address, messages: &historyHotRegions}
				}, nil)
			}(ch, srv.StatusAddr, body)
		}
	}
	return results, nil
}

func (e *hotRegionsHistoryRetriver) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if e.extractor.SkipRequest || e.isDrained {
		return nil, nil
	}

	if !e.retrieving {
		e.retrieving = true
		results, err := e.initialize(ctx, sctx)
		if err != nil {
			e.isDrained = true
			return nil, err
		}
		// Initialize the heap
		e.heap = &hotRegionsResponseHeap{}
		for _, ch := range results {
			result := <-ch
			if result.err != nil || len(result.messages.HistoryHotRegion) == 0 {
				if result.err != nil {
					sctx.GetSessionVars().StmtCtx.AppendWarning(result.err)
				}
				continue
			}
			*e.heap = append(*e.heap, result)
		}
		heap.Init(e.heap)
	}
	// Merge the results
	var finalRows [][]types.Datum
	tikvStore, ok := sctx.GetStore().(helper.Storage)
	if !ok {
		return nil, errors.New("Information about hot region can be gotten only when the storage is TiKV")
	}
	tikvHelper := &helper.Helper{
		Store:       tikvStore,
		RegionCache: tikvStore.GetRegionCache(),
	}
	tz := sctx.GetSessionVars().Location()
	is := sessiontxn.GetTxnManager(sctx).GetTxnInfoSchema()
	tables := tikvHelper.GetTablesInfoWithKeyRange(is, tikvHelper.FilterMemDBs)
	for e.heap.Len() > 0 && len(finalRows) < hotRegionsHistoryBatchSize {
		minTimeItem := heap.Pop(e.heap).(hotRegionsResult)
		rows, err := e.getHotRegionRowWithSchemaInfo(minTimeItem.messages.HistoryHotRegion[0], tikvHelper, tables, tz)
		if err != nil {
			return nil, err
		}
		if rows != nil {
			finalRows = append(finalRows, rows...)
		}
		minTimeItem.messages.HistoryHotRegion = minTimeItem.messages.HistoryHotRegion[1:]
		// Fetch next message item
		if len(minTimeItem.messages.HistoryHotRegion) != 0 {
			heap.Push(e.heap, minTimeItem)
		}
	}
	// All streams are drained
	e.isDrained = e.heap.Len() == 0
	return finalRows, nil
}

func (*hotRegionsHistoryRetriver) getHotRegionRowWithSchemaInfo(
	hisHotRegion *HistoryHotRegion,
	tikvHelper *helper.Helper,
	tables []helper.TableInfoWithKeyRange,
	tz *time.Location,
) ([][]types.Datum, error) {
	regionsInfo := []*pd.RegionInfo{
		{
			ID:       int64(hisHotRegion.RegionID),
			StartKey: hisHotRegion.StartKey,
			EndKey:   hisHotRegion.EndKey,
		}}
	regionsTableInfos := tikvHelper.ParseRegionsTableInfos(regionsInfo, tables)

	var rows [][]types.Datum
	// Ignore row without corresponding schema.
	if tableInfos, ok := regionsTableInfos[int64(hisHotRegion.RegionID)]; ok {
		for _, tableInfo := range tableInfos {
			updateTimestamp := time.UnixMilli(hisHotRegion.UpdateTime)
			if updateTimestamp.Location() != tz {
				updateTimestamp.In(tz)
			}
			updateTime := types.NewTime(types.FromGoTime(updateTimestamp), mysql.TypeTimestamp, types.MinFsp)
			row := make([]types.Datum, len(infoschema.GetTableTiDBHotRegionsHistoryCols()))
			row[0].SetMysqlTime(updateTime)
			row[1].SetString(strings.ToUpper(tableInfo.DB.Name.O), mysql.DefaultCollationName)
			row[2].SetString(strings.ToUpper(tableInfo.Table.Name.O), mysql.DefaultCollationName)
			row[3].SetInt64(tableInfo.Table.ID)
			if tableInfo.IsIndex {
				row[4].SetString(strings.ToUpper(tableInfo.Index.Name.O), mysql.DefaultCollationName)
				row[5].SetInt64(tableInfo.Index.ID)
			} else {
				row[4].SetNull()
				row[5].SetNull()
			}
			row[6].SetInt64(int64(hisHotRegion.RegionID))
			row[7].SetInt64(int64(hisHotRegion.StoreID))
			row[8].SetInt64(int64(hisHotRegion.PeerID))
			if hisHotRegion.IsLearner {
				row[9].SetInt64(1)
			} else {
				row[9].SetInt64(0)
			}
			if hisHotRegion.IsLeader {
				row[10].SetInt64(1)
			} else {
				row[10].SetInt64(0)
			}
			row[11].SetString(strings.ToUpper(hisHotRegion.HotRegionType), mysql.DefaultCollationName)
			row[12].SetInt64(hisHotRegion.HotDegree)
			row[13].SetFloat64(hisHotRegion.FlowBytes)
			row[14].SetFloat64(hisHotRegion.KeyRate)
			row[15].SetFloat64(hisHotRegion.QueryRate)
			rows = append(rows, row)
		}
	}

	return rows, nil
}

