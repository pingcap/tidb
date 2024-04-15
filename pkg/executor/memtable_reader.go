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
	"cmp"
	"container/heap"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/diagnosticspb"
	"github.com/pingcap/sysutil"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/set"
	pd "github.com/tikv/pd/client/http"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

const clusterLogBatchSize = 256
const hotRegionsHistoryBatchSize = 256

type dummyCloser struct{}

func (dummyCloser) close() error { return nil }

func (dummyCloser) getRuntimeStats() execdetails.RuntimeStats { return nil }

type memTableRetriever interface {
	retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error)
	close() error
	getRuntimeStats() execdetails.RuntimeStats
}

// MemTableReaderExec executes memTable information retrieving from the MemTable components
type MemTableReaderExec struct {
	exec.BaseExecutor
	table     *model.TableInfo
	retriever memTableRetriever
	// cacheRetrieved is used to indicate whether has the parent executor retrieved
	// from inspection cache in inspection mode.
	cacheRetrieved bool
}

func (*MemTableReaderExec) isInspectionCacheableTable(tblName string) bool {
	switch tblName {
	case strings.ToLower(infoschema.TableClusterConfig),
		strings.ToLower(infoschema.TableClusterInfo),
		strings.ToLower(infoschema.TableClusterSystemInfo),
		strings.ToLower(infoschema.TableClusterLoad),
		strings.ToLower(infoschema.TableClusterHardware):
		return true
	default:
		return false
	}
}

// Next implements the Executor Next interface.
func (e *MemTableReaderExec) Next(ctx context.Context, req *chunk.Chunk) error {
	var (
		rows [][]types.Datum
		err  error
	)

	// The `InspectionTableCache` will be assigned in the begin of retrieving` and be
	// cleaned at the end of retrieving, so nil represents currently in non-inspection mode.
	if cache, tbl := e.Ctx().GetSessionVars().InspectionTableCache, e.table.Name.L; cache != nil &&
		e.isInspectionCacheableTable(tbl) {
		// TODO: cached rows will be returned fully, we should refactor this part.
		if !e.cacheRetrieved {
			// Obtain data from cache first.
			cached, found := cache[tbl]
			if !found {
				rows, err := e.retriever.retrieve(ctx, e.Ctx())
				cached = variable.TableSnapshot{Rows: rows, Err: err}
				cache[tbl] = cached
			}
			e.cacheRetrieved = true
			rows, err = cached.Rows, cached.Err
		}
	} else {
		rows, err = e.retriever.retrieve(ctx, e.Ctx())
	}
	if err != nil {
		return err
	}

	if len(rows) == 0 {
		req.Reset()
		return nil
	}

	req.GrowAndReset(len(rows))
	mutableRow := chunk.MutRowFromTypes(exec.RetTypes(e))
	for _, row := range rows {
		mutableRow.SetDatums(row...)
		req.AppendRow(mutableRow.ToRow())
	}
	return nil
}

// Close implements the Executor Close interface.
func (e *MemTableReaderExec) Close() error {
	if stats := e.retriever.getRuntimeStats(); stats != nil && e.RuntimeStats() != nil {
		defer e.Ctx().GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.ID(), stats)
	}
	return e.retriever.close()
}

type clusterConfigRetriever struct {
	dummyCloser
	retrieved bool
	extractor *plannercore.ClusterTableExtractor
}

// retrieve implements the memTableRetriever interface
func (e *clusterConfigRetriever) retrieve(_ context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if e.extractor.SkipRequest || e.retrieved {
		return nil, nil
	}
	e.retrieved = true
	return fetchClusterConfig(sctx, e.extractor.NodeTypes, e.extractor.Instances)
}

func fetchClusterConfig(sctx sessionctx.Context, nodeTypes, nodeAddrs set.StringSet) ([][]types.Datum, error) {
	type result struct {
		idx  int
		rows [][]types.Datum
		err  error
	}
	if !hasPriv(sctx, mysql.ConfigPriv) {
		return nil, plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("CONFIG")
	}
	serversInfo, err := infoschema.GetClusterServerInfo(sctx)
	failpoint.Inject("mockClusterConfigServerInfo", func(val failpoint.Value) {
		if s := val.(string); len(s) > 0 {
			// erase the error
			serversInfo, err = parseFailpointServerInfo(s), nil
		}
	})
	if err != nil {
		return nil, err
	}
	serversInfo = infoschema.FilterClusterServerInfo(serversInfo, nodeTypes, nodeAddrs)
	//nolint: prealloc
	var finalRows [][]types.Datum
	wg := sync.WaitGroup{}
	ch := make(chan result, len(serversInfo))
	for i, srv := range serversInfo {
		typ := srv.ServerType
		address := srv.Address
		statusAddr := srv.StatusAddr
		if len(statusAddr) == 0 {
			sctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("%s node %s does not contain status address", typ, address))
			continue
		}
		wg.Add(1)
		go func(index int) {
			util.WithRecovery(func() {
				defer wg.Done()
				var url string
				switch typ {
				case "pd":
					url = fmt.Sprintf("%s://%s%s", util.InternalHTTPSchema(), statusAddr, pd.Config)
				case "tikv", "tidb", "tiflash":
					url = fmt.Sprintf("%s://%s/config", util.InternalHTTPSchema(), statusAddr)
				case "tiproxy":
					url = fmt.Sprintf("%s://%s/api/admin/config?format=json", util.InternalHTTPSchema(), statusAddr)
				case "ticdc":
					url = fmt.Sprintf("%s://%s/config", util.InternalHTTPSchema(), statusAddr)
				case "tso":
					url = fmt.Sprintf("%s://%s/tso/api/v1/config", util.InternalHTTPSchema(), statusAddr)
				case "scheduling":
					url = fmt.Sprintf("%s://%s/scheduling/api/v1/config", util.InternalHTTPSchema(), statusAddr)
				default:
					ch <- result{err: errors.Errorf("currently we do not support get config from node type: %s(%s)", typ, address)}
					return
				}

				req, err := http.NewRequest(http.MethodGet, url, nil)
				if err != nil {
					ch <- result{err: errors.Trace(err)}
					return
				}
				req.Header.Add("PD-Allow-follower-handle", "true")
				resp, err := util.InternalHTTPClient().Do(req)
				if err != nil {
					ch <- result{err: errors.Trace(err)}
					return
				}
				defer func() {
					terror.Log(resp.Body.Close())
				}()
				if resp.StatusCode != http.StatusOK {
					ch <- result{err: errors.Errorf("request %s failed: %s", url, resp.Status)}
					return
				}
				var nested map[string]any
				if err = json.NewDecoder(resp.Body).Decode(&nested); err != nil {
					ch <- result{err: errors.Trace(err)}
					return
				}
				data := config.FlattenConfigItems(nested)
				type item struct {
					key string
					val string
				}
				var items []item
				for key, val := range data {
					if config.ContainHiddenConfig(key) {
						continue
					}
					var str string
					switch val := val.(type) {
					case string: // remove quotes
						str = val
					default:
						tmp, err := json.Marshal(val)
						if err != nil {
							ch <- result{err: errors.Trace(err)}
							return
						}
						str = string(tmp)
					}
					items = append(items, item{key: key, val: str})
				}
				slices.SortFunc(items, func(i, j item) int { return cmp.Compare(i.key, j.key) })
				var rows [][]types.Datum
				for _, item := range items {
					rows = append(rows, types.MakeDatums(
						typ,
						address,
						item.key,
						item.val,
					))
				}
				ch <- result{idx: index, rows: rows}
			}, nil)
		}(i)
	}

	wg.Wait()
	close(ch)

	// Keep the original order to make the result more stable
	var results []result //nolint: prealloc
	for result := range ch {
		if result.err != nil {
			sctx.GetSessionVars().StmtCtx.AppendWarning(result.err)
			continue
		}
		results = append(results, result)
	}
	slices.SortFunc(results, func(i, j result) int { return cmp.Compare(i.idx, j.idx) })
	for _, result := range results {
		finalRows = append(finalRows, result.rows...)
	}
	return finalRows, nil
}

type clusterServerInfoRetriever struct {
	dummyCloser
	extractor      *plannercore.ClusterTableExtractor
	serverInfoType diagnosticspb.ServerInfoType
	retrieved      bool
}

// retrieve implements the memTableRetriever interface
func (e *clusterServerInfoRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	switch e.serverInfoType {
	case diagnosticspb.ServerInfoType_LoadInfo,
		diagnosticspb.ServerInfoType_SystemInfo:
		if !hasPriv(sctx, mysql.ProcessPriv) {
			return nil, plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("PROCESS")
		}
	case diagnosticspb.ServerInfoType_HardwareInfo:
		if !hasPriv(sctx, mysql.ConfigPriv) {
			return nil, plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("CONFIG")
		}
	}
	if e.extractor.SkipRequest || e.retrieved {
		return nil, nil
	}
	e.retrieved = true
	serversInfo, err := infoschema.GetClusterServerInfo(sctx)
	if err != nil {
		return nil, err
	}
	serversInfo = infoschema.FilterClusterServerInfo(serversInfo, e.extractor.NodeTypes, e.extractor.Instances)
	return infoschema.FetchClusterServerInfoWithoutPrivilegeCheck(ctx, sctx.GetSessionVars(), serversInfo, e.serverInfoType, true)
}

func parseFailpointServerInfo(s string) []infoschema.ServerInfo {
	servers := strings.Split(s, ";")
	serversInfo := make([]infoschema.ServerInfo, 0, len(servers))
	for _, server := range servers {
		parts := strings.Split(server, ",")
		serversInfo = append(serversInfo, infoschema.ServerInfo{
			StatusAddr: parts[2],
			Address:    parts[1],
			ServerType: parts[0],
		})
	}
	return serversInfo
}

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
	allSchemaNames := is.AllSchemaNames()
	schemas := ensureSchemaTables(is, allSchemaNames)
	schemas = tikvHelper.FilterMemDBs(schemas)
	tables := tikvHelper.GetTablesInfoWithKeyRange(schemas)
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

type tikvRegionPeersRetriever struct {
	dummyCloser
	extractor *plannercore.TikvRegionPeersExtractor
	retrieved bool
}

func (e *tikvRegionPeersRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if e.extractor.SkipRequest || e.retrieved {
		return nil, nil
	}
	e.retrieved = true
	tikvStore, ok := sctx.GetStore().(helper.Storage)
	if !ok {
		return nil, errors.New("Information about hot region can be gotten only when the storage is TiKV")
	}
	tikvHelper := &helper.Helper{
		Store:       tikvStore,
		RegionCache: tikvStore.GetRegionCache(),
	}
	pdCli, err := tikvHelper.TryGetPDHTTPClient()
	if err != nil {
		return nil, err
	}

	var regionsInfo, regionsInfoByStoreID []pd.RegionInfo
	regionMap := make(map[int64]*pd.RegionInfo)
	storeMap := make(map[int64]struct{})

	if len(e.extractor.StoreIDs) == 0 && len(e.extractor.RegionIDs) == 0 {
		regionsInfo, err := pdCli.GetRegions(ctx)
		if err != nil {
			return nil, err
		}
		return e.packTiKVRegionPeersRows(regionsInfo.Regions, storeMap)
	}

	for _, storeID := range e.extractor.StoreIDs {
		// if a region_id located in 1, 4, 7 store we will get all of them when request any store_id,
		// storeMap is used to filter peers on unexpected stores.
		storeMap[int64(storeID)] = struct{}{}
		storeRegionsInfo, err := pdCli.GetRegionsByStoreID(ctx, storeID)
		if err != nil {
			return nil, err
		}
		for i, regionInfo := range storeRegionsInfo.Regions {
			// regionMap is used to remove dup regions and record the region in regionsInfoByStoreID.
			if _, ok := regionMap[regionInfo.ID]; !ok {
				regionsInfoByStoreID = append(regionsInfoByStoreID, regionInfo)
				regionMap[regionInfo.ID] = &storeRegionsInfo.Regions[i]
			}
		}
	}

	if len(e.extractor.RegionIDs) == 0 {
		return e.packTiKVRegionPeersRows(regionsInfoByStoreID, storeMap)
	}

	for _, regionID := range e.extractor.RegionIDs {
		regionInfoByStoreID, ok := regionMap[int64(regionID)]
		if !ok {
			// if there is storeIDs, target region_id is fetched by storeIDs,
			// otherwise we need to fetch it from PD.
			if len(e.extractor.StoreIDs) == 0 {
				regionInfo, err := pdCli.GetRegionByID(ctx, regionID)
				if err != nil {
					return nil, err
				}
				regionsInfo = append(regionsInfo, *regionInfo)
			}
		} else {
			regionsInfo = append(regionsInfo, *regionInfoByStoreID)
		}
	}

	return e.packTiKVRegionPeersRows(regionsInfo, storeMap)
}

func (e *tikvRegionPeersRetriever) isUnexpectedStoreID(storeID int64, storeMap map[int64]struct{}) bool {
	if len(e.extractor.StoreIDs) == 0 {
		return false
	}
	if _, ok := storeMap[storeID]; ok {
		return false
	}
	return true
}

func (e *tikvRegionPeersRetriever) packTiKVRegionPeersRows(
	regionsInfo []pd.RegionInfo, storeMap map[int64]struct{}) ([][]types.Datum, error) {
	//nolint: prealloc
	var rows [][]types.Datum
	for _, region := range regionsInfo {
		records := make([][]types.Datum, 0, len(region.Peers))
		pendingPeerIDSet := set.NewInt64Set()
		for _, peer := range region.PendingPeers {
			pendingPeerIDSet.Insert(peer.ID)
		}
		downPeerMap := make(map[int64]int64, len(region.DownPeers))
		for _, peerStat := range region.DownPeers {
			downPeerMap[peerStat.Peer.ID] = peerStat.DownSec
		}
		for _, peer := range region.Peers {
			// isUnexpectedStoreID return true if we should filter this peer.
			if e.isUnexpectedStoreID(peer.StoreID, storeMap) {
				continue
			}

			row := make([]types.Datum, len(infoschema.GetTableTiKVRegionPeersCols()))
			row[0].SetInt64(region.ID)
			row[1].SetInt64(peer.ID)
			row[2].SetInt64(peer.StoreID)
			if peer.IsLearner {
				row[3].SetInt64(1)
			} else {
				row[3].SetInt64(0)
			}
			if peer.ID == region.Leader.ID {
				row[4].SetInt64(1)
			} else {
				row[4].SetInt64(0)
			}
			if downSec, ok := downPeerMap[peer.ID]; ok {
				row[5].SetString(downPeer, mysql.DefaultCollationName)
				row[6].SetInt64(downSec)
			} else if pendingPeerIDSet.Exist(peer.ID) {
				row[5].SetString(pendingPeer, mysql.DefaultCollationName)
			} else {
				row[5].SetString(normalPeer, mysql.DefaultCollationName)
			}
			records = append(records, row)
		}
		rows = append(rows, records...)
	}
	return rows, nil
}
