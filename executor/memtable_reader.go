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
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"container/heap"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/diagnosticspb"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/sysutil"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/infoschema"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/pdapi"
	"github.com/pingcap/tidb/util/set"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const clusterLogBatchSize = 256

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
	baseExecutor
	table     *model.TableInfo
	retriever memTableRetriever
	// cacheRetrieved is used to indicate whether has the parent executor retrieved
	// from inspection cache in inspection mode.
	cacheRetrieved bool
}

func (e *MemTableReaderExec) isInspectionCacheableTable(tblName string) bool {
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
	if cache, tbl := e.ctx.GetSessionVars().InspectionTableCache, e.table.Name.L; cache != nil &&
		e.isInspectionCacheableTable(tbl) {
		// TODO: cached rows will be returned fully, we should refactor this part.
		if !e.cacheRetrieved {
			// Obtain data from cache first.
			cached, found := cache[tbl]
			if !found {
				rows, err := e.retriever.retrieve(ctx, e.ctx)
				cached = variable.TableSnapshot{Rows: rows, Err: err}
				cache[tbl] = cached
			}
			e.cacheRetrieved = true
			rows, err = cached.Rows, cached.Err
		}
	} else {
		rows, err = e.retriever.retrieve(ctx, e.ctx)
	}
	if err != nil {
		return err
	}

	if len(rows) == 0 {
		req.Reset()
		return nil
	}

	req.GrowAndReset(len(rows))
	mutableRow := chunk.MutRowFromTypes(retTypes(e))
	for _, row := range rows {
		mutableRow.SetDatums(row...)
		req.AppendRow(mutableRow.ToRow())
	}
	return nil
}

// Close implements the Executor Close interface.
func (e *MemTableReaderExec) Close() error {
	if stats := e.retriever.getRuntimeStats(); stats != nil && e.runtimeStats != nil {
		e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.id, stats)
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
		return nil, plannercore.ErrSpecificAccessDenied.GenWithStackByArgs("CONFIG")
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
	serversInfo = filterClusterServerInfo(serversInfo, nodeTypes, nodeAddrs)

	var finalRows [][]types.Datum
	wg := sync.WaitGroup{}
	ch := make(chan result, len(serversInfo))
	for i, srv := range serversInfo {
		typ := srv.ServerType
		address := srv.Address
		statusAddr := srv.StatusAddr
		if len(statusAddr) == 0 {
			sctx.GetSessionVars().StmtCtx.AppendWarning(errors.Errorf("%s node %s does not contain status address", typ, address))
			continue
		}
		wg.Add(1)
		go func(index int) {
			util.WithRecovery(func() {
				defer wg.Done()
				var url string
				switch typ {
				case "pd":
					url = fmt.Sprintf("%s://%s%s", util.InternalHTTPSchema(), statusAddr, pdapi.Config)
				case "tikv", "tidb":
					url = fmt.Sprintf("%s://%s/config", util.InternalHTTPSchema(), statusAddr)
				case "tiflash":
					// TODO: support show tiflash config once tiflash supports it
					return
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
				var nested map[string]interface{}
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
				sort.Slice(items, func(i, j int) bool { return items[i].key < items[j].key })
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
	var results []result
	for result := range ch {
		if result.err != nil {
			sctx.GetSessionVars().StmtCtx.AppendWarning(result.err)
			continue
		}
		results = append(results, result)
	}
	sort.Slice(results, func(i, j int) bool { return results[i].idx < results[j].idx })
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
			return nil, plannercore.ErrSpecificAccessDenied.GenWithStackByArgs("PROCESS")
		}
	case diagnosticspb.ServerInfoType_HardwareInfo:
		if !hasPriv(sctx, mysql.ConfigPriv) {
			return nil, plannercore.ErrSpecificAccessDenied.GenWithStackByArgs("CONFIG")
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
	serversInfo = filterClusterServerInfo(serversInfo, e.extractor.NodeTypes, e.extractor.Instances)

	type result struct {
		idx  int
		rows [][]types.Datum
		err  error
	}
	wg := sync.WaitGroup{}
	ch := make(chan result, len(serversInfo))
	infoTp := e.serverInfoType
	finalRows := make([][]types.Datum, 0, len(serversInfo)*10)
	for i, srv := range serversInfo {
		address := srv.Address
		remote := address
		if srv.ServerType == "tidb" {
			remote = srv.StatusAddr
		}
		wg.Add(1)
		go func(index int, remote, address, serverTP string) {
			util.WithRecovery(func() {
				defer wg.Done()
				items, err := getServerInfoByGRPC(ctx, remote, infoTp)
				if err != nil {
					ch <- result{idx: index, err: err}
					return
				}
				partRows := serverInfoItemToRows(items, serverTP, address)
				ch <- result{idx: index, rows: partRows}
			}, nil)
		}(i, remote, address, srv.ServerType)
	}
	wg.Wait()
	close(ch)
	// Keep the original order to make the result more stable
	var results []result
	for result := range ch {
		if result.err != nil {
			sctx.GetSessionVars().StmtCtx.AppendWarning(result.err)
			continue
		}
		results = append(results, result)
	}
	sort.Slice(results, func(i, j int) bool { return results[i].idx < results[j].idx })
	for _, result := range results {
		finalRows = append(finalRows, result.rows...)
	}
	return finalRows, nil
}

func serverInfoItemToRows(items []*diagnosticspb.ServerInfoItem, tp, addr string) [][]types.Datum {
	rows := make([][]types.Datum, 0, len(items))
	for _, v := range items {
		for _, item := range v.Pairs {
			row := types.MakeDatums(
				tp,
				addr,
				v.Tp,
				v.Name,
				item.Key,
				item.Value,
			)
			rows = append(rows, row)
		}
	}
	return rows
}

func getServerInfoByGRPC(ctx context.Context, address string, tp diagnosticspb.ServerInfoType) ([]*diagnosticspb.ServerInfoItem, error) {
	opt := grpc.WithInsecure()
	security := config.GetGlobalConfig().Security
	if len(security.ClusterSSLCA) != 0 {
		clusterSecurity := security.ClusterSecurity()
		tlsConfig, err := clusterSecurity.ToTLSConfig()
		if err != nil {
			return nil, errors.Trace(err)
		}
		opt = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	}
	conn, err := grpc.Dial(address, opt)
	if err != nil {
		return nil, err
	}
	defer func() {
		err := conn.Close()
		if err != nil {
			log.Error("close grpc connection error", zap.Error(err))
		}
	}()

	cli := diagnosticspb.NewDiagnosticsClient(conn)
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	r, err := cli.ServerInfo(ctx, &diagnosticspb.ServerInfoRequest{Tp: tp})
	if err != nil {
		return nil, err
	}
	return r.Items, nil
}

func parseFailpointServerInfo(s string) []infoschema.ServerInfo {
	servers := strings.Split(s, ";")
	serversInfo := make([]infoschema.ServerInfo, 0, len(servers))
	for _, server := range servers {
		parts := strings.Split(server, ",")
		serversInfo = append(serversInfo, infoschema.ServerInfo{
			ServerType: parts[0],
			Address:    parts[1],
			StatusAddr: parts[2],
		})
	}
	return serversInfo
}

func filterClusterServerInfo(serversInfo []infoschema.ServerInfo, nodeTypes, addresses set.StringSet) []infoschema.ServerInfo {
	if len(nodeTypes) == 0 && len(addresses) == 0 {
		return serversInfo
	}

	filterServers := make([]infoschema.ServerInfo, 0, len(serversInfo))
	for _, srv := range serversInfo {
		// Skip some node type which has been filtered in WHERE clause
		// e.g: SELECT * FROM cluster_config WHERE type='tikv'
		if len(nodeTypes) > 0 && !nodeTypes.Exist(srv.ServerType) {
			continue
		}
		// Skip some node address which has been filtered in WHERE clause
		// e.g: SELECT * FROM cluster_config WHERE address='192.16.8.12:2379'
		if len(addresses) > 0 && !addresses.Exist(srv.Address) {
			continue
		}
		filterServers = append(filterServers, srv)
	}
	return filterServers
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

func (h *logResponseHeap) Push(x interface{}) {
	*h = append(*h, x.(logStreamResult))
}

func (h *logResponseHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (e *clusterLogRetriever) initialize(ctx context.Context, sctx sessionctx.Context) ([]chan logStreamResult, error) {
	if !hasPriv(sctx, mysql.ProcessPriv) {
		return nil, plannercore.ErrSpecificAccessDenied.GenWithStackByArgs("PROCESS")
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
	serversInfo = filterClusterServerInfo(serversInfo, nodeTypes, instances)

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
	opt := grpc.WithInsecure()
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

	var results []chan logStreamResult
	for _, srv := range serversInfo {
		typ := srv.ServerType
		address := srv.Address
		statusAddr := srv.StatusAddr
		if len(statusAddr) == 0 {
			sctx.GetSessionVars().StmtCtx.AppendWarning(errors.Errorf("%s node %s does not contain status address", typ, address))
			continue
		}
		ch := make(chan logStreamResult)
		results = append(results, ch)

		go func(ch chan logStreamResult, serverType, address, statusAddr string) {
			util.WithRecovery(func() {
				defer close(ch)

				// The TiDB provides diagnostics service via status address
				remote := address
				if serverType == "tidb" {
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
		loggingTime := time.Unix(headMessage.Time/1000, (headMessage.Time%1000)*int64(time.Millisecond))
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

func (e *clusterLogRetriever) getRuntimeStats() execdetails.RuntimeStats {
	return nil
}

type tikvRegionStatusRetriever struct {
	retrieved bool
	isDrained bool
	extractor *plannercore.TikvRegionStatusExtractor
	rows      [][]types.Datum
}

func (e *tikvRegionStatusRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if e.extractor.SkipRequest || e.isDrained {
		return nil, nil
	}
	dbNames := e.extractor.DBNames
	tableNames := e.extractor.TableNames
	indexNames := e.extractor.IndexNames
	tableIDs := e.extractor.TableIDs
	indexIDs := e.extractor.IndexIDs
	infoSchema := sctx.GetInfoSchema().(infoschema.InfoSchema)
	startKeys, endKeys := e.extractStartKeysAndEndKeys(dbNames, tableNames, indexNames, tableIDs, indexIDs, infoSchema)
	tikvStore, ok := sctx.GetStore().(helper.Storage)
	if !ok {
		return nil, errors.New("Information about TiKV region status can be gotten only when the storage is TiKV")
	}
	tikvHelper := &helper.Helper{
		Store:       tikvStore,
		RegionCache: tikvStore.GetRegionCache(),
	}
	for i := range startKeys {
		regionsInfo, err := tikvHelper.GetRegionsInfoByStartKeyAndEndKey(startKeys[i], endKeys[i])
		if err != nil {
			return nil, err
		}
		tableInfos := tikvHelper.GetRegionsTableInfo(regionsInfo, infoSchema.AllSchemas())
		for _, region := range regionsInfo.Regions {
			tableList := tableInfos[region.ID]
			if len(tableList) == 0 {
				e.setNewTiKVRegionStatusCol(&region, nil)
			}
			for _, table := range tableList {
				e.setNewTiKVRegionStatusCol(&region, &table)
			}
		}
	}
	return e.rows, nil
}

func (e *tikvRegionStatusRetriever) setNewTiKVRegionStatusCol(region *helper.RegionInfo, table *helper.TableInfo) {
	row := make([]types.Datum, len(infoschema.TableTiKVRegionStatusCols))
	row[0].SetInt64(region.ID)
	row[1].SetString(region.StartKey, mysql.DefaultCollationName)
	row[2].SetString(region.EndKey, mysql.DefaultCollationName)
	if table != nil {
		row[3].SetInt64(table.Table.ID)
		row[4].SetString(table.DB.Name.O, mysql.DefaultCollationName)
		row[5].SetString(table.Table.Name.O, mysql.DefaultCollationName)
		if table.IsIndex {
			row[6].SetInt64(1)
			row[7].SetInt64(table.Index.ID)
			row[8].SetString(table.Index.Name.O, mysql.DefaultCollationName)
		} else {
			row[6].SetInt64(0)
		}
	}
	row[9].SetInt64(region.Epoch.ConfVer)
	row[10].SetInt64(region.Epoch.Version)
	row[11].SetUint64(region.WrittenBytes)
	row[12].SetUint64(region.ReadBytes)
	row[13].SetInt64(region.ApproximateSize)
	row[14].SetInt64(region.ApproximateKeys)
	if region.ReplicationStatus != nil {
		row[15].SetString(region.ReplicationStatus.State, mysql.DefaultCollationName)
		row[16].SetInt64(region.ReplicationStatus.StateID)
	}
	e.rows = append(e.rows, row)
}

func (e *tikvRegionStatusRetriever) extractStartKeysAndEndKeys(dbNames, tableNames, indexNames set.StringSet,
	tableIDs, indexIDs set.Int64Set, infoSchema infoschema.InfoSchema) ([][]byte, [][]byte) {
	var dbSchemas []*model.DBInfo
	var tableInfos []*model.TableInfo
	var startKeys, endKeys [][]byte
	if len(dbNames) == 0 {
		dbSchemas = infoSchema.AllSchemas()
	} else {
		for dbName := range dbNames {
			if dbInfo, ok := infoSchema.SchemaByName(model.NewCIStr(dbName)); ok {
				dbSchemas = append(dbSchemas, dbInfo)
			}
		}
	}
	for _, dbSchema := range dbSchemas {
		for _, tableInfo := range dbSchema.Tables {
			tableInfos = append(tableInfos, tableInfo)
		}
	}
	if len(tableNames) != 0 {
		var tmpTableInfos []*model.TableInfo
		for _, tableInfo := range tableInfos {
			if tableNames.Exist(tableInfo.Name.L) {
				tmpTableInfos = append(tmpTableInfos, tableInfo)
			}
		}
		tableInfos = tmpTableInfos
	}
	if len(tableIDs) != 0 {
		var tmpTableInfos []*model.TableInfo
		for _, tableInfo := range tableInfos {
			if tableIDs.Exist(tableInfo.ID) {
				tmpTableInfos = append(tmpTableInfos, tableInfo)
			}
		}
		tableInfos = tmpTableInfos
	}
	tableToPhysicalIDs := make(map[*model.TableInfo][]int64)
	for _, tableInfo := range tableInfos {
		if pi := tableInfo.GetPartitionInfo(); pi != nil {
			physicalIDs := []int64{}
			for _, def := range pi.Definitions {
				physicalIDs = append(physicalIDs, def.ID)
			}
			tableToPhysicalIDs[tableInfo] = physicalIDs
		}
	}
	// If there is no requirement for index,extract startkeys and endkeys return.
	if len(indexIDs) == 0 && len(indexNames) == 0 {
		for tableInfo, physicalIDs := range tableToPhysicalIDs {
			for _, physicalID := range physicalIDs {
				startKey, endKey := tablecodec.GetTableHandleKeyRange(physicalID)
				startKeys = append(startKeys, startKey)
				endKeys = append(endKeys, endKey)
				for _, indexInfo := range tableInfo.Indices {
					startKey, endKey := tablecodec.GetTableIndexKeyRange(physicalID, indexInfo.ID)
					startKeys = append(startKeys, startKey)
					endKeys = append(endKeys, endKey)
				}
			}
		}
		return startKeys, endKeys
	}
	for _, tableInfo := range tableInfos {
		for _, indexInfo := range tableInfo.Indices {
			if (len(indexNames) == 0 || indexNames.Exist(indexInfo.Name.L)) &&
				(len(indexIDs) == 0 || indexIDs.Exist(indexInfo.ID)) {
				physicalIDs := tableToPhysicalIDs[tableInfo]
				for _, physicalID := range physicalIDs {
					startKey, endKey := tablecodec.GetTableIndexKeyRange(physicalID, indexInfo.ID)
					startKeys = append(startKeys, startKey)
					endKeys = append(endKeys, endKey)
				}
			}
		}
	}
	return startKeys, endKeys
}
