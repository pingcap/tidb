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

	"github.com/jeremywohl/flatten"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/diagnosticspb"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/sysutil"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/infoschema"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/pdapi"
	"github.com/pingcap/tidb/util/set"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const clusterLogBatchSize = 1024

type clusterRetriever interface {
	retrieve(ctx sessionctx.Context) ([][]types.Datum, error)
}

// ClusterReaderExec executes cluster information retrieving from the cluster components
type ClusterReaderExec struct {
	baseExecutor
	retriever clusterRetriever
}

// Next implements the Executor Next interface.
func (e *ClusterReaderExec) Next(ctx context.Context, req *chunk.Chunk) error {
	rows, err := e.retriever.retrieve(e.ctx)
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

type clusterConfigRetriever struct {
	retrieved bool
	extractor *plannercore.ClusterTableExtractor
}

func parseFailpointInjectServerInfo(s string) []infoschema.ServerInfo {
	var serversInfo []infoschema.ServerInfo
	servers := strings.Split(s, ";")
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

// retrieve implements the clusterRetriever interface
func (e *clusterConfigRetriever) retrieve(ctx sessionctx.Context) ([][]types.Datum, error) {
	if e.extractor.SkipRequest || e.retrieved {
		return nil, nil
	}
	e.retrieved = true

	type result struct {
		idx  int
		rows [][]types.Datum
		err  error
	}
	serversInfo, err := infoschema.GetClusterServerInfo(ctx)
	failpoint.Inject("mockClusterConfigServerInfo", func(val failpoint.Value) {
		if s := val.(string); len(s) > 0 {
			serversInfo = parseFailpointInjectServerInfo(s)
			// erase the error
			err = nil
		}
	})
	if err != nil {
		return nil, err
	}

	var finalRows [][]types.Datum
	wg := sync.WaitGroup{}
	ch := make(chan result, len(serversInfo))
	for i, srv := range serversInfo {
		typ := srv.ServerType
		address := srv.Address
		statusAddr := srv.StatusAddr
		if len(statusAddr) == 0 {
			ctx.GetSessionVars().StmtCtx.AppendWarning(errors.Errorf("%s node %s does not contain status address", typ, address))
			continue
		}
		wg.Add(1)
		go func(index int) {
			util.WithRecovery(func() {
				defer wg.Done()
				var url string
				switch typ {
				case "pd":
					url = fmt.Sprintf("http://%s%s", statusAddr, pdapi.Config)
				case "tikv", "tidb":
					url = fmt.Sprintf("http://%s/config", statusAddr)
				default:
					ch <- result{err: errors.Errorf("unknown node type: %s(%s)", typ, address)}
					return
				}

				req, err := http.NewRequest(http.MethodGet, url, nil)
				if err != nil {
					ch <- result{err: errors.Trace(err)}
					return
				}
				req.Header.Add("PD-Allow-follower-handle", "true")
				resp, err := http.DefaultClient.Do(req)
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
				data, err := flatten.Flatten(nested, "", flatten.DotStyle)
				if err != nil {
					ch <- result{err: errors.Trace(err)}
					return
				}
				// Sorts by keys and make the result stable
				type item struct {
					key string
					val string
				}
				var items []item
				for key, val := range data {
					items = append(items, item{key: key, val: fmt.Sprintf("%v", val)})
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
			ctx.GetSessionVars().StmtCtx.AppendWarning(result.err)
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
	extractor    *plannercore.ClusterTableExtractor
	serverInfoTP diagnosticspb.ServerInfoType
}

// retrieve implements the clusterRetriever interface
func (e *clusterServerInfoRetriever) retrieve(ctx sessionctx.Context) ([][]types.Datum, error) {
	if e.extractor.SkipRequest {
		return nil, nil
	}
	return e.dataForClusterInfo(ctx, e.serverInfoTP)
}

func (e *clusterServerInfoRetriever) dataForClusterInfo(ctx sessionctx.Context, infoTp diagnosticspb.ServerInfoType) ([][]types.Datum, error) {
	serversInfo, err := getClusterServerInfoWithFilter(ctx, e.extractor.NodeTypes, e.extractor.Addresses)
	if err != nil {
		return nil, err
	}
	type result struct {
		idx  int
		rows [][]types.Datum
		err  error
	}
	wg := sync.WaitGroup{}
	ch := make(chan result, len(serversInfo))
	ipMap := make(map[string]struct{}, len(serversInfo))
	finalRows := make([][]types.Datum, 0, len(serversInfo)*10)
	for i, srv := range serversInfo {
		address := srv.Address
		if srv.ServerType == "tidb" {
			address = srv.StatusAddr
		}
		ip := address
		if idx := strings.Index(address, ":"); idx != -1 {
			ip = address[:idx]
		}
		if _, ok := ipMap[ip]; ok {
			continue
		}
		ipMap[ip] = struct{}{}
		wg.Add(1)
		go func(index int, address, serverTP string) {
			util.WithRecovery(func() {
				defer wg.Done()
				items, err := getServerInfoByGRPC(address, infoTp)
				if err != nil {
					ch <- result{idx: index, err: err}
					return
				}
				partRows := serverInfoItemToRows(items, serverTP, address)
				ch <- result{idx: index, rows: partRows}
			}, nil)
		}(i, address, srv.ServerType)
	}
	wg.Wait()
	close(ch)
	// Keep the original order to make the result more stable
	var results []result
	for result := range ch {
		if result.err != nil {
			ctx.GetSessionVars().StmtCtx.AppendWarning(result.err)
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

func getServerInfoByGRPC(address string, tp diagnosticspb.ServerInfoType) ([]*diagnosticspb.ServerInfoItem, error) {
	opt := grpc.WithInsecure()
	security := config.GetGlobalConfig().Security
	if len(security.ClusterSSLCA) != 0 {
		tlsConfig, err := security.ToTLSConfig()
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
	// FIXME: use session context instead of context.Background().
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	r, err := cli.ServerInfo(ctx, &diagnosticspb.ServerInfoRequest{Tp: tp})
	if err != nil {
		return nil, err
	}
	return r.Items, nil
}

func getClusterServerInfoWithFilter(ctx sessionctx.Context, nodeTypes, addresses set.StringSet) ([]infoschema.ServerInfo, error) {
	isFailpointTestMode := false
	serversInfo, err := infoschema.GetClusterServerInfo(ctx)
	failpoint.Inject("mockClusterServerInfo", func(val failpoint.Value) {
		if s := val.(string); len(s) > 0 {
			serversInfo = serversInfo[:0]
			servers := strings.Split(s, ";")
			for _, server := range servers {
				parts := strings.Split(server, ",")
				serversInfo = append(serversInfo, infoschema.ServerInfo{
					ServerType: parts[0],
					Address:    parts[1],
					StatusAddr: parts[2],
				})
			}
			// erase the error
			err = nil
			isFailpointTestMode = true
		}
	})
	if err != nil {
		return nil, err
	}
	filterServers := make([]infoschema.ServerInfo, 0, len(serversInfo))
	for _, srv := range serversInfo {
		// Skip some node type which has been filtered in WHERE cluase
		// e.g: SELECT * FROM cluster_config WHERE type='tikv'
		if len(nodeTypes) > 0 && !nodeTypes.Exist(srv.ServerType) {
			continue
		}
		// Skip some node address which has been filtered in WHERE cluase
		// e.g: SELECT * FROM cluster_config WHERE address='192.16.8.12:2379'
		if len(addresses) > 0 && !addresses.Exist(srv.Address) {
			continue
		}
		filterServers = append(filterServers, srv)
	}
	return filterServers, nil
}

type clusterLogRetriever struct {
	isDrained  bool
	retrieving bool
	heap       *logResponseHeap
	extractor  *plannercore.ClusterLogTableExtractor
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

func (e *clusterLogRetriever) startRetrieving(ctx sessionctx.Context) ([]chan logStreamResult, error) {
	isFailpointTestMode := false
	serversInfo, err := infoschema.GetClusterServerInfo(ctx)
	failpoint.Inject("mockClusterLogServerInfo", func(val failpoint.Value) {
		if s := val.(string); len(s) > 0 {
			serversInfo = parseFailpointInjectServerInfo(s)
			// erase the error
			err = nil
			isFailpointTestMode = true
		}
	})
	if err != nil {
		return nil, err
	}

	// gRPC options
	opt := grpc.WithInsecure()
	security := config.GetGlobalConfig().Security
	if len(security.ClusterSSLCA) != 0 {
		tlsConfig, err := security.ToTLSConfig()
		if err != nil {
			return nil, errors.Trace(err)
		}
		opt = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	}

	var levels []diagnosticspb.LogLevel
	for l := range e.extractor.LogLevels {
		levels = append(levels, sysutil.ParseLogLevel(l))
	}
	req := &diagnosticspb.SearchLogRequest{
		StartTime: e.extractor.StartTime,
		EndTime:   e.extractor.EndTime,
		Levels:    levels,
		Patterns:  e.extractor.Patterns,
	}

	// There is no performance isssue to check this variable everytime because it will
	// be elimitation in non-failpoint mode.
	if !isFailpointTestMode {
		if req.StartTime == 0 {
			req.StartTime = time.Now().UnixNano()/int64(time.Millisecond) - int64(30*time.Minute/time.Millisecond)
		}

		// To avoid search log interface overload, the user should specify at least one pattern
		// in normally SQL. (But in test mode we should relax this limitation)
		if len(req.Patterns) == 0 && len(req.Levels) == 0 {
			return nil, errors.New("denied to scan full logs (use `SELECT * FROM cluster_log WHERE message LIKE '%'` explicitly if intentionally)")
		}
	}

	var results []chan logStreamResult
	for _, srv := range serversInfo {
		typ := srv.ServerType
		// Skip some node type which has been filtered in WHERE cluase
		// e.g: SELECT * FROM cluster_log WHERE type='tikv'
		if len(e.extractor.NodeTypes) > 0 && !e.extractor.NodeTypes.Exist(typ) {
			continue
		}
		address := srv.Address
		// Skip some node address which has been filtered in WHERE cluase
		// e.g: SELECT * FROM cluster_log WHERE address='192.16.8.12:2379'
		if len(e.extractor.Addresses) > 0 && !e.extractor.Addresses.Exist(address) {
			continue
		}
		statusAddr := srv.StatusAddr
		if len(statusAddr) == 0 {
			ctx.GetSessionVars().StmtCtx.AppendWarning(errors.Errorf("%s node %s does not contain status address", typ, address))
			continue
		}
		ch := make(chan logStreamResult)
		results = append(results, ch)

		go func(ch chan logStreamResult, serverType, address, statusAddr string) {
			util.WithRecovery(func() {
				defer close(ch)

				// The TiDB provide diagnostics service via status address
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
				stream, err := cli.SearchLog(context.Background(), req)
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
						ch <- logStreamResult{addr: address, typ: serverType, err: err}
						return
					}
					ch <- logStreamResult{next: ch, addr: address, typ: serverType, messages: res.Messages}
				}
			}, nil)
		}(ch, typ, address, statusAddr)
	}

	return results, nil
}

func (e *clusterLogRetriever) retrieve(ctx sessionctx.Context) ([][]types.Datum, error) {
	if e.extractor.SkipRequest || e.isDrained {
		return nil, nil
	}

	if !e.retrieving {
		e.retrieving = true
		results, err := e.startRetrieving(ctx)
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
					ctx.GetSessionVars().StmtCtx.AppendWarning(result.err)
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
		min := heap.Pop(e.heap).(logStreamResult)
		item := min.messages[0]
		time := time.Unix(item.Time/1000, (item.Time%1000)*int64(time.Millisecond))
		finalRows = append(finalRows, types.MakeDatums(
			time.Format("2006/01/02 15:04:05.000"),
			min.typ,
			min.addr,
			strings.ToUpper(item.Level.String()),
			item.Message,
		))
		min.messages = min.messages[1:]
		// Current streaming result is drained, read the next to supply.
		if len(min.messages) == 0 {
			result := <-min.next
			if result.err != nil {
				ctx.GetSessionVars().StmtCtx.AppendWarning(result.err)
				continue
			}
			if len(result.messages) > 0 {
				heap.Push(e.heap, result)
			}
		} else {
			heap.Push(e.heap, min)
		}
	}

	// All stream are draied
	e.isDrained = e.heap.Len() == 0

	return finalRows, nil
}
