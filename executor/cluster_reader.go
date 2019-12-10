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
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"

	"github.com/jeremywohl/flatten"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/infoschema"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/pdapi"
)

type clusterRetriever interface {
	retrieve(ctx sessionctx.Context) ([][]types.Datum, error)
}

// ClusterReaderExec executes cluster information retrieving from the cluster components
type ClusterReaderExec struct {
	baseExecutor
	retrieved bool
	retriever clusterRetriever
}

// Next implements the Executor Next interface.
func (e *ClusterReaderExec) Next(ctx context.Context, req *chunk.Chunk) error {
	if e.retrieved {
		req.Reset()
		return nil
	}

	rows, err := e.retriever.retrieve(e.ctx)
	if err != nil {
		return err
	}
	e.retrieved = true

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
	extractor *plannercore.ClusterConfigTableExtractor
}

// retrieve implements the clusterRetriever interface
func (e *clusterConfigRetriever) retrieve(ctx sessionctx.Context) ([][]types.Datum, error) {
	if e.extractor.SkipRequest {
		return nil, nil
	}

	type result struct {
		idx  int
		rows [][]types.Datum
		err  error
	}
	serversInfo, err := infoschema.GetClusterServerInfo(ctx)
	failpoint.Inject("mockClusterConfigServerInfo", func(val failpoint.Value) {
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
		// Skip some node type which has been filtered in WHERE cluase
		// e.g: SELECT * FROM cluster_config WHERE type='tikv'
		if len(e.extractor.NodeTypes) > 0 && !e.extractor.NodeTypes.Exist(typ) {
			continue
		}
		address := srv.Address
		// Skip some node address which has been filtered in WHERE cluase
		// e.g: SELECT * FROM cluster_config WHERE address='192.16.8.12:2379'
		if len(e.extractor.Addresses) > 0 && !e.extractor.Addresses.Exist(address) {
			continue
		}
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
