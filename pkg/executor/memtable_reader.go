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
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/diagnosticspb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/set"
	pd "github.com/tikv/pd/client/http"
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

// Open implements the Executor Open interface.
func (e *MemTableReaderExec) Open(ctx context.Context) error {
	err := e.BaseExecutor.Open(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	// Activate the transaction, otherwise SELECT .. FROM INFORMATION_SCHEMA.XX .. does not block GC worker.
	// And if the query last too long (10min), it causes error "GC life time is shorter than transaction duration"
	if txn, err1 := e.Ctx().Txn(false); err1 == nil && txn != nil && txn.Valid() {
		// Call e.Ctx().Txn(true) may panic, it's too difficult to debug all the callers.
		_, err = e.Ctx().Txn(true)
	}
	return err
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

