// Copyright 2020 PingCAP, Inc.
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
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/set"
	"github.com/pingcap/tidb/pkg/util/stringutil"
	pd "github.com/tikv/pd/client/http"
)

// SetConfigExec executes 'SET CONFIG' statement.
type SetConfigExec struct {
	exec.BaseExecutor
	p        *core.SetConfig
	jsonBody string
}

// Open implements the Executor Open interface.
func (s *SetConfigExec) Open(context.Context) error {
	if s.p.Type != "" {
		s.p.Type = strings.ToLower(s.p.Type)
		if s.p.Type != "tikv" && s.p.Type != "tidb" && s.p.Type != "pd" && s.p.Type != "tiflash" {
			return errors.Errorf("unknown type %v", s.p.Type)
		}
		if s.p.Type == "tidb" {
			return errors.Errorf("TiDB doesn't support to change configs online, please use SQL variables")
		}
	}
	if s.p.Instance != "" {
		s.p.Instance = strings.ToLower(s.p.Instance)
		if !isValidInstance(s.p.Instance) {
			return errors.Errorf("invalid instance %v", s.p.Instance)
		}
	}
	s.p.Name = strings.ToLower(s.p.Name)

	if s.p.Type == "tiflash" {
		if !strings.HasPrefix(s.p.Name, "raftstore-proxy.") {
			errorBody := "This command can only change config items begin with 'raftstore-proxy'. For other TiFlash config items, please update the config file directly. Your change to the config file will take effect immediately without a restart."
			return errors.Errorf(errorBody)
		}
		s.p.Name = strings.TrimPrefix(s.p.Name, "raftstore-proxy.")
	}

	body, err := ConvertConfigItem2JSON(s.Ctx(), s.p.Name, s.p.Value)
	s.jsonBody = body
	return err
}

// TestSetConfigServerInfoKey is used as the key to store 'TestSetConfigServerInfoFunc' in the context.
var TestSetConfigServerInfoKey stringutil.StringerStr = "TestSetConfigServerInfoKey"

// TestSetConfigHTTPHandlerKey is used as the key to store 'TestSetConfigDoRequestFunc' in the context.
var TestSetConfigHTTPHandlerKey stringutil.StringerStr = "TestSetConfigHTTPHandlerKey"

// Next implements the Executor Next interface.
func (s *SetConfigExec) Next(_ context.Context, req *chunk.Chunk) error {
	req.Reset()
	getServerFunc := infoschema.GetClusterServerInfo
	if v := s.Ctx().Value(TestSetConfigServerInfoKey); v != nil {
		getServerFunc = v.(func(sessionctx.Context) ([]infoschema.ServerInfo, error))
	}

	serversInfo, err := getServerFunc(s.Ctx())
	if err != nil {
		return err
	}
	nodeTypes := set.NewStringSet()
	nodeAddrs := set.NewStringSet()
	if s.p.Type != "" {
		nodeTypes.Insert(s.p.Type)
	}
	if s.p.Instance != "" {
		nodeAddrs.Insert(s.p.Instance)
	}
	serversInfo = infoschema.FilterClusterServerInfo(serversInfo, nodeTypes, nodeAddrs)
	if s.p.Instance != "" && len(serversInfo) == 0 {
		return errors.Errorf("instance %v is not found in this cluster", s.p.Instance)
	}

	for _, serverInfo := range serversInfo {
		var url string
		switch serverInfo.ServerType {
		case "pd":
			url = fmt.Sprintf("%s://%s%s", util.InternalHTTPSchema(), serverInfo.StatusAddr, pd.Config)
		case "tikv":
			url = fmt.Sprintf("%s://%s/config", util.InternalHTTPSchema(), serverInfo.StatusAddr)
		case "tiflash":
			url = fmt.Sprintf("%s://%s/config", util.InternalHTTPSchema(), serverInfo.StatusAddr)
		case "tidb":
			return errors.Errorf("TiDB doesn't support to change configs online, please use SQL variables")
		default:
			return errors.Errorf("Unknown server type %s", serverInfo.ServerType)
		}
		if err := s.doRequest(url); err != nil {
			s.Ctx().GetSessionVars().StmtCtx.AppendWarning(err)
		}
	}
	return nil
}

func (s *SetConfigExec) doRequest(url string) (retErr error) {
	body := bytes.NewBufferString(s.jsonBody)
	req, err := http.NewRequest(http.MethodPost, url, body)
	if err != nil {
		return err
	}
	var httpHandler func(req *http.Request) (*http.Response, error)
	if v := s.Ctx().Value(TestSetConfigHTTPHandlerKey); v != nil {
		httpHandler = v.(func(*http.Request) (*http.Response, error))
	} else {
		httpHandler = util.InternalHTTPClient().Do
	}
	resp, err := httpHandler(req)
	if err != nil {
		return err
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			if retErr == nil {
				retErr = err
			}
		}
	}()
	if resp.StatusCode == http.StatusOK {
		return nil
	} else if resp.StatusCode >= 400 && resp.StatusCode < 600 {
		message, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return errors.Errorf("bad request to %s: %s", url, message)
	}
	return errors.Errorf("request %s failed: %s", url, resp.Status)
}

func isValidInstance(instance string) bool {
	host, port, err := net.SplitHostPort(instance)
	if err != nil {
		return false
	}
	if port == "" {
		return false
	}
	_, err = net.LookupIP(host)
	return err == nil
}

// ConvertConfigItem2JSON converts the config item specified by key and val to json.
// For example:
//
//	set config x key="val" ==> {"key":"val"}
//	set config x key=233 ==> {"key":233}
func ConvertConfigItem2JSON(ctx sessionctx.Context, key string, val expression.Expression) (body string, err error) {
	if val == nil {
		return "", errors.Errorf("cannot set config to null")
	}
	isNull := false
	str := ""
	switch val.GetType(ctx.GetExprCtx().GetEvalCtx()).EvalType() {
	case types.ETString:
		var s string
		s, isNull, err = val.EvalString(ctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
		if err == nil && !isNull {
			str = fmt.Sprintf("%q", s)
		}
	case types.ETInt:
		var i int64
		i, isNull, err = val.EvalInt(ctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
		if err == nil && !isNull {
			if mysql.HasIsBooleanFlag(val.GetType(ctx.GetExprCtx().GetEvalCtx()).GetFlag()) {
				str = "true"
				if i == 0 {
					str = "false"
				}
			} else {
				str = fmt.Sprintf("%v", i)
			}
		}
	case types.ETReal:
		var f float64
		f, isNull, err = val.EvalReal(ctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
		if err == nil && !isNull {
			str = fmt.Sprintf("%v", f)
		}
	case types.ETDecimal:
		var d *types.MyDecimal
		d, isNull, err = val.EvalDecimal(ctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
		if err == nil && !isNull {
			str = string(d.ToString())
		}
	default:
		return "", errors.Errorf("unsupported config value type")
	}
	if err != nil {
		return
	}
	if isNull {
		return "", errors.Errorf("can't set config to null")
	}
	body = fmt.Sprintf(`{"%s":%s}`, key, str)
	return body, nil
}
