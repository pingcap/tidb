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
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/pdapi"
	"github.com/pingcap/tidb/util/set"
	"github.com/pingcap/tidb/util/stringutil"
)

// SetConfigExec executes 'SET CONFIG' statement.
type SetConfigExec struct {
	baseExecutor
	p *core.SetConfig
	v string
}

// Open implements the Executor Open interface.
func (s *SetConfigExec) Open(ctx context.Context) error {
	// TODO: create a new privilege for this operation instead of using the SuperPriv
	checker := privilege.GetPrivilegeManager(s.ctx)
	if checker != nil && !checker.RequestVerification(s.ctx.GetSessionVars().ActiveRoles, "", "", "", mysql.SuperPriv) {
		return core.ErrSpecificAccessDenied.GenWithStackByArgs("SET CONFIG")
	}

	if s.p.Type != "" {
		s.p.Type = strings.ToLower(s.p.Type)
		if s.p.Type != "tikv" && s.p.Type != "tidb" && s.p.Type != "pd" {
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

	val, isNull, err := s.p.Value.EvalString(s.ctx, chunk.Row{})
	if err != nil {
		return err
	}
	if isNull {
		return errors.Errorf("can't set config to null")
	}
	s.v = val
	return nil
}

// TestSetConfigServerInfoKey is used as the key to store 'TestSetConfigServerInfoFunc' in the context.
var TestSetConfigServerInfoKey stringutil.StringerStr = "TestSetConfigServerInfoKey"

// TestSetConfigHTTPHandlerKey is used as the key to store 'TestSetConfigDoRequestFunc' in the context.
var TestSetConfigHTTPHandlerKey stringutil.StringerStr = "TestSetConfigHTTPHandlerKey"


// Next implements the Executor Next interface.
func (s *SetConfigExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	getServerFunc := infoschema.GetClusterServerInfo
	if v := s.ctx.Value(TestSetConfigServerInfoKey); v != nil {
		getServerFunc = v.(func(sessionctx.Context) ([]infoschema.ServerInfo, error))
	}

	serversInfo, err := getServerFunc(s.ctx)
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
	serversInfo = filterClusterServerInfo(serversInfo, nodeTypes, nodeAddrs)

	for _, serverInfo := range serversInfo {
		var url string
		switch serverInfo.ServerType {
		case "pd":
			url = fmt.Sprintf("%s://%s%s", util.InternalHTTPSchema(), serverInfo.StatusAddr, pdapi.Config)
		case "tikv":
			url = fmt.Sprintf("%s://%s/config", util.InternalHTTPSchema(), serverInfo.StatusAddr)
		default:
			continue
		}
		if err := s.doRequest(url); err != nil {
			s.ctx.GetSessionVars().StmtCtx.AppendWarning(err)
		}
	}
	return nil
}

func (s *SetConfigExec) doRequest(url string) error {
	body := bytes.NewBufferString(fmt.Sprintf("{'%s':'%s'}", s.p.Name, s.v))
	req, err := http.NewRequest(http.MethodPost, url, body)
	if err != nil {
		return err
	}
	var httpHandler func(req *http.Request) (*http.Response, error)
	if v := s.ctx.Value(TestSetConfigHTTPHandlerKey); v != nil {
		httpHandler = v.(func(*http.Request) (*http.Response, error))
	} else {
		httpHandler = util.InternalHTTPClient().Do
	}
	resp, err := httpHandler(req)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusOK {
		return nil
	} else if resp.StatusCode >= 400 && resp.StatusCode < 600 {
		message, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return errors.Errorf("bad request to %s: %s", url, message)
	}
	return errors.Errorf("request %s failed: %s", url, resp.Status)
}

func isValidInstance(instance string) bool {
	var ip, port string
	for i := len(instance) - 1; i >= 0; i-- {
		if instance[i] == ':' {
			ip = instance[:i]
			port = instance[i+1:]
		}
	}
	if port == "" {
		return false
	}
	v := net.ParseIP(ip)
	return v != nil
}
