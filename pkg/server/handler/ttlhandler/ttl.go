// Copyright 2023 PingCAP, Inc.
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

package ttlhandler

import (
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/server/handler"
	"github.com/pingcap/tidb/pkg/session"
	ttlcient "github.com/pingcap/tidb/pkg/ttl/client"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// TTLJobTriggerHandler is used to trigger a TTL job manually
type TTLJobTriggerHandler struct {
	store kv.Storage
}

// NewTTLJobTriggerHandler returns a new TTLJobTriggerHandler
func NewTTLJobTriggerHandler(store kv.Storage) *TTLJobTriggerHandler {
	return &TTLJobTriggerHandler{store: store}
}

// ServeHTTP handles request of triger a ttl job
func (h TTLJobTriggerHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		handler.WriteError(w, errors.Errorf("This api only support POST method"))
		return
	}

	params := mux.Vars(req)
	dbName := strings.ToLower(params["db"])
	tableName := strings.ToLower(params["table"])

	ctx := req.Context()
	dom, err := session.GetDomain(h.store)
	if err != nil {
		log.Error("failed to get session domain", zap.Error(err))
		handler.WriteError(w, err)
		return
	}

	cli := dom.TTLJobManager().GetCommandCli()
	resp, err := ttlcient.TriggerNewTTLJob(ctx, cli, dbName, tableName)
	if err != nil {
		log.Error("failed to trigger new TTL job", zap.Error(err))
		handler.WriteError(w, err)
		return
	}
	handler.WriteData(w, resp)
	logutil.Logger(ctx).Info("trigger TTL job manually successfully",
		zap.String("dbName", dbName),
		zap.String("tableName", tableName),
		zap.Any("response", resp))
}
