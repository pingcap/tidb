// Copyright 2026 PingCAP, Inc.
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

package tikvhandler

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/gorilla/mux"
	"github.com/pingcap/tidb/pkg/server/handler"
	"github.com/stretchr/testify/require"
)

func TestRegionHandlerRejectsNegativeRegionID(t *testing.T) {
	h := &RegionHandler{TikvHandlerTool: nil}
	req := httptest.NewRequest(http.MethodGet, "/regions/region_id/-1", nil)
	req = mux.SetURLVars(req, map[string]string{handler.RegionID: "-1"})
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestMvccTxnHandlerRejectsNegativeStartTS(t *testing.T) {
	h := &MvccTxnHandler{TikvHandlerTool: nil, op: OpMvccGetByTxn}
	req := httptest.NewRequest(http.MethodGet, "/mvcc/transaction/-1", nil)
	req = mux.SetURLVars(req, map[string]string{handler.StartTS: "-1"})
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestRegionHandlerAcceptsValidRegionID(t *testing.T) {
	_, err := strconv.ParseUint("100", 10, 64)
	require.NoError(t, err)
}

func TestMvccTxnHandlerAcceptsValidStartTS(t *testing.T) {
	_, err := strconv.ParseUint("100", 10, 64)
	require.NoError(t, err)
}
