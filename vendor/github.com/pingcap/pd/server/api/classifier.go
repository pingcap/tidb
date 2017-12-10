// Copyright 2017 PingCAP, Inc.
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

package api

import (
	"net/http"

	"github.com/juju/errors"
	"github.com/pingcap/pd/server"
	"github.com/unrolled/render"
)

var (
	errClassifierNotSupportHTTP = errors.New("classifier does not support HTTP")
)

// classifierHandler redirects requests to server's namespace classifier if the
// classifier implements http.Handler.
type classifierHandler struct {
	svr    *server.Server
	rd     *render.Render
	prefix string
}

func newClassifierHandler(svr *server.Server, rd *render.Render, prefix string) *classifierHandler {
	return &classifierHandler{
		svr:    svr,
		rd:     rd,
		prefix: prefix,
	}
}

func (h *classifierHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	cluster := h.svr.GetRaftCluster()
	if cluster == nil {
		h.rd.JSON(w, http.StatusInternalServerError, server.ErrNotBootstrapped.Error())
		return
	}
	classifier := cluster.GetNamespaceClassifier()
	if handler, ok := classifier.(http.Handler); ok {
		http.StripPrefix(h.prefix, handler).ServeHTTP(w, r)
	} else {
		h.rd.JSON(w, http.StatusNotAcceptable, errClassifierNotSupportHTTP.Error())
	}
}
