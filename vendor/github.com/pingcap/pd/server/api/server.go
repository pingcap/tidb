// Copyright 2016 PingCAP, Inc.
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

	"github.com/gorilla/mux"
	"github.com/pingcap/pd/server"
	"github.com/urfave/negroni"
)

const apiPrefix = "/pd"

// NewHandler creates a HTTP handler for API.
func NewHandler(svr *server.Server) http.Handler {
	engine := negroni.New()

	recovery := negroni.NewRecovery()
	engine.Use(recovery)

	router := mux.NewRouter()
	router.PathPrefix(apiPrefix).Handler(negroni.New(
		newRedirector(svr),
		negroni.Wrap(createRouter(apiPrefix, svr)),
	))

	engine.UseHandler(router)

	return engine
}
