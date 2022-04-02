// Copyright 2022 PingCAP, Inc.
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

package sharedservices

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/session"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var globalServiceManager atomic.Value

func StartSharedServices() error {
	ctx, cancel := context.WithCancel(context.Background())
	etcd, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://127.0.0.1:2379"},
		DialTimeout: 2 * time.Second,
	})

	if err != nil {
		cancel()
		return err
	}

	manager := &ServicesManager{
		domains: session.NewDomainMap(),
		ctx:     ctx,
		etcd:    etcd,
		cancel:  cancel,
	}

	if err := manager.Start(); err != nil {
		_ = etcd.Close()
		return err
	}

	globalServiceManager.Store(manager)
	return nil
}

func StopSharedServices() error {
	manager, err := getManager()
	if err != nil {
		return err
	}
	return manager.Stop()
}

func getManager() (*ServicesManager, error) {
	manager, ok := globalServiceManager.Load().(*ServicesManager)
	if !ok {
		return nil, errors.New("globalServiceManager not init")
	}
	return manager, nil
}

func HandleHttp(router *mux.Router) {
	router.HandleFunc("/clusters", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != "POST" {
			writeError(w, http.StatusMethodNotAllowed, errors.Errorf("Method '%s' not supported", req.Method))
		}

		var info ClusterInfo
		if err := unmarshalJsonRequest(req, &info); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}

		manager, err := getManager()
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		if err = manager.RegisterCluster(&info); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
	})

	router.HandleFunc("/clusters/{clusterID}/services/{serviceTp}", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != "PUT" {
			writeError(w, http.StatusMethodNotAllowed, errors.Errorf("Method '%s' not supported", req.Method))
		}

		vars := mux.Vars(req)
		clusterID := vars["clusterID"]
		serviceTp := vars["serviceTp"]
		manager, err := getManager()
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		if err = manager.EnableClusterService(clusterID, ServiceTp(serviceTp)); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
	})
}

func unmarshalJsonRequest(req *http.Request, v any) error {
	data, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return err
	}

	return json.Unmarshal(data, v)
}

func writeError(w http.ResponseWriter, status int, err error) {
	w.WriteHeader(status)
	_, err = w.Write([]byte(err.Error()))
	terror.Log(errors.Trace(err))
}
