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
	"net/http"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/errors"
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
	router.HandleFunc("/register", func(writer http.ResponseWriter, request *http.Request) {
		manager, err := getManager()
		if err != nil {
			writer.WriteHeader(500)
			_, _ = writer.Write([]byte(err.Error()))
			return
		}

		err = manager.registerServiceInfo(&ServiceDesc{
			Tp: "ddl",
			Domain: &DomainInfo{
				Addr: "tikv://127.0.0.1:2379",
			},
		})

		if err != nil {
			writer.WriteHeader(400)
			_, _ = writer.Write([]byte(err.Error()))
			return
		}
	})
}
