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

	"github.com/gorilla/mux"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/session"
)

var globalServiceManager atomic.Value

func StartSharedServices(do *domain.Domain) error {
	ctx, cancel := context.WithCancel(context.Background())

	manager := &ServicesManager{
		do:      do,
		domains: session.NewDomainMap(),
		ctx:     ctx,
		cancel:  cancel,
	}

	if err := manager.Start(); err != nil {
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

		err = manager.doRegisterService(&ServiceDesc{
			Tp: "ddl",
			Domain: &DomainDesc{
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
