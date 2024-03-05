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

package globalkilltest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/util"
	"go.uber.org/zap"
)

const (
	timeoutCheckPDStatus   = 10 * time.Second
	timeoutCheckTiKVStatus = 30 * time.Second
	timeoutCheckTiDBStatus = 60 * time.Second // First start up of TiDB would take a long time.

	retryInterval = 500 * time.Millisecond
)

func withRetry[T any](fn func() (T, error), timeout time.Duration) (resp T, err error) {
	startTime := time.Now()
	retry := 0
	for time.Since(startTime) < timeout {
		retry += 1
		resp, err = fn()
		if err == nil {
			break
		}
		log.Debug("withRetry", zap.Error(err), zap.Int("retry", retry))
		time.Sleep(retryInterval)
	}
	return resp, errors.Trace(err)
}

func checkPDHealth(host string) error {
	url := util.ComposeURL(host, "/health")

	request := func() (any, error) {
		resp, err := util.InternalHTTPClient().Get(url)
		if err != nil {
			return nil, errors.Trace(err)
		}

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("PD health status code %d", resp.StatusCode)
		}
		defer resp.Body.Close()
		var health struct {
			Health string `json:"health"`
		}
		dec := json.NewDecoder(resp.Body)
		err = dec.Decode(&health)
		if err != nil {
			return nil, errors.Trace(err)
		}

		log.Info("PD health", zap.Any("health", health))
		if health.Health != "true" {
			return nil, fmt.Errorf("PD not healthy %v", health.Health)
		}
		return nil, nil
	}

	_, err := withRetry(request, timeoutCheckPDStatus)
	return errors.Trace(err)
}

func checkTiKVStatus() error {
	url := util.ComposeURL("127.0.0.1:20180", "/status")

	request := func() (any, error) {
		resp, err := util.InternalHTTPClient().Get(url)
		if err != nil {
			return nil, errors.Trace(err)
		}

		log.Info("TiKV status", zap.Any("status", resp.StatusCode))
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("TiKV status code %d", resp.StatusCode)
		}
		resp.Body.Close()
		return nil, nil
	}

	_, err := withRetry(request, timeoutCheckTiKVStatus)
	return errors.Trace(err)
}

func checkTiDBStatus(statusPort int) error {
	host := fmt.Sprintf("127.0.0.1:%d", statusPort)
	url := util.ComposeURL(host, "/status")

	request := func() (any, error) {
		resp, err := util.InternalHTTPClient().Get(url)
		if err != nil {
			return nil, errors.Trace(err)
		}

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("TiDB status code %d", resp.StatusCode)
		}
		defer resp.Body.Close()
		var status server.Status
		dec := json.NewDecoder(resp.Body)
		err = dec.Decode(&status)
		if err != nil {
			return nil, errors.Trace(err)
		}

		log.Info("TiDB status", zap.Any("status", status))
		return nil, nil
	}

	_, err := withRetry(request, timeoutCheckTiDBStatus)
	return errors.Trace(err)
}
