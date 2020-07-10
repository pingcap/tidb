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

package telemetry

import (
	"context"
	"encoding/json"

	"github.com/pingcap/errors"
	"go.etcd.io/etcd/clientv3"
)

const (
	statusKey = "/tidb/telemetry/status"
)

type status struct {
	CheckAt       string `json:"check_at"`
	IsError       bool   `json:"is_error"`
	ErrorMessage  string `json:"error_msg"`
	IsRequestSent bool   `json:"is_request_sent"`
}

// updateTelemetryStatus updates the telemetry status to etcd.
func updateTelemetryStatus(s status, etcdClient *clientv3.Client) error {
	if etcdClient == nil {
		return nil
	}
	j, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return errors.Trace(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), etcdOpTimeout)
	defer cancel()
	_, err = etcdClient.Put(ctx, statusKey, string(j))
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// GetTelemetryStatus returns the telemetry status saved in etcd.
func GetTelemetryStatus(etcdClient *clientv3.Client) (string, error) {
	if etcdClient == nil {
		return "", nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), etcdOpTimeout)
	defer cancel()
	resp, err := etcdClient.Get(ctx, statusKey)
	if err != nil {
		return "", errors.Trace(err)
	}
	if len(resp.Kvs) == 0 {
		return "", nil
	}
	return string(resp.Kvs[0].Value), nil
}
