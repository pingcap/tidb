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

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"go.etcd.io/etcd/clientv3"
)

const (
	// trackingIDKey is a random tracking for the cluster that is saved to etcd. Tracking ID can be reset by user.
	trackingIDKey = "/tidb/telemetry/tracking_id"
)

// ResetTrackingID generates a new tracking ID.
func ResetTrackingID(etcdClient *clientv3.Client) (string, error) {
	if etcdClient == nil {
		return "", nil
	}
	id := uuid.New().String()
	ctx, cancel := context.WithTimeout(context.Background(), etcdOpTimeout)
	defer cancel()
	_, err := etcdClient.Put(ctx, trackingIDKey, id)
	if err != nil {
		return "", errors.Trace(err)
	}
	return id, nil
}

// GetTrackingID gets the current tracking ID.
// It is possible that return value is empty, which means the ID is not generated yet.
func GetTrackingID(etcdClient *clientv3.Client) (string, error) {
	if etcdClient == nil {
		return "", nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), etcdOpTimeout)
	defer cancel()
	resp, err := etcdClient.Get(ctx, trackingIDKey)
	if err != nil {
		return "", errors.Trace(err)
	}
	if len(resp.Kvs) == 0 {
		return "", nil
	}
	return string(resp.Kvs[0].Value), nil
}
