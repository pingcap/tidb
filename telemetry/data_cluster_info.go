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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/sqlexec"
)

type clusterInfoItem struct {
	InstanceType   string `json:"instanceType"`
	ListenHostHash string `json:"listenHostHash"`
	ListenPort     string `json:"listenPort"`
	StatusHostHash string `json:"statusHostHash,omitempty"`
	StatusPort     string `json:"statusPort,omitempty"`
	Version        string `json:"version,omitempty"`
	GitHash        string `json:"gitHash,omitempty"`
	StartTime      string `json:"startTime,omitempty"`
	UpTime         string `json:"upTime,omitempty"`
}

func getClusterInfo(ctx sessionctx.Context) ([]*clusterInfoItem, error) {
	// Explicitly list all field names instead of using `*` to avoid potential leaking sensitive info when adding new fields in future.
	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(`SELECT TYPE, INSTANCE, STATUS_ADDRESS, VERSION, GIT_HASH, START_TIME, UPTIME FROM information_schema.cluster_info`)
	if err != nil {
		return nil, errors.Trace(err)
	}
	r := make([]*clusterInfoItem, 0)
	for _, row := range rows {
		if row.Len() < 7 {
			continue
		}
		listenHostHash, listenPort := parseAddressAndHash(row.GetString(1))
		statusHostHash, statusPort := parseAddressAndHash(row.GetString(2))
		r = append(r, &clusterInfoItem{
			InstanceType:   row.GetString(0),
			ListenHostHash: listenHostHash,
			ListenPort:     listenPort,
			StatusHostHash: statusHostHash,
			StatusPort:     statusPort,
			Version:        row.GetString(3),
			GitHash:        row.GetString(4),
			StartTime:      row.GetString(5),
			UpTime:         row.GetString(6),
		})
	}
	return r, nil
}
