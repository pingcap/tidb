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

package unistore

import (
	"io/ioutil"
	"os"

	usconf "github.com/ngaut/unistore/config"
	ussvr "github.com/ngaut/unistore/server"
	"github.com/pingcap/errors"
	pd "github.com/pingcap/pd/v4/client"
)

// New creates a embed unistore client, pd client and cluster handler.
func New(path string) (*RPCClient, pd.Client, *Cluster, error) {
	persistent := true
	if path == "" {
		var err error
		if path, err = ioutil.TempDir("", "tidb-unistore-temp"); err != nil {
			return nil, nil, nil, err
		}
		persistent = false
	}

	if err := os.MkdirAll(path, 0777); err != nil {
		return nil, nil, nil, err
	}

	conf := usconf.DefaultConf
	conf.Engine.ValueThreshold = 0
	conf.Engine.DBPath = path
	conf.Server.Raft = false

	if !persistent {
		conf.Engine.VolatileMode = true
		conf.Engine.MaxMemTableSize = 12 << 20
		conf.Engine.SyncWrite = false
		conf.Engine.NumCompactors = 1
		conf.Engine.CompactL0WhenClose = false
		conf.Engine.VlogFileSize = 16 << 20
	}

	srv, rm, pd, err := ussvr.NewMock(&conf, 1)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	cluster := newCluster(rm)
	client := &RPCClient{
		usSvr:      srv,
		cluster:    cluster,
		path:       path,
		persistent: persistent,
		rawHandler: newRawHandler(),
	}
	pdClient := newPDClient(pd)

	return client, pdClient, cluster, nil
}
