// Copyright 2018 PingCAP, Inc.
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

package domain

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/printer"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

//GetServerInfo gets self DDL server static information.
func GetServerInfo(ddlID string) *util.ServerInfo {
	cfg := config.GetGlobalConfig()
	info := &util.ServerInfo{
		ID:         ddlID,
		IP:         cfg.AdvertiseAddress,
		StatusPort: cfg.Status.StatusPort,
		Lease:      cfg.Lease,
	}
	info.Version = mysql.ServerVersion
	info.GitHash = printer.TiDBGitHash
	return info
}

// GetOwnerServerInfo gets owner DDL server static information from PD.
func GetOwnerServerInfo(d ddl.DDL) (*util.ServerInfo, error) {
	ctx := context.Background()
	ddlOwnerID, err := d.OwnerManager().GetOwnerID(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ownerInfo, err := d.SchemaSyncer().GetServerInfo(ctx, ddlOwnerID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return ownerInfo, nil
}

// GetAllServerInfo gets all DDL servers static information from PD.
func GetAllServerInfo(d ddl.DDL) (map[string]*util.ServerInfo, error) {
	ctx := context.Background()
	AllServerInfo, err := d.SchemaSyncer().GetAllServerInfo(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return AllServerInfo, nil
}

// StoreServerInfo stores self DDL server static information to PD when domain Init.
func StoreServerInfoToPD(d ddl.DDL) error {
	info := GetServerInfo(d.GetID())
	ctx := context.Background()
	return d.SchemaSyncer().StoreServerInfo(ctx, info)
}

// RemoveServerInfo remove self DDL server static information from PD when domain close.
func RemoveServerInfoFromPD(d ddl.DDL) {
	err := d.SchemaSyncer().RemoveServerInfo()
	if err != nil {
		log.Errorf("[ddl] remove self server info failed %v", err)
	}
}
