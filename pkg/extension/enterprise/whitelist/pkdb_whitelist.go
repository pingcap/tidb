// Copyright 2023-2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package whitelist

import (
	"context"
	"net"
	"strings"

	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

type access uint8

const (
	// unKnowPermission indicates the access privilege is unknown.
	unKnowPermission access = iota
	// accept indicates the access privilege is accept.
	confirmed
	// reject indicates the access privilege is reject.
	denied
)

type accessItem struct {
	// id and name seems to be useless here
	ipList []*net.IPNet
	access access
}

// ConnectionController is used to control the connection privilege.
type ConnectionController struct {
	items []*accessItem
}

func (av *ConnectionController) validate(c *variable.ConnectionInfo) {
	// if whitelist is empty, all ip can connect
	if len(av.items) == 0 {
		c.IPInWhiteList = true
		return
	}
	// get host ip
	c.IPInWhiteList = false
	hostIP, err := net.LookupIP(c.Host)
	if err != nil {
		logutil.BgLogger().Error("whitelist plugin parse ip failed", zap.Error(err))
		return
	}

	for _, item := range av.items {
		for _, ipList := range item.ipList {
			if ipList.Contains(hostIP[0]) {
				switch item.access {
				case confirmed, unKnowPermission:
					c.IPInWhiteList = true
					return
				case denied:
					return
				}
			}
		}
	}
}

var (
	// CreateWhitelistTableSQL is the SQL statement to create whitelist table.
	CreateWhitelistTableSQL = "CREATE TABLE IF NOT EXISTS mysql.whitelist (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
		"name VARCHAR(16) UNIQUE, list TEXT,action ENUM('accept','reject'))"
	// SelectWhitelistTableSQL is the SQL statement to select whitelist table.
	SelectWhitelistTableSQL = "SELECT * FROM mysql.whitelist"
)

func loadIPLists(ctx context.Context, exec sqlexec.SQLExecutor) ([]*accessItem, error) {
	rows, err := executeSQL(ctx, exec, SelectWhitelistTableSQL)
	if err != nil {
		return nil, err
	}

	var items []*accessItem
	for _, row := range rows {
		if it := extractAccessItemFromRow(row); it != nil {
			items = append(items, it)
		}
	}
	return items, nil
}

func extractAccessItemFromRow(rowFromTable chunk.Row) *accessItem {
	var item accessItem
	ipAddress := strings.Split(rowFromTable.GetString(2), ",")
	ipAddr4Log := make([]string, 0, len(ipAddress))
	for _, str := range ipAddress {
		str = strings.TrimSpace(str)
		_, ip, err := net.ParseCIDR(str)
		if err != nil {
			logutil.BgLogger().Error("ip address is invalid", zap.String("ip", str))
			continue
		}
		item.ipList = append(item.ipList, ip)
		ipAddr4Log = append(ipAddr4Log, ip.String())
	}
	switch rowFromTable.GetEnum(3).String() {
	case "accept":
		item.access = confirmed
	case "reject":
		item.access = denied
	default:
		item.access = unKnowPermission
		logutil.BgLogger().Warn("invalidate action type, use default action accept", zap.String("action", rowFromTable.GetString(3)))
	}
	if len(item.ipList) > 0 {
		logutil.BgLogger().Info("load access items", zap.String("ipList", strings.Join(ipAddress, ",")), zap.String("action", rowFromTable.GetEnum(3).String()))
		return &item
	}
	return nil
}
