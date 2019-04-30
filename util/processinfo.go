// Copyright 2017 PingCAP, Inc.
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

package util

import (
	"fmt"
	"time"

	"github.com/pingcap/parser/mysql"
)

// ProcessInfo is a struct used for show processlist statement.
type ProcessInfo struct {
	ID      uint64
	User    string
	Host    string
	DB      string
	Command byte
	Plan    interface{}
	Time    time.Time
	State   uint16
	Info    string
}

// ToRow returns []interface{} for the row data of "show processlist" and "select * from infoschema.processlist".
func (pi *ProcessInfo) ToRow(full bool) []interface{} {
	var info string
	if full {
		info = pi.Info
	} else {
		info = fmt.Sprintf("%.100v", pi.Info)
	}
	t := uint64(time.Since(pi.Time) / time.Second)
	return []interface{}{
		pi.ID,
		pi.User,
		pi.Host,
		pi.DB,
		mysql.Command2Str[pi.Command],
		t,
		fmt.Sprintf("%d", pi.State),
		info,
	}
}

// SessionManager is an interface for session manage. Show processlist and
// kill statement rely on this interface.
type SessionManager interface {
	// ShowProcessList returns map[connectionID]ProcessInfo
	ShowProcessList() map[uint64]ProcessInfo
	GetProcessInfo(id uint64) (ProcessInfo, bool)
	Kill(connectionID uint64, query bool)
}
