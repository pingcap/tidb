// Copyright 2023 PingCAP, Inc. Licensed under Apache-2.0.
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

package export

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"time"
)

type dumpTableList struct {
	tableNames string
}

type userGrants struct {
	defaultRoleName string
	privilegesLists string
}

type userInfo struct {
	userName string
	host     string
}

type accessMeta struct {
	dumpStartTime time.Time
	user          *userInfo
	dumpTableList *dumpTableList
	grants        *userGrants
	where         string
	dumpEndTime   time.Time
	output        string
}

func getTableList(conf *Config) *dumpTableList {
	tableStr := ""
	for db, tabs := range conf.Tables {
		if len(tabs) == 0 {
			tableStr += db + " " + "\n"
			continue
		}
		for _, tab := range tabs {
			tableStr += db + "." + tab.Name + " "
		}
		tableStr += "\n"
	}
	return &dumpTableList{tableNames: tableStr}
}

func getSimpleQueryResult(sql string, db *sql.Conn) ([]string, error) {
	var res []string
	rows, err := db.QueryContext(context.Background(), sql)
	if err != nil {
		return nil, err
	}
	defer func() {
		if rows != nil {
			rows.Close()
		}
	}()
	for rows.Next() {
		var rowStr string
		err = rows.Scan(&rowStr)
		if err != nil {
			return res, err
		}
		res = append(res, rowStr)
	}
	return res, rows.Err()
}

func strConcat(str []string, defaultStrng string) string {
	if len(str) == 0 {
		return defaultStrng
	}
	var res string
	for i, s := range str {
		if i < len(str)-1 {
			res += s + "\n"
		} else {
			res += s
		}
	}
	return res
}

func (am *accessMeta) getUserGrants(db *sql.Conn) error {
	defaultRoles, err := getSimpleQueryResult("SELECT CURRENT_ROLE();", db)
	if err != nil {
		return err
	}
	defaultRoleStr := strConcat(defaultRoles, "NONE")

	username, err := getSimpleQueryResult("SELECT USER();", db)
	if err != nil {
		return err
	}
	usernameStr := strConcat(username, "NULL")
	ss := strings.Split(usernameStr, "@")
	if len(ss) != 2 {
		am.user.userName = "NULL"
		am.user.host = "NULL"
	} else {
		am.user.host = ss[1]
	}

	grants, err := getSimpleQueryResult("SHOW GRANTS;", db)
	if err != nil {
		return err
	}
	grantStr := strConcat(grants, "NULL")

	am.grants = &userGrants{
		defaultRoleName: defaultRoleStr,
		privilegesLists: grantStr,
	}
	return nil
}

func (am *accessMeta) setDumpEndTime() {
	am.dumpEndTime = time.Now()
}

func newAccessMeta(conf *Config) *accessMeta {
	user := &userInfo{
		userName: conf.User,
		host:     conf.Host,
	}
	tables := getTableList(conf)
	return &accessMeta{
		user:          user,
		where:         conf.Where,
		dumpTableList: tables,
		output:        conf.OutputDirPath,
		dumpStartTime: time.Now(),
	}
}

func (am *accessMeta) formatPrint() string {
	output := "dump task start time: " + am.dumpStartTime.String() + "\n"

	output += "dump task end time: " + am.dumpEndTime.String() + "\n"

	output += "\nuser info: " + am.user.userName + "@" + am.user.host + "\n"

	output += "role info: " + am.grants.defaultRoleName + "\n"

	output += "privileges info: \n" + am.grants.privilegesLists + "\n"

	output += "\ndump table info: \n" + am.dumpTableList.tableNames + "\n"

	if len(am.where) > 0 {
		output += "dump data conditions: " + am.where + "\n"
	}

	return output
}

func (am *accessMeta) writeAccessMeta() error {
	accessMetaFileName := am.output + "/accessmeta"
	accessMetaStr := am.formatPrint()
	return os.WriteFile(accessMetaFileName, []byte(accessMetaStr), 0644)
}
