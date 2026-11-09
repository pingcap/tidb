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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestStrConcat(t *testing.T) {
	var strs []string
	require.Equal(t, strConcat(strs, "NULL"), "NULL")
	require.Equal(t, strConcat(strs, "NONE"), "NONE")
	strs = append(strs, "aaa")
	strs = append(strs, "bbb")
	require.Equal(t, strConcat(strs, "NONE"), "aaa\nbbb")
}

func TestGetTableList(t *testing.T) {
	conf := &Config{}
	conf.Tables = make(map[databaseName][]*TableInfo)
	dtl := getTableList(conf)
	dst := &dumpTableList{tableNames: ""}
	require.Equal(t, dtl.tableNames, dst.tableNames)
	ts := make([]*TableInfo, 0, 10)
	t1 := &TableInfo{
		Name:         "aaa",
		AvgRowLength: 100,
		Type:         1,
	}
	t2 := &TableInfo{
		Name:         "bbb",
		AvgRowLength: 100,
		Type:         0,
	}
	conf.Tables["test"] = ts
	dtl = getTableList(conf)
	dst = &dumpTableList{tableNames: "test \n"}
	require.Equal(t, dtl.tableNames, dst.tableNames)
	ts = append(ts, t1)
	conf.Tables["test"] = ts
	dtl = getTableList(conf)
	dst = &dumpTableList{tableNames: "test.aaa \n"}
	require.Equal(t, dtl.tableNames, dst.tableNames)
	ts = append(ts, t2)
	conf.Tables["test"] = ts
	dtl = getTableList(conf)
	dst = &dumpTableList{tableNames: "test.aaa test.bbb \n"}
	require.Equal(t, dtl.tableNames, dst.tableNames)
}

func newConf() *Config {
	conf := &Config{
		User:          "root",
		Host:          "127.0.0.1",
		Where:         "where id>10;",
		Tables:        make(map[databaseName][]*TableInfo),
		OutputDirPath: "/data1/tst/dump-data-file",
	}
	ts := make([]*TableInfo, 0, 10)
	t1 := &TableInfo{
		Name:         "aaa",
		AvgRowLength: 100,
		Type:         1,
	}
	t2 := &TableInfo{
		Name:         "bbb",
		AvgRowLength: 100,
		Type:         0,
	}
	ts = append(ts, t1, t2)
	conf.Tables["test"] = ts
	return conf
}

func TestFormatPrint(t *testing.T) {
	conf := newConf()
	am := newAccessMeta(conf, nil)
	am.grants = &userGrants{
		defaultRoleName: "NONE",
		privilegesLists: "grant all privileges on *.* to root@'127.0.0.1' with grant option",
	}
	fmtStr := am.formatPrint()

	expStr := "user info: root@127.0.0.1\nrole info: NONE\nprivileges info:\ngrant all privileges on *.* to root@'127.0.0.1' with grant option\n\ndump table info:\ntest.aaa test.bbb \n\ndump data conditions: where id>10;\n"

	require.True(t, strings.Contains(strings.ReplaceAll(fmtStr, " ", ""), strings.ReplaceAll(expStr, " ", "")))
}

func TestFormatPrintWithoutRoleInfo(t *testing.T) {
	conf := newConf()
	am := newAccessMeta(conf, nil)
	am.grants = &userGrants{
		privilegesLists: "grant all privileges on *.* to root@'127.0.0.1' with grant option",
	}
	fmtStr := am.formatPrint()

	require.NotContains(t, fmtStr, "role info:")
	require.Contains(t, fmtStr, "privileges info:")
}

func TestWriteAccessMeta(t *testing.T) {
	conf := newConf()
	tempDir := t.TempDir()

	extStore, err := storage.NewLocalStorage(tempDir)
	require.NoError(t, err)
	t.Cleanup(extStore.Close)

	am := newAccessMeta(conf, extStore)
	am.grants = &userGrants{
		defaultRoleName: "NONE",
		privilegesLists: "grant all privileges on *.* to root@'127.0.0.1' with grant option",
	}
	am.setDumpEndTime()

	err = am.writeAccessMeta(context.Background())
	require.NoError(t, err)

	metaPath := filepath.Join(tempDir, "accessmeta")
	require.FileExists(t, metaPath)

	content, err := os.ReadFile(metaPath)
	require.NoError(t, err)
	require.NotEmpty(t, content)
	require.Contains(t, string(content), "user info: root@127.0.0.1")
}
