// Copyright 2015 PingCAP, Inc.
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

package printer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"runtime"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/israce"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/versioninfo"
	"go.uber.org/zap"
)

var buildVersion string

func init() {
	buildVersion = runtime.Version()
}

// PrintTiDBInfo prints the TiDB version information.
func PrintTiDBInfo() {
	fields := []zap.Field{
		zap.String("Release Version", mysql.TiDBReleaseVersion),
		zap.String("Edition", versioninfo.TiDBEdition),
		zap.String("Git Commit Hash", versioninfo.TiDBGitHash),
		zap.String("Git Branch", versioninfo.TiDBGitBranch),
		zap.String("UTC Build Time", versioninfo.TiDBBuildTS),
		zap.String("GoVersion", buildVersion),
		zap.Bool("Race Enabled", israce.RaceEnabled),
		zap.Bool("Check Table Before Drop", config.CheckTableBeforeDrop),
	}
	if versioninfo.TiDBEnterpriseExtensionGitHash != "" {
		fields = append(fields, zap.String("Enterprise Extension Commit Hash", versioninfo.TiDBEnterpriseExtensionGitHash))
	}
	logutil.BgLogger().Info("Welcome to TiDB.", fields...)
	configJSON, err := json.Marshal(config.GetGlobalConfig())
	if err != nil {
		panic(err)
	}
	logutil.BgLogger().Info("loaded config", zap.ByteString("config", configJSON))
}

// GetTiDBInfo returns the git hash and build time of this tidb-server binary.
func GetTiDBInfo() string {
	enterpriseVersion := ""
	if versioninfo.TiDBEnterpriseExtensionGitHash != "" {
		enterpriseVersion = fmt.Sprintf("\nEnterprise Extension Commit Hash: %s", versioninfo.TiDBEnterpriseExtensionGitHash)
	}
	return fmt.Sprintf("Release Version: %s\n"+
		"Edition: %s\n"+
		"Git Commit Hash: %s\n"+
		"Git Branch: %s\n"+
		"UTC Build Time: %s\n"+
		"GoVersion: %s\n"+
		"Race Enabled: %v\n"+
		"Check Table Before Drop: %v\n"+
		"Store: %s"+
		"%s",
		mysql.TiDBReleaseVersion,
		versioninfo.TiDBEdition,
		versioninfo.TiDBGitHash,
		versioninfo.TiDBGitBranch,
		versioninfo.TiDBBuildTS,
		buildVersion,
		israce.RaceEnabled,
		config.CheckTableBeforeDrop,
		config.GetGlobalConfig().Store,
		enterpriseVersion,
	)
}

// checkValidity checks whether cols and every data have the same length.
func checkValidity(cols []string, datas [][]string) bool {
	colLen := len(cols)
	if len(datas) == 0 || colLen == 0 {
		return false
	}

	for _, data := range datas {
		if colLen != len(data) {
			return false
		}
	}

	return true
}

func getMaxColLen(cols []string, datas [][]string) []int {
	maxColLen := make([]int, len(cols))
	for i, col := range cols {
		maxColLen[i] = len(col)
	}

	for _, data := range datas {
		for i, v := range data {
			if len(v) > maxColLen[i] {
				maxColLen[i] = len(v)
			}
		}
	}

	return maxColLen
}

func getPrintDivLine(maxColLen []int) []byte {
	var value = make([]byte, 0)
	for _, v := range maxColLen {
		value = append(value, '+')
		value = append(value, bytes.Repeat([]byte{'-'}, v+2)...)
	}
	value = append(value, '+')
	value = append(value, '\n')
	return value
}

func getPrintCol(cols []string, maxColLen []int) []byte {
	var value = make([]byte, 0)
	for i, v := range cols {
		value = append(value, '|')
		value = append(value, ' ')
		value = append(value, []byte(v)...)
		value = append(value, bytes.Repeat([]byte{' '}, maxColLen[i]+1-len(v))...)
	}
	value = append(value, '|')
	value = append(value, '\n')
	return value
}

func getPrintRow(data []string, maxColLen []int) []byte {
	var value = make([]byte, 0)
	for i, v := range data {
		value = append(value, '|')
		value = append(value, ' ')
		value = append(value, []byte(v)...)
		value = append(value, bytes.Repeat([]byte{' '}, maxColLen[i]+1-len(v))...)
	}
	value = append(value, '|')
	value = append(value, '\n')
	return value
}

func getPrintRows(datas [][]string, maxColLen []int) []byte {
	var value = make([]byte, 0)
	for _, data := range datas {
		value = append(value, getPrintRow(data, maxColLen)...)
	}
	return value
}

// GetPrintResult gets a result with a formatted string.
func GetPrintResult(cols []string, datas [][]string) (string, bool) {
	if !checkValidity(cols, datas) {
		return "", false
	}

	var value = make([]byte, 0)
	maxColLen := getMaxColLen(cols, datas)

	value = append(value, getPrintDivLine(maxColLen)...)
	value = append(value, getPrintCol(cols, maxColLen)...)
	value = append(value, getPrintDivLine(maxColLen)...)
	value = append(value, getPrintRows(datas, maxColLen)...)
	value = append(value, getPrintDivLine(maxColLen)...)
	return string(value), true
}
