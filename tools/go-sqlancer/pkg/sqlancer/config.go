// Copyright 2022 PingCAP, Inc.
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

package sqlancer

import (
	"errors"
	"regexp"
	"strings"
)

var (
	dsnPattern = regexp.MustCompile(`^(\w*?)\:?(\w*?)\@tcp\((\S*?)\:(\d*?)\)\/(.*?)$`)
)

// Config struct
type Config struct {
	DSN      string
	DBName   string
	LogLevel string
	Silent   bool

	Depth           int
	ViewCount       int
	EnableHint      bool
	EnableExprIndex bool

	EnablePQSApproach   bool
	EnableNoRECApproach bool
	EnableTLPApproach   bool
}

// NewConfig create default config
func NewConfig() *Config {
	return &Config{
		DSN:                 "",
		DBName:              "test",
		Silent:              true,
		Depth:               1,
		LogLevel:            "info",
		ViewCount:           10,
		EnableHint:          false,
		EnableExprIndex:     false,
		EnablePQSApproach:   false,
		EnableNoRECApproach: false,
		EnableTLPApproach:   false,
	}
}

// SetDSN set dsn and parse dbname
func (conf *Config) SetDSN(dsn string) error {
	dsn = strings.Trim(dsn, " ")

	dsnMatches := dsnPattern.FindStringSubmatch(dsn)
	if len(dsnMatches) == 6 {
		if dsnMatches[5] == "" {
			conf.DSN = dsn
			conf.DBName = "test"
		} else {
			conf.DSN = strings.TrimRight(dsn, dsnMatches[5])
			conf.DBName = dsnMatches[5]
		}
	} else {
		return errors.New("invalid dsn")
	}
	return nil
}
