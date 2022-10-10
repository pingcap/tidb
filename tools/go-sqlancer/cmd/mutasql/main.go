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

package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/sqlancer"
)

var (
	// TODO: clean these items
	conf      = sqlancer.NewConfig()
	dsn       = flag.String("dsn", "", "dsn of target db for testing")
	duration  = flag.Duration("duration", 5*time.Hour, "fuzz duration")
	silent    = flag.Bool("silent", false, "silent when verify failed")
	logLevel  = flag.String("log-level", "info", "set log level: info, warn, error, debug [default: info]")
	depth     = flag.Int("depth", 1, "sql depth")
	viewCount = flag.Int("view-count", 10, "count of views to be created")
	hint      = flag.Bool("enable-hint", false, "enable sql hint for TiDB")
	exprIdx   = flag.Bool("enable-expr-idx", false, "enable create expression index")
)

func main() {
	loadConfig()
	mutasql, err := sqlancer.NewMutaSQL(conf)
	if err != nil {
		panic(fmt.Sprintf("new mutasql failed, error: %+v\n", err))
	}

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()
	mutasql.Start(ctx)
}

func loadConfig() {
	flag.Parse()
	if err := conf.SetDSN(*dsn); err != nil {
		panic(err)
	}
	conf.ViewCount = *viewCount
	conf.Depth = *depth
	conf.Silent = *silent
	conf.LogLevel = *logLevel
	conf.EnableHint = *hint
	conf.EnableExprIndex = *exprIdx
}
