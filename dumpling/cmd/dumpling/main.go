// Copyright 2019 PingCAP, Inc.
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

package main

import (
	"errors"
	"fmt"
	_ "net/http/pprof"
	"os"
	"time"

	"github.com/pingcap/dumpling/v4/cli"
	"github.com/pingcap/dumpling/v4/export"
	"github.com/spf13/pflag"
)

var (
	database      string
	host          string
	user          string
	port          int
	password      string
	threads       int
	outputDir     string
	fileSize      uint64
	statementSize uint64
	logLevel      string
	logFile       string
	logFormat     string
	consistency   string
	snapshot      string
	noViews       bool
	statusAddr    string
	rows          uint64
	where         string
	fileType      string
	noHeader      bool
	noSchemas     bool
	noData        bool
	csvNullValue  string
	sql           string

	escapeBackslash bool
)

var defaultOutputDir = timestampDirName()

func timestampDirName() string {
	return fmt.Sprintf("./export-%s", time.Now().Format(time.RFC3339))
}

func main() {
	pflag.Usage = func() {
		fmt.Fprint(os.Stderr, "Dumpling is a CLI tool that helps you dump MySQL/TiDB data\n\nUsage:\n  dumpling [flags]\n\nFlags:\n")
		pflag.PrintDefaults()
	}
	pflag.ErrHelp = errors.New("")

	pflag.StringVarP(&database, "database", "B", "", "Database to dump")
	pflag.StringVarP(&host, "host", "H", "127.0.0.1", "The host to connect to")
	pflag.StringVarP(&user, "user", "u", "root", "Username with privileges to run the dump")
	pflag.IntVarP(&port, "port", "P", 4000, "TCP/IP port to connect to")
	pflag.StringVarP(&password, "password", "p", "", "User password")
	pflag.IntVarP(&threads, "threads", "t", 4, "Number of goroutines to use, default 4")
	pflag.Uint64VarP(&fileSize, "filesize", "F", export.UnspecifiedSize, "The approximate size of output file")
	pflag.Uint64VarP(&statementSize, "statement-size", "S", export.UnspecifiedSize, "Attempted size of INSERT statement in bytes")
	pflag.StringVarP(&outputDir, "output", "o", defaultOutputDir, "Output directory")
	pflag.StringVar(&logLevel, "loglevel", "info", "Log level: {debug|info|warn|error|dpanic|panic|fatal}")
	pflag.StringVarP(&logFile, "logfile", "L", "", "Log file `path`, leave empty to write to console")
	pflag.StringVar(&logFormat, "logfmt", "text", "Log `format`: {text|json}")
	pflag.StringVar(&consistency, "consistency", "auto", "Consistency level during dumping: {auto|none|flush|lock|snapshot}")
	pflag.StringVar(&snapshot, "snapshot", "", "Snapshot position. Valid only when consistency=snapshot")
	pflag.BoolVarP(&noViews, "no-views", "W", true, "Do not dump views")
	pflag.StringVar(&statusAddr, "status-addr", ":8281", "dumpling API server and pprof addr")
	pflag.Uint64VarP(&rows, "rows", "r", export.UnspecifiedSize, "Split table into chunks of this many rows, default unlimited")
	pflag.StringVar(&where, "where", "", "Dump only selected records")
	pflag.BoolVar(&escapeBackslash, "escape-backslash", true, "use backslash to escape quotation marks")
	pflag.StringVar(&fileType, "filetype", "sql", "The type of export file (sql/csv)")
	pflag.BoolVar(&noHeader, "no-header", false, "whether not to dump CSV table header")
	pflag.BoolVarP(&noSchemas, "no-schemas", "m", false, "Do not dump table schemas with the data")
	pflag.BoolVarP(&noData, "no-data", "d", false, "Do not dump table data")
	pflag.StringVar(&csvNullValue, "csv-null-value", "\\N", "The null value used when export to csv")
	pflag.StringVarP(&sql, "sql", "s", "", "Dump data with given sql")

	printVersion := pflag.BoolP("version", "V", false, "Print Dumpling version")

	pflag.Parse()

	println(cli.LongVersion())

	if *printVersion {
		return
	}

	conf := export.DefaultConfig()
	conf.Database = database
	conf.Host = host
	conf.User = user
	conf.Port = port
	conf.Password = password
	conf.Threads = threads
	conf.FileSize = fileSize
	conf.StatementSize = statementSize
	conf.OutputDirPath = outputDir
	conf.Consistency = consistency
	conf.NoViews = noViews
	conf.StatusAddr = statusAddr
	conf.Rows = rows
	conf.Where = where
	conf.EscapeBackslash = escapeBackslash
	conf.LogLevel = logLevel
	conf.LogFile = logFile
	conf.LogFormat = logFormat
	conf.FileType = fileType
	conf.NoHeader = noHeader
	conf.NoSchemas = noSchemas
	conf.NoData = noData
	conf.CsvNullValue = csvNullValue
	conf.Sql = sql

	err := export.Dump(conf)
	if err != nil {
		fmt.Printf("dump failed: %s\n", err.Error())
		os.Exit(1)
	}
}
