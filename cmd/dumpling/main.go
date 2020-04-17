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
	"fmt"
	_ "net/http/pprof"
	"os"
	"time"

	"github.com/pingcap/dumpling/v4/cli"
	"github.com/pingcap/dumpling/v4/export"
	"github.com/spf13/cobra"
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

	escapeBackslash bool

	rootCmd = &cobra.Command{
		Use:   "dumpling",
		Short: "A tool to dump MySQL/TiDB data",
		Long:  `Dumpling is a CLI tool that helps you dump MySQL/TiDB data`,
		Run: func(cmd *cobra.Command, args []string) {
			run()
		},
	}
)

var defaultOutputDir = timestampDirName()

func timestampDirName() string {
	return fmt.Sprintf("./export-%s", time.Now().Format(time.RFC3339))
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&database, "database", "B", "", "Database to dump")
	rootCmd.PersistentFlags().StringVarP(&host, "host", "H", "127.0.0.1", "The host to connect to")
	rootCmd.PersistentFlags().StringVarP(&user, "user", "u", "root", "Username with privileges to run the dump")
	rootCmd.PersistentFlags().IntVarP(&port, "port", "P", 4000, "TCP/IP port to connect to")
	rootCmd.PersistentFlags().StringVarP(&password, "password", "p", "", "User password")
	rootCmd.PersistentFlags().IntVarP(&threads, "threads", "t", 4, "Number of goroutines to use, default 4")
	rootCmd.PersistentFlags().Uint64VarP(&fileSize, "filesize", "F", export.UnspecifiedSize, "The approximate size of output file")
	rootCmd.PersistentFlags().Uint64VarP(&statementSize, "statement-size", "S", export.UnspecifiedSize, "Attempted size of INSERT statement in bytes")
	rootCmd.PersistentFlags().StringVarP(&outputDir, "output", "o", defaultOutputDir, "Output directory")
	rootCmd.PersistentFlags().StringVar(&logLevel, "loglevel", "info", "Log level: {debug|info|warn|error|dpanic|panic|fatal}")
	rootCmd.PersistentFlags().StringVar(&consistency, "consistency", "auto", "Consistency level during dumping: {auto|none|flush|lock|snapshot}")
	rootCmd.PersistentFlags().StringVar(&snapshot, "snapshot", "", "Snapshot position. Valid only when consistency=snapshot")
	rootCmd.PersistentFlags().BoolVarP(&noViews, "no-views", "W", true, "Do not dump views")
	rootCmd.PersistentFlags().StringVar(&statusAddr, "status-addr", ":8281", "dumpling API server and pprof addr")
	rootCmd.PersistentFlags().Uint64VarP(&rows, "rows", "r", export.UnspecifiedSize, "Split table into chunks of this many rows, default unlimited")
	rootCmd.PersistentFlags().StringVar(&where, "where", "", "Dump only selected records")
	rootCmd.PersistentFlags().BoolVar(&escapeBackslash, "escape-backslash", true, "use backslash to escape quotation marks")
	rootCmd.PersistentFlags().StringVar(&fileType, "filetype", "sql", "The type of export file (sql/csv)")
	rootCmd.PersistentFlags().BoolVar(&noHeader, "no-header", false, "whether not to dump CSV table header")
	rootCmd.PersistentFlags().BoolVarP(&noSchemas, "no-schemas", "m", false, "Do not dump table schemas with the data")
	rootCmd.PersistentFlags().BoolVarP(&noData, "no-data", "d", false, "Do not dump table data")
}

func run() {
	println(cli.LongVersion())

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
	conf.FileType = fileType
	conf.NoHeader = noHeader
	conf.NoSchemas = noSchemas
	conf.NoData = noData

	err := export.Dump(conf)
	if err != nil {
		fmt.Printf("dump failed: %s\n", err.Error())
		os.Exit(1)
	}
	return
}

func main() {
	rootCmd.Execute()
}
