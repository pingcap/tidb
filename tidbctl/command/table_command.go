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

package command

import (
	"fmt"

	"github.com/spf13/cobra"
)

// NewTableCommand is the entrance of table command
func NewTableCommand() *cobra.Command {
	m := &cobra.Command{
		Use:   "table <sub_command> <db_name> <table_name>",
		Short: "show the table information given the table name",
		// Run:   showTableIDCommandFunc,
	}

	m.AddCommand(NewTableIDCommand())
	m.AddCommand(NewTableStoreCommand())

	return m
}

// NewTableIDCommand return the tableID of the given tableName
func NewTableIDCommand() *cobra.Command {
	m := &cobra.Command{
		Use:   "showID <db_name> <table_name>",
		Short: "show the table id given the table name",
		Run:   showTableIDCommandFunc,
	}
	// Add flags for command `showID`
	m.Flags().BoolP("verbose", "v", false, "Print tableInfo in verbose mode")

	return m
}

func showTableIDCommandFunc(cmd *cobra.Command, args []string) {
	args = Filter(args, func(a string) bool { return a != "" })
	if len(args) != 2 {
		fmt.Println(NewTableIDCommand().UsageString())
		return

	}
	dbName := args[0]
	tableName := args[1]

	host, err := cmd.Flags().GetString("tidb")
	if err != nil {
		fmt.Println("Fail to get tidb address from cmd flags", cmd.Flags())
	}

	verbose, err := cmd.Flags().GetBool("verbose")
	if err != nil {
		verbose = false
	}

	ti, err := getTableInfo(host, dbName, tableName, verbose)
	if err != nil {
		fmt.Printf("Failed to get tableInfo via host: %s, db: %s, table: %s.\nThe error is %s\n",
			host, dbName, tableName, err)
		return
	}

	if verbose {
		fmt.Println("TableInfo", ti)
	}
	fmt.Println("TableID: ", ti.ID)

}

// NewTableStoreCommand returns all the stores containing the given table
func NewTableStoreCommand() *cobra.Command {
	m := &cobra.Command{
		Use:   "showStore <db_name> <table_name>",
		Short: "show the table stores given the table name",
		Run:   showTableStoreCommandFunc,
	}
	// Add flags for command `showStore`
	m.Flags().BoolP("verbose", "v", false, "Print tableInfo in verbose mode")

	return m
}

func showTableStoreCommandFunc(cmd *cobra.Command, args []string) {
	args = Filter(args, func(a string) bool { return a != "" })
	if len(args) != 2 {
		fmt.Println(NewTableIDCommand().UsageString())
		return

	}
	dbName := args[0]
	tableName := args[1]

	host, err := cmd.Flags().GetString("tidb")
	if err != nil {
		fmt.Println("Fail to get tidb address from cmd flags", cmd.Flags())
	}

	verbose, err := cmd.Flags().GetBool("verbose")
	if err != nil {
		fmt.Println("fail to get verbose flag, set to false. error is", err)
		verbose = false
	}

	ti, err := getTableInfo(host, dbName, tableName, verbose)
	if err != nil {
		fmt.Printf("Failed to get tableInfo via host: %s, db: %s, table: %s.\nThe error is %s",
			host, dbName, tableName, err)
		return
	}

	if verbose {
		fmt.Println("TableInfo", ti)
	}

	storeMap := make(map[int64]int)
	for _, region := range ti.RecordRegions {
		for _, peer := range region.Peers {
			storeMap[peer.StoreID]++
		}
	}

	fmt.Println("Stores and corresponding peer counts are:")
	for k := range storeMap {
		fmt.Printf("StoreID: %d\t peer count: %d\n", k, storeMap[k])
	}

}
