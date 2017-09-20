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

	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/spf13/cobra"
)

type tableInfo struct {
	Name string `json:"name"`
	ID   int64  `json:"id"`
}

func (t tableInfo) String() string {
	return fmt.Sprintf("tableInfo{name: %s, id: %d}", t.Name, t.ID)
}

// Filter is the higher-order function to manipulate collections
// TODO move to public function package
func Filter(vs []string, f func(string) bool) []string {
	vsf := make([]string, 0)
	for _, v := range vs {
		if f(v) {
			vsf = append(vsf, v)
		}
	}
	return vsf
}

// NewTableIDCommand return the tableID of the given tableName
func NewTableIDCommand() *cobra.Command {
	m := &cobra.Command{
		Use:   "showTableID <db_name> <table_name>",
		Short: "show the table id given the table name",
		Run:   showTableIDCommandFunc,
	}

	// Add flags for command `showTableID`
	m.Flags().BoolP("verbose", "v", false, "Print tableInfo in verbose mode")

	return m
}

func showTableIDCommandFunc(cmd *cobra.Command, args []string) {
	args = Filter(args, func(a string) bool { return a != "" })
	if len(args) != 2 {
		fmt.Println(NewTableIDCommand().UsageString())
		return

	}
	host, err := cmd.Flags().GetString("tidb")
	if err != nil {
		fmt.Println("Fail to get tidb address from cmd flags", cmd.Flags())
	}
	dbName := args[0]
	tableName := args[1]

	if host == "" || dbName == "" || tableName == "" {
		fmt.Printf("host, dbName and tableName are all required, but now\n "+
			"host: %s\n dbName: %s\n tableName: %s\n", host, dbName, tableName)
		return
	}

	urlString := fmt.Sprintf("%s/tables/%s/%s/regions", host, dbName, tableName)

	verbose, err := cmd.Flags().GetBool("verbose")
	if err != nil {
		verbose = false
	}
	if verbose {
		fmt.Println("the url is", urlString)
	}

	resp, err := http.Get(urlString)

	if err != nil {
		fmt.Println(err)
		return
	}

	defer resp.Body.Close()

	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
		return
	}

	var ti tableInfo
	err = json.Unmarshal(content, &ti)
	if err != nil {
		fmt.Println("Error when unmarshaling the response body", string(content))
		fmt.Println("The error is", err)
		return
	}
	if verbose {
		fmt.Println("TableInfo", ti)
	}
	fmt.Println("TableID: ", ti.ID)

}
