// Copyright 2016 PingCAP, Inc.
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
	"net/http"
	"path"
	"strconv"

	"github.com/spf13/cobra"
)

var (
	storesPrefix = "pd/api/v1/stores"
	storePrefix  = "pd/api/v1/store/%s"
)

// NewStoreCommand return a store subcommand of rootCmd
func NewStoreCommand() *cobra.Command {
	s := &cobra.Command{
		Use:   "store [delete|label|weight] <store_id>",
		Short: "show the store status",
		Run:   showStoreCommandFunc,
	}
	s.AddCommand(NewDeleteStoreCommand())
	s.AddCommand(NewLabelStoreCommand())
	s.AddCommand(NewSetStoreWeightCommand())
	return s
}

// NewDeleteStoreCommand return a  delete subcommand of storeCmd
func NewDeleteStoreCommand() *cobra.Command {
	d := &cobra.Command{
		Use:   "delete <store_id>",
		Short: "delete the store",
		Run:   deleteStoreCommandFunc,
	}
	return d
}

// NewLabelStoreCommand returns a label subcommand of storeCmd.
func NewLabelStoreCommand() *cobra.Command {
	l := &cobra.Command{
		Use:   "label <store_id> <key> <value>",
		Short: "set a store's label value",
		Run:   labelStoreCommandFunc,
	}
	return l
}

// NewSetStoreWeightCommand returns a weight subcommand of storeCmd.
func NewSetStoreWeightCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "weight <store_id> <leader_weight> <region_weight>",
		Short: "set a store's leader and region balance weight",
		Run:   setStoreWeightCommandFunc,
	}
}

func showStoreCommandFunc(cmd *cobra.Command, args []string) {
	var prefix string
	prefix = storesPrefix
	if len(args) == 1 {
		if _, err := strconv.Atoi(args[0]); err != nil {
			fmt.Println("store_id should be a number")
			return
		}
		prefix = fmt.Sprintf(storePrefix, args[0])
	}
	r, err := doRequest(cmd, prefix, http.MethodGet)
	if err != nil {
		fmt.Printf("Failed to get store: %s\n", err)
		return
	}
	fmt.Println(r)
}

func deleteStoreCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		fmt.Println("Usage: store delete <store_id>")
		return
	}
	if _, err := strconv.Atoi(args[0]); err != nil {
		fmt.Println("store_id should be a number")
		return
	}
	prefix := fmt.Sprintf(storePrefix, args[0])
	_, err := doRequest(cmd, prefix, http.MethodDelete)
	if err != nil {
		fmt.Printf("Failed to delete store %s: %s\n", args[0], err)
		return
	}
	fmt.Println("Success!")
}

func labelStoreCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 3 {
		fmt.Println("Usage: store label <store_id> <key> <value>")
		return
	}
	if _, err := strconv.Atoi(args[0]); err != nil {
		fmt.Println("store_id should be a number")
		return
	}
	prefix := fmt.Sprintf(path.Join(storePrefix, "label"), args[0])
	postJSON(cmd, prefix, map[string]interface{}{args[1]: args[2]})
}

func setStoreWeightCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 3 {
		fmt.Println("Usage: store weight <store_id> <leader_weight> <region_weight>")
		return
	}
	leader, err := strconv.ParseFloat(args[1], 64)
	if err != nil || leader < 0 {
		fmt.Println("leader_weight should be a number that >= 0.")
		return
	}
	region, err := strconv.ParseFloat(args[2], 64)
	if err != nil || region < 0 {
		fmt.Println("region_weight should be a number that >= 0")
		return
	}
	prefix := fmt.Sprintf(path.Join(storePrefix, "weight"), args[0])
	postJSON(cmd, prefix, map[string]interface{}{
		"leader": leader,
		"region": region,
	})
}
