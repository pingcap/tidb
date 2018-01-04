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
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"strconv"

	"github.com/spf13/cobra"
)

var (
	configPrefix      = "pd/api/v1/config"
	schedulePrefix    = "pd/api/v1/config/schedule"
	replicationPrefix = "pd/api/v1/config/replicate"
	namespacePrefix   = "pd/api/v1/config/namespace"
)

// NewConfigCommand return a config subcommand of rootCmd
func NewConfigCommand() *cobra.Command {
	conf := &cobra.Command{
		Use:   "config <subcommand>",
		Short: "tune pd configs",
	}
	conf.AddCommand(NewShowConfigCommand())
	conf.AddCommand(NewSetConfigCommand())
	conf.AddCommand(NewDeleteConfigCommand())
	return conf
}

// NewShowConfigCommand return a show subcommand of configCmd
func NewShowConfigCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "show [namespace|replication|all]",
		Short: "show schedule config of PD",
		Run:   showConfigCommandFunc,
	}
	sc.AddCommand(NewShowAllConfigCommand())
	sc.AddCommand(NewShowNamespaceConfigCommand())
	sc.AddCommand(NewShowReplicationConfigCommand())
	return sc
}

// NewShowNamespaceConfigCommand return a show all subcommand of show subcommand
func NewShowNamespaceConfigCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "namespace <name>",
		Short: "show namespace config of PD",
		Run:   showNamespaceConfigCommandFunc,
	}
	return sc
}

// NewShowAllConfigCommand return a show all subcommand of show subcommand
func NewShowAllConfigCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "all",
		Short: "show all config of PD",
		Run:   showAllConfigCommandFunc,
	}
	return sc
}

// NewShowReplicationConfigCommand return a show all subcommand of show subcommand
func NewShowReplicationConfigCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "replication",
		Short: "show replication config of PD",
		Run:   showReplicationConfigCommandFunc,
	}
	return sc
}

// NewSetConfigCommand return a set subcommand of configCmd
func NewSetConfigCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "set [namespace <name>] <option> <value>",
		Short: "set the option with value",
		Run:   setConfigCommandFunc,
	}
	sc.AddCommand(NewSetNamespaceConfigCommand())
	return sc
}

// NewSetNamespaceConfigCommand a set subcommand of set subcommand
func NewSetNamespaceConfigCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "namespace <name> <option> <value>",
		Short: "set the namespace config's option with value",
		Run:   setNamespaceConfigCommandFunc,
	}
	return sc
}

// NewDeleteConfigCommand a set subcommand of cfgCmd
func NewDeleteConfigCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "delete namespace <name> [<option>]",
		Short: "delete the config option",
	}
	sc.AddCommand(NewDeleteNamespaceConfigCommand())
	return sc
}

// NewDeleteNamespaceConfigCommand a set subcommand of delete subcommand
func NewDeleteNamespaceConfigCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "namespace <name> [<option>]",
		Short: "delete the namespace config's all options or given option",
		Run:   deleteNamespaceConfigCommandFunc,
	}
	return sc
}

func showConfigCommandFunc(cmd *cobra.Command, args []string) {
	r, err := doRequest(cmd, schedulePrefix, http.MethodGet)
	if err != nil {
		fmt.Printf("Failed to get config: %s\n", err)
		return
	}
	fmt.Println(r)
}

func showReplicationConfigCommandFunc(cmd *cobra.Command, args []string) {
	r, err := doRequest(cmd, replicationPrefix, http.MethodGet)
	if err != nil {
		fmt.Printf("Failed to get config: %s\n", err)
		return
	}
	fmt.Println(r)
}

func showAllConfigCommandFunc(cmd *cobra.Command, args []string) {
	r, err := doRequest(cmd, configPrefix, http.MethodGet)
	if err != nil {
		fmt.Printf("Failed to get config: %s\n", err)
		return
	}
	fmt.Println(r)
}

func showNamespaceConfigCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		fmt.Println(cmd.UsageString())
		return
	}
	prefix := path.Join(namespacePrefix, args[0])
	r, err := doRequest(cmd, prefix, http.MethodGet)
	if err != nil {
		fmt.Printf("Failed to get config: %s\n", err)
		return
	}
	fmt.Println(r)
}

func postConfigDataWithPath(cmd *cobra.Command, key, value, path string) error {
	var val interface{}
	data := make(map[string]interface{})
	val, err := strconv.ParseFloat(value, 64)
	if err != nil {
		val = value
	}
	data[key] = val
	reqData, err := json.Marshal(data)
	req, err := getRequest(cmd, path, http.MethodPost, "application/json", bytes.NewBuffer(reqData))
	if err != nil {
		return err
	}
	_, err = dail(req)
	if err != nil {
		return err
	}
	return nil
}

func setConfigCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		fmt.Println(cmd.UsageString())
		return
	}
	opt, val := args[0], args[1]
	err := postConfigDataWithPath(cmd, opt, val, configPrefix)
	if err != nil {
		fmt.Printf("Failed to set config: %s\n", err)
		return
	}
	fmt.Println("Success!")
}

func setNamespaceConfigCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 3 {
		fmt.Println(cmd.UsageString())
		return
	}
	name, opt, val := args[0], args[1], args[2]
	prefix := path.Join(namespacePrefix, name)
	err := postConfigDataWithPath(cmd, opt, val, prefix)
	if err != nil {
		fmt.Printf("Failed to set namespace:%s config: %s\n", name, err)
		return
	}
	fmt.Println("Success!")
}

func deleteNamespaceConfigCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 && len(args) != 2 {
		fmt.Println(cmd.UsageString())
		return
	}
	name, opt := args[0], args[1]
	prefix := path.Join(namespacePrefix, name)

	var err error
	if len(args) == 2 {
		// delete namespace config's option by setting the option with zero value
		err = postConfigDataWithPath(cmd, opt, "0", prefix)
	} else {
		_, err = doRequest(cmd, prefix, http.MethodDelete)
	}

	if err != nil {
		fmt.Printf("Failed to delete namespace:%s config %s: %s\n", name, opt, err)
		return
	}
	fmt.Println("Success!")
}
