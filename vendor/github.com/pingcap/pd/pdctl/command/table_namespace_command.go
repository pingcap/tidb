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
	"net/http"
	"strconv"

	"github.com/spf13/cobra"
)

const (
	namespacesPrefix     = "pd/api/v1/classifier/table/namespaces"
	namespaceTablePrefix = "pd/api/v1/classifier/table/namespaces/table"
	namespaceMetaPrefix  = "pd/api/v1/classifier/table/namespaces/meta"
	storeNsPrefix        = "pd/api/v1/classifier/table/store_ns/%s"
)

// NewTableNamespaceCommand return a table namespace sub-command of rootCmd
func NewTableNamespaceCommand() *cobra.Command {
	s := &cobra.Command{
		Use:   "table_ns [create|add|remove|set_store|rm_store|set_meta|rm_meta]",
		Short: "show the table namespace information",
		Run:   showNamespaceCommandFunc,
	}
	s.AddCommand(NewCreateNamespaceCommand())
	s.AddCommand(NewAddTableIDCommand())
	s.AddCommand(NewRemoveTableIDCommand())
	s.AddCommand(NewSetNamespaceStoreCommand())
	s.AddCommand(NewRemoveNamespaceStoreCommand())
	s.AddCommand(newSetMetaNamespaceCommand())
	s.AddCommand(newRemoveMetaNamespaceCommand())
	return s
}

// NewCreateNamespaceCommand returns a create sub-command of namespaceCmd
func NewCreateNamespaceCommand() *cobra.Command {
	d := &cobra.Command{
		Use:   "create <namespace>",
		Short: "create namespace",
		Run:   createNamespaceCommandFunc,
	}
	return d
}

// NewAddTableIDCommand returns a add sub-command of namespaceCmd
func NewAddTableIDCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "add <name> <table_id>",
		Short: "add table id to namespace",
		Run:   addTableCommandFunc,
	}
	return c
}

// NewRemoveTableIDCommand returns a remove sub-command of namespaceCmd
func NewRemoveTableIDCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "remove <name> <table_id>",
		Short: "remove table id from namespace",
		Run:   removeTableCommandFunc,
	}
	return c
}

func showNamespaceCommandFunc(cmd *cobra.Command, args []string) {
	r, err := doRequest(cmd, namespacesPrefix, http.MethodGet)
	if err != nil {
		fmt.Printf("Failed to get the namespace information: %s\n", err)
		return
	}
	fmt.Println(r)
}

func createNamespaceCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		fmt.Println("Usage: namespace create <name>")
		return
	}

	input := map[string]interface{}{
		"namespace": args[0],
	}

	postJSON(cmd, namespacesPrefix, input)
}

func addTableCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		fmt.Println("Usage: namespace add <name> <table_id>")
		return
	}
	if _, err := strconv.Atoi(args[1]); err != nil {
		fmt.Println("table_id shoud be a number")
		return
	}

	input := map[string]interface{}{
		"namespace": args[0],
		"table_id":  args[1],
		"action":    "add",
	}

	postJSON(cmd, namespaceTablePrefix, input)
}

func removeTableCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		fmt.Println("Usage: namespace remove <name> <table_id>")
		return
	}
	if _, err := strconv.Atoi(args[1]); err != nil {
		fmt.Println("table_id shoud be a number")
		return
	}

	input := map[string]interface{}{
		"namespace": args[0],
		"table_id":  args[1],
		"action":    "remove",
	}

	postJSON(cmd, namespaceTablePrefix, input)
}

// NewSetNamespaceStoreCommand returns a set subcommand of storeNsCmd.
func NewSetNamespaceStoreCommand() *cobra.Command {
	n := &cobra.Command{
		Use:   "set_store <store_id> <namespace>",
		Short: "set namespace to store",
		Run:   setNamespaceStoreCommandFunc,
	}

	return n
}

// NewRemoveNamespaceStoreCommand returns a rm subcommand of storeNsCmd.
func NewRemoveNamespaceStoreCommand() *cobra.Command {
	n := &cobra.Command{
		Use:   "rm_store <store_id> <namespace>",
		Short: "remove namespace from store",
		Run:   removeNamespaceStoreCommandFunc,
	}

	return n
}

func setNamespaceStoreCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		fmt.Println("Usage: namespace set_ns <store_id> <namespace>")
		return
	}
	_, err := strconv.Atoi(args[0])
	if err != nil {
		fmt.Println("store_id should be a number")
		return
	}
	prefix := fmt.Sprintf(storeNsPrefix, args[0])
	postJSON(cmd, prefix, map[string]interface{}{
		"namespace": args[1],
		"action":    "add",
	})
}

func removeNamespaceStoreCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		fmt.Println("Usage: namespace rm_ns <store_id> <namespace>")
		return
	}
	_, err := strconv.Atoi(args[0])
	if err != nil {
		fmt.Println("store_id should be a number")
		return
	}
	prefix := fmt.Sprintf(storeNsPrefix, args[0])
	postJSON(cmd, prefix, map[string]interface{}{
		"namespace": args[1],
		"action":    "remove",
	})
}

func newSetMetaNamespaceCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "set_meta <namespace>",
		Short: "set meta to namespace",
		Run:   setMetaNamespaceCommandFunc,
	}
}

func newRemoveMetaNamespaceCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "rm_meta <namespace>",
		Short: "remove meta from namespace",
		Run:   removeMetaNamespaceCommandFunc,
	}
}

func setMetaNamespaceCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		fmt.Println("Usage: set_meta <namespace>")
		return
	}
	input := map[string]interface{}{
		"namespace": args[0],
		"action":    "add",
	}
	postJSON(cmd, namespaceMetaPrefix, input)
}

func removeMetaNamespaceCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		fmt.Println("Usage: rm_meta <namespace>")
		return
	}
	input := map[string]interface{}{
		"namespace": args[0],
		"action":    "remove",
	}
	postJSON(cmd, namespaceMetaPrefix, input)
}
