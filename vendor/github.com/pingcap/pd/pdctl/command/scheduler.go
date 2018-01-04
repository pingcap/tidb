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

var (
	schedulersPrefix = "pd/api/v1/schedulers"
)

// NewSchedulerCommand returns a scheduler command.
func NewSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "scheduler",
		Short: "scheduler commands",
	}
	c.AddCommand(NewShowSchedulerCommand())
	c.AddCommand(NewAddSchedulerCommand())
	c.AddCommand(NewRemoveSchedulerCommand())
	return c
}

// NewShowSchedulerCommand returns a command to show schedulers.
func NewShowSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "show",
		Short: "show schedulers",
		Run:   showSchedulerCommandFunc,
	}
	return c
}

func showSchedulerCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 0 {
		fmt.Println(cmd.UsageString())
		return
	}

	r, err := doRequest(cmd, schedulersPrefix, http.MethodGet)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(r)
}

// NewAddSchedulerCommand returns a command to add scheduler.
func NewAddSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "add <scheduler>",
		Short: "add a scheduler",
	}
	c.AddCommand(NewGrantLeaderSchedulerCommand())
	c.AddCommand(NewEvictLeaderSchedulerCommand())
	c.AddCommand(NewShuffleLeaderSchedulerCommand())
	c.AddCommand(NewShuffleRegionSchedulerCommand())
	return c
}

// NewGrantLeaderSchedulerCommand returns a command to add a grant-leader-scheduler.
func NewGrantLeaderSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "grant-leader-scheduler <store_id>",
		Short: "add a scheduler to grant leader to a store",
		Run:   addSchedulerForStoreCommandFunc,
	}
	return c
}

// NewEvictLeaderSchedulerCommand returns a command to add a evict-leader-scheduler.
func NewEvictLeaderSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "evict-leader-scheduler <store_id>",
		Short: "add a scheduler to evict leader from a store",
		Run:   addSchedulerForStoreCommandFunc,
	}
	return c
}

func addSchedulerForStoreCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		fmt.Println(cmd.UsageString())
		return
	}

	storeID, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		fmt.Println(err)
		return
	}

	input := make(map[string]interface{})
	input["name"] = cmd.Name()
	input["store_id"] = storeID
	postJSON(cmd, schedulersPrefix, input)
}

// NewShuffleLeaderSchedulerCommand returns a command to add a shuffle-leader-scheduler.
func NewShuffleLeaderSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "shuffle-leader-scheduler",
		Short: "add a scheduler to shuffle leaders between stores",
		Run:   addSchedulerCommandFunc,
	}
	return c
}

// NewShuffleRegionSchedulerCommand returns a command to add a shuffle-region-scheduler.
func NewShuffleRegionSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "shuffle-region-scheduler",
		Short: "add a scheduler to shuffle regions between stores",
		Run:   addSchedulerCommandFunc,
	}
	return c
}

func addSchedulerCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 0 {
		fmt.Println(cmd.UsageString())
		return
	}

	input := make(map[string]interface{})
	input["name"] = cmd.Name()
	postJSON(cmd, schedulersPrefix, input)
}

// NewRemoveSchedulerCommand returns a command to remove scheduler.
func NewRemoveSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "remove <scheduler>",
		Short: "remove a scheduler",
		Run:   removeSchedulerCommandFunc,
	}
	return c
}

func removeSchedulerCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		fmt.Println(cmd.Usage())
		return
	}

	path := schedulersPrefix + "/" + args[0]
	_, err := doRequest(cmd, path, http.MethodDelete)
	if err != nil {
		fmt.Println(err)
		return
	}
}
