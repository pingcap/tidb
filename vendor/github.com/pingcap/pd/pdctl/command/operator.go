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

	"github.com/juju/errors"
	"github.com/spf13/cobra"
)

var (
	operatorsPrefix = "pd/api/v1/operators"
)

// NewOperatorCommand returns a operator command.
func NewOperatorCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "operator",
		Short: "operator commands",
	}
	c.AddCommand(NewShowOperatorCommand())
	c.AddCommand(NewAddOperatorCommand())
	c.AddCommand(NewRemoveOperatorCommand())
	return c
}

// NewShowOperatorCommand returns a command to show operators.
func NewShowOperatorCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "show [kind]",
		Short: "show operators",
		Run:   showOperatorCommandFunc,
	}
	return c
}

func showOperatorCommandFunc(cmd *cobra.Command, args []string) {
	var path string
	if len(args) == 0 {
		path = operatorsPrefix
	} else if len(args) == 1 {
		path = fmt.Sprintf("%s?kind=%s", operatorsPrefix, args[0])
	} else {
		fmt.Println(cmd.UsageString())
		return
	}

	r, err := doRequest(cmd, path, http.MethodGet)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(r)
}

// NewAddOperatorCommand returns a command to add operators.
func NewAddOperatorCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "add <operator>",
		Short: "add an operator",
	}
	c.AddCommand(NewTransferLeaderCommand())
	c.AddCommand(NewTransferRegionCommand())
	c.AddCommand(NewTransferPeerCommand())
	c.AddCommand(NewAddPeerCommand())
	c.AddCommand(NewRemovePeerCommand())
	return c
}

// NewTransferLeaderCommand returns a command to transfer leader.
func NewTransferLeaderCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "transfer-leader <region_id> <to_store_id>",
		Short: "transfer a region's leader to the specified store",
		Run:   transferLeaderCommandFunc,
	}
	return c
}

func transferLeaderCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		fmt.Println(cmd.UsageString())
		return
	}

	ids, err := parseUint64s(args)
	if err != nil {
		fmt.Println(err)
		return
	}

	input := make(map[string]interface{})
	input["name"] = cmd.Name()
	input["region_id"] = ids[0]
	input["to_store_id"] = ids[1]
	postJSON(cmd, operatorsPrefix, input)
}

// NewTransferRegionCommand returns a command to transfer region.
func NewTransferRegionCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "transfer-region <region_id> <to_store_id>...",
		Short: "transfer a region's peers to the specified stores",
		Run:   transferRegionCommandFunc,
	}
	return c
}

func transferRegionCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) <= 2 {
		fmt.Println(cmd.UsageString())
		return
	}

	ids, err := parseUint64s(args)
	if err != nil {
		fmt.Println(err)
		return
	}

	input := make(map[string]interface{})
	input["name"] = cmd.Name()
	input["region_id"] = ids[0]
	input["to_store_ids"] = ids[1:]
	postJSON(cmd, operatorsPrefix, input)
}

// NewTransferPeerCommand returns a command to transfer region.
func NewTransferPeerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "transfer-peer <region_id> <from_store_id> <to_store_id>",
		Short: "transfer a region's peer from the specified store to another store",
		Run:   transferPeerCommandFunc,
	}
	return c
}

func transferPeerCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 3 {
		fmt.Println(cmd.UsageString())
		return
	}

	ids, err := parseUint64s(args)
	if err != nil {
		fmt.Println(err)
		return
	}

	input := make(map[string]interface{})
	input["name"] = cmd.Name()
	input["region_id"] = ids[0]
	input["from_store_id"] = ids[1]
	input["to_store_id"] = ids[2]
	postJSON(cmd, operatorsPrefix, input)
}

// NewAddPeerCommand returns a command to add region peer.
func NewAddPeerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "add-peer <region_id> <to_store_id>",
		Short: "add a region peer on specified store",
		Run:   addPeerCommandFunc,
	}
	return c
}

func addPeerCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		fmt.Println(cmd.UsageString())
		return
	}

	ids, err := parseUint64s(args)
	if err != nil {
		fmt.Println(err)
		return
	}

	input := make(map[string]interface{})
	input["name"] = cmd.Name()
	input["region_id"] = ids[0]
	input["store_id"] = ids[1]
	postJSON(cmd, operatorsPrefix, input)
}

// NewRemovePeerCommand returns a command to add region peer.
func NewRemovePeerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "remove-peer <region_id> <from_store_id>",
		Short: "remove a region peer on specified store",
		Run:   removePeerCommandFunc,
	}
	return c
}

func removePeerCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		fmt.Println(cmd.UsageString())
		return
	}

	ids, err := parseUint64s(args)
	if err != nil {
		fmt.Println(err)
		return
	}

	input := make(map[string]interface{})
	input["name"] = cmd.Name()
	input["region_id"] = ids[0]
	input["store_id"] = ids[1]
	postJSON(cmd, operatorsPrefix, input)
}

// NewRemoveOperatorCommand returns a command to remove operators.
func NewRemoveOperatorCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "remove <region_id>",
		Short: "remove the region operator",
		Run:   removeOperatorCommandFunc,
	}
	return c
}

func removeOperatorCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		fmt.Println(cmd.UsageString())
		return
	}

	path := operatorsPrefix + "/" + args[0]
	_, err := doRequest(cmd, path, http.MethodDelete)
	if err != nil {
		fmt.Println(err)
		return
	}
}

func parseUint64s(args []string) ([]uint64, error) {
	results := make([]uint64, 0, len(args))
	for _, arg := range args {
		v, err := strconv.ParseUint(arg, 10, 64)
		if err != nil {
			return nil, errors.Trace(err)
		}
		results = append(results, v)
	}
	return results, nil
}
