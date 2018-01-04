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

	"github.com/spf13/cobra"
)

const (
	hotReadRegionsPrefix  = "pd/api/v1/hotspot/regions/read"
	hotWriteRegionsPrefix = "pd/api/v1/hotspot/regions/write"
	hotStoresPrefix       = "pd/api/v1/hotspot/stores"
)

// NewHotSpotCommand return a hot subcommand of rootCmd
func NewHotSpotCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "hot",
		Short: "show the hotspot status of the cluster",
	}
	cmd.AddCommand(NewHotWriteRegionCommand())
	cmd.AddCommand(NewHotReadRegionCommand())
	cmd.AddCommand(NewHotStoreCommand())
	return cmd
}

// NewHotWriteRegionCommand return a hot regions subcommand of hotSpotCmd
func NewHotWriteRegionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "write",
		Short: "show the hot write regions",
		Run:   showHotWriteRegionsCommandFunc,
	}
	return cmd
}

func showHotWriteRegionsCommandFunc(cmd *cobra.Command, args []string) {
	r, err := doRequest(cmd, hotWriteRegionsPrefix, http.MethodGet)
	if err != nil {
		fmt.Printf("Failed to get hotspot: %s\n", err)
		return
	}
	fmt.Println(r)
}

// NewHotReadRegionCommand return a hot read regions subcommand of hotSpotCmd
func NewHotReadRegionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "read",
		Short: "show the hot read regions",
		Run:   showHotReadRegionsCommandFunc,
	}
	return cmd
}

func showHotReadRegionsCommandFunc(cmd *cobra.Command, args []string) {
	r, err := doRequest(cmd, hotReadRegionsPrefix, http.MethodGet)
	if err != nil {
		fmt.Printf("Failed to get hotspot: %s\n", err)
		return
	}
	fmt.Println(r)
}

// NewHotStoreCommand return a hot stores subcommand of hotSpotCmd
func NewHotStoreCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "store",
		Short: "show the hot stores",
		Run:   showHotStoresCommandFunc,
	}
	return cmd
}

func showHotStoresCommandFunc(cmd *cobra.Command, args []string) {
	r, err := doRequest(cmd, hotStoresPrefix, http.MethodGet)
	if err != nil {
		fmt.Printf("Failed to get hotspot: %s\n", err)
		return
	}
	fmt.Println(r)
}
