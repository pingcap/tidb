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
	"strconv"
	"time"

	"github.com/spf13/cobra"
)

const (
	physicalShiftBits = 18
	logicalBits       = 0x3FFFF
)

// NewTSOCommand return a ping subcommand of rootCmd
func NewTSOCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "tso <timestamp>",
		Short: "parse TSO to the system and logic time",
		Run:   showTSOCommandFunc,
	}
	return cmd
}

func showTSOCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		fmt.Println("Usage: tso <timestamp>")
		return
	}
	ts, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		fmt.Printf("Failed to parse TSO: %s\n", err)
		return
	}
	logical := ts & logicalBits
	physical := ts >> physicalShiftBits
	physicalTime := time.Unix(int64(physical/1000), 0)
	fmt.Println("system: ", physicalTime)
	fmt.Println("logic: ", logical)
}
