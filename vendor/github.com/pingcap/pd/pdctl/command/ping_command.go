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
	"time"

	"github.com/spf13/cobra"
)

// NewPingCommand return a ping subcommand of rootCmd
func NewPingCommand() *cobra.Command {
	m := &cobra.Command{
		Use:   "ping",
		Short: "show the total time spend ping the pd",
		Run:   showPingCommandFunc,
	}
	return m
}

func showPingCommandFunc(cmd *cobra.Command, args []string) {
	start := time.Now()
	_, err := doRequest(cmd, pingPrefix, http.MethodGet)
	if err != nil {
		fmt.Println(err)
		return
	}
	elapsed := time.Since(start)
	fmt.Println("time:", elapsed)
}
