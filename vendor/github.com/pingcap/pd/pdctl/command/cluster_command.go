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

const clusterPrefix = "pd/api/v1/cluster"

// NewClusterCommand return a cluster subcommand of rootCmd
func NewClusterCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cluster",
		Short: "show the cluster information",
		Run:   showClusterCommandFunc,
	}
	return cmd
}

func showClusterCommandFunc(cmd *cobra.Command, args []string) {
	r, err := doRequest(cmd, clusterPrefix, http.MethodGet)
	if err != nil {
		fmt.Printf("Failed to get the cluster information: %s\n", err)
		return
	}
	fmt.Println(r)
}
