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

var (
	labelsPrefix      = "pd/api/v1/labels"
	labelsStorePrefix = "pd/api/v1/labels/stores"
)

// NewLabelCommand return a member subcommand of rootCmd
func NewLabelCommand() *cobra.Command {
	l := &cobra.Command{
		Use:   "label [store]",
		Short: "show the labels",
		Run:   showLabelsCommandFunc,
	}
	l.AddCommand(NewLabelListStoresCommand())
	return l
}

// NewLabelListStoresCommand return a label subcommand of labelCmd
func NewLabelListStoresCommand() *cobra.Command {
	l := &cobra.Command{
		Use:   "store <name> [value]",
		Short: "show the stores with specify label",
		Run:   showLabelListStoresCommandFunc,
	}
	return l
}

func showLabelsCommandFunc(cmd *cobra.Command, args []string) {
	r, err := doRequest(cmd, labelsPrefix, http.MethodGet)
	if err != nil {
		fmt.Printf("Failed to get labels: %s\n", err)
		return
	}
	fmt.Println(r)
}

func getValue(args []string, i int) string {
	if len(args) <= i {
		return ""
	}
	return args[i]
}

func showLabelListStoresCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) > 2 {
		fmt.Println("Usage: label store name [value]")
		return
	}
	namePrefix := fmt.Sprintf("name=%s", getValue(args, 0))
	valuePrefix := fmt.Sprintf("value=%s", getValue(args, 1))
	prefix := fmt.Sprintf("%s?%s&%s", labelsStorePrefix, namePrefix, valuePrefix)
	r, err := doRequest(cmd, prefix, http.MethodGet)
	if err != nil {
		fmt.Printf("Failed to get stores through label: %s\n", err)
		return
	}
	fmt.Println(r)
}
