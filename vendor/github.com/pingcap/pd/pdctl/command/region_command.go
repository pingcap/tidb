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
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/spf13/cobra"
)

var (
	regionsPrefix   = "pd/api/v1/regions"
	regionIDPrefix  = "pd/api/v1/region/id"
	regionKeyPrefix = "pd/api/v1/region/key"
)

type regionInfo struct {
	Region *metapb.Region `json:"region"`
	Leader *metapb.Peer   `json:"leader"`
}

// NewRegionCommand return a region subcommand of rootCmd
func NewRegionCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "region <region_id>",
		Short: "show the region status",
		Run:   showRegionCommandFunc,
	}
	r.AddCommand(NewRegionWithKeyCommand())
	return r
}

func showRegionCommandFunc(cmd *cobra.Command, args []string) {
	var prefix string
	prefix = regionsPrefix
	if len(args) == 1 {
		if _, err := strconv.Atoi(args[0]); err != nil {
			fmt.Println("region_id should be a number")
			return
		}
		prefix = regionIDPrefix + "/" + args[0]
	}
	r, err := doRequest(cmd, prefix, http.MethodGet)
	if err != nil {
		fmt.Printf("Failed to get region: %s\n", err)
		return
	}
	fmt.Println(r)
}

// NewRegionWithKeyCommand return a region with key subcommand of regionCmd
func NewRegionWithKeyCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "key [--format=raw|pb|proto|protobuf] <key>",
		Short: "show the region with key",
		Run:   showRegionWithTableCommandFunc,
	}
	r.Flags().String("format", "raw", "the key format")
	return r
}

func showRegionWithTableCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		fmt.Println(cmd.UsageString())
		return
	}

	var (
		key string
		err error
	)

	format := cmd.Flags().Lookup("format").Value.String()
	switch format {
	case "raw":
		key = args[0]
	case "pb", "proto", "protobuf":
		key, err = decodeProtobufText(args[0])
		if err != nil {
			fmt.Println("Error: ", err)
			return
		}
	default:
		fmt.Println("Error: unknown format")
		return
	}
	// TODO: Deal with path escaped
	prefix := regionKeyPrefix + "/" + key
	r, err := doRequest(cmd, prefix, http.MethodGet)
	if err != nil {
		fmt.Printf("Failed to get region: %s\n", err)
		return
	}
	fmt.Println(r)

}

func decodeProtobufText(text string) (string, error) {
	var buf []byte
	r := bytes.NewBuffer([]byte(text))
	for {
		c, err := r.ReadByte()
		if err != nil {
			if err != io.EOF {
				return "", err
			}
			break
		}
		if c == '\\' {
			_, err := fmt.Sscanf(string(r.Next(3)), "%03o", &c)
			if err != nil {
				return "", err
			}
		}
		buf = append(buf, c)
	}
	return string(buf), nil
}
