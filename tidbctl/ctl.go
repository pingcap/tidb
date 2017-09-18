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

package tidbctl

import (
	"fmt"

	"errors"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/pingcap/tidb/tidbctl/command"
	"github.com/spf13/cobra"
)

var (
	errInvalidAddr = errors.New("Invalid TiDB address, Cannot get connect to it")
	statusPrefix   = "status"
)

// CommandFlags are flags that used in all Commands
type CommandFlags struct {
	URL string
}

var (
	rootCmd = &cobra.Command{
		Use:   "tidbctl",
		Short: "TiDB control",
	}
)

func init() {
	rootCmd.AddCommand(
		command.NewTableIDCommand(),
	)
	cobra.EnablePrefixMatching = true
}

// Start run Command
func Start(args []string) {
	rootCmd.SetArgs(args)
	rootCmd.SilenceErrors = true
	rootCmd.ParseFlags(args)

	// TiDB address validation
	err := validTiDBAddrInCmd(rootCmd)
	if err != nil {
		fmt.Println("Error when validate tidb url: ", err)
		return
	}

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(rootCmd.UsageString())
	}
}

func validTiDBAddrInCmd(cmd *cobra.Command) error {
	addr, err := cmd.Flags().GetString("tidb")
	if err != nil {
		return err
	}
	return validTiDBAddr(addr)
}

// validTiDBAddr used to validate tidb addr
// TODO move to global package if necessary
func validTiDBAddr(tidb string) error {
	u, err := url.Parse(tidb)
	if err != nil {
		return err
	}
	if u.Scheme == "" {
		u.Scheme = "http"
	}
	addr := u.String()
	// use status endpoint as the validation
	reps, err := http.Get(fmt.Sprintf("%s/%s", addr, statusPrefix))
	if err != nil {
		return err
	}
	defer reps.Body.Close()
	ioutil.ReadAll(reps.Body)
	if reps.StatusCode != http.StatusOK {
		return errInvalidAddr
	}
	return nil
}
