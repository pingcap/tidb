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

package pdctl

import (
	"fmt"

	"github.com/pingcap/pd/pdctl/command"
	"github.com/spf13/cobra"
)

// CommandFlags are flags that used in all Commands
type CommandFlags struct {
	URL      string
	CAPath   string
	CertPath string
	KeyPath  string
}

var (
	rootCmd = &cobra.Command{
		Use:   "pdctl",
		Short: "Placement Driver control",
	}
	commandFlags = CommandFlags{}
)

func init() {
	rootCmd.PersistentFlags().StringVarP(&commandFlags.URL, "pd", "u", "http://127.0.0.1:2379", "pd address")
	rootCmd.Flags().StringVar(&commandFlags.CAPath, "cacert", "", "path of file that contains list of trusted SSL CAs.")
	rootCmd.Flags().StringVar(&commandFlags.CertPath, "cert", "", "path of file that contains X509 certificate in PEM format.")
	rootCmd.Flags().StringVar(&commandFlags.KeyPath, "key", "", "path of file that contains X509 key in PEM format.")
	rootCmd.AddCommand(
		command.NewConfigCommand(),
		command.NewRegionCommand(),
		command.NewStoreCommand(),
		command.NewMemberCommand(),
		command.NewExitCommand(),
		command.NewLabelCommand(),
		command.NewPingCommand(),
		command.NewOperatorCommand(),
		command.NewSchedulerCommand(),
		command.NewTSOCommand(),
		command.NewHotSpotCommand(),
		command.NewClusterCommand(),
		command.NewTableNamespaceCommand(),
		command.NewHealthCommand(),
	)
	cobra.EnablePrefixMatching = true
}

// Start run Command
func Start(args []string) {
	rootCmd.SetArgs(args)
	rootCmd.SilenceErrors = true
	rootCmd.ParseFlags(args)
	rootCmd.SetUsageTemplate(command.UsageTemplate)

	if len(commandFlags.CAPath) != 0 {
		if err := command.InitHTTPSClient(commandFlags.CAPath, commandFlags.CertPath, commandFlags.KeyPath); err != nil {
			fmt.Println(err)
			return
		}
	}

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(rootCmd.UsageString())
	}
}
