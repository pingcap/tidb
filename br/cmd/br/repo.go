// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"text/tabwriter"

	"github.com/pingcap/errors"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/repo"
	"github.com/pingcap/tidb/br/pkg/task"
	taskrepo "github.com/pingcap/tidb/br/pkg/task/repo"
	"github.com/pingcap/tidb/br/pkg/version/build"
	tidblogutil "github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/spf13/cobra"
	"github.com/tikv/client-go/v2/oracle"
)

const (
	repoSnapshotFlagBackupID = "backup-id"
	repoSnapshotFlagView     = "view"
	repoSnapshotFlagYes      = "yes"
	repoSnapshotFlagYesShort = "y"
)

// NewRepoCommand returns a repository management subcommand.
func NewRepoCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "repo",
		Short:        "manage BR snapshot repositories",
		SilenceUsage: true,
		PersistentPreRunE: func(c *cobra.Command, args []string) error {
			if err := Init(c); err != nil {
				return errors.Trace(err)
			}
			build.LogInfo(build.BR)
			tidblogutil.LogEnvVariables()
			task.LogArguments(c)
			return nil
		},
	}
	cmd.AddCommand(newRepoSnapshotCommand())
	return cmd
}

func newRepoSnapshotCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "snapshot",
		Short: "manage snapshot backups in a repository",
	}
	cmd.AddCommand(
		newRepoSnapshotListCommand(),
		newRepoSnapshotGetCommand(),
		newRepoSnapshotDeleteCommand(),
	)
	return cmd
}

func newRepoSnapshotListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "list snapshot backups",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := parseRepoSnapshotConfig(cmd)
			if err != nil {
				cmd.SilenceUsage = false
				return errors.Trace(err)
			}
			ctx := GetDefaultContext()
			backups, err := taskrepo.RunRepoSnapshotListItems(
				ctx,
				glue.GetConsole(tidbGlue),
				taskrepo.RepoSnapshotListConfig{Config: cfg},
			)
			if err != nil {
				return errors.Trace(err)
			}
			return printRepoSnapshotList(cmd, backups)
		},
	}
	return cmd
}

func newRepoSnapshotGetCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get",
		Short: "print a metadata view from a snapshot backup",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := parseRepoSnapshotConfig(cmd)
			if err != nil {
				cmd.SilenceUsage = false
				return errors.Trace(err)
			}
			backupID, err := parseRequiredRepoSnapshotBackupID(cmd)
			if err != nil {
				cmd.SilenceUsage = false
				return errors.Trace(err)
			}
			view, err := cmd.Flags().GetString(repoSnapshotFlagView)
			if err != nil {
				cmd.SilenceUsage = false
				return errors.Trace(err)
			}
			ctx := GetDefaultContext()
			return errors.Trace(taskrepo.RunRepoSnapshotGetTo(
				ctx,
				glue.ConsoleOperations{ConsoleGlue: glue.NoOPConsoleGlue{}},
				taskrepo.RepoSnapshotGetConfig{
					Config:   cfg,
					BackupID: backupID,
					View:     view,
				},
				cmd.OutOrStdout(),
			))
		},
	}
	cmd.Flags().String(repoSnapshotFlagBackupID, "", "snapshot backup id")
	cmd.Flags().String(repoSnapshotFlagView, "basic", "metadata view: basic, tables, or files")
	_ = cmd.MarkFlagRequired(repoSnapshotFlagBackupID)
	return cmd
}

func newRepoSnapshotDeleteCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "delete one snapshot backup or pending attempt from the repository",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := parseRepoSnapshotConfig(cmd)
			if err != nil {
				cmd.SilenceUsage = false
				return errors.Trace(err)
			}
			backupID, err := parseRequiredRepoSnapshotBackupID(cmd)
			if err != nil {
				cmd.SilenceUsage = false
				return errors.Trace(err)
			}
			skipPrompt, err := cmd.Flags().GetBool(repoSnapshotFlagYes)
			if err != nil {
				cmd.SilenceUsage = false
				return errors.Trace(err)
			}
			ctx := GetDefaultContext()
			result, err := taskrepo.RunRepoSnapshotDelete(ctx, glue.GetConsole(tidbGlue), taskrepo.RepoSnapshotDeleteConfig{
				Config:     cfg,
				BackupID:   backupID,
				SkipPrompt: skipPrompt,
			})
			if err != nil {
				if berrors.Is(err, berrors.ErrOperationAborted) {
					_, err = fmt.Fprintln(cmd.OutOrStdout(), "operation canceled")
					return errors.Trace(err)
				}
				return errors.Trace(err)
			}
			return printRepoSnapshotMutationResult(
				cmd,
				"deleted",
				backupID,
				result.BackupID,
				result.MetadataDeleted,
				result.DataDeleted,
				result.PendingDeleted,
			)
		},
	}
	cmd.Flags().String(repoSnapshotFlagBackupID, "", "snapshot backup id")
	cmd.Flags().BoolP(
		repoSnapshotFlagYes,
		repoSnapshotFlagYesShort,
		false,
		"skip the confirmation prompt and execute the command directly",
	)
	_ = cmd.MarkFlagRequired(repoSnapshotFlagBackupID)
	return cmd
}

func parseRepoSnapshotConfig(cmd *cobra.Command) (taskrepo.Config, error) {
	cfg := task.Config{LogProgress: HasLogFile()}
	if err := cfg.ParseFromFlags(cmd.Flags()); err != nil {
		return taskrepo.Config{}, errors.Trace(err)
	}
	return taskrepo.Config{
		BackendOptions: cfg.BackendOptions,
		Storage:        cfg.Storage,
		CipherInfo:     cfg.CipherInfo,
		NoCreds:        cfg.NoCreds,
		SendCreds:      cfg.SendCreds,
	}, nil
}

func parseRequiredRepoSnapshotBackupID(cmd *cobra.Command) (repo.BackupID, error) {
	backupID, err := taskrepo.ParseSnapshotBackupIDFlag(cmd.Flags())
	if err != nil {
		return 0, errors.Trace(err)
	}
	if backupID.IsZero() {
		return 0, errors.Annotatef(berrors.ErrInvalidArgument, "--%s is required", repoSnapshotFlagBackupID)
	}
	return backupID, nil
}

func printRepoSnapshotList(cmd *cobra.Command, backups []taskrepo.RepoSnapshotListItem) error {
	tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 8, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "BACKUP ID\tBACKUP TIME (EST.)\tSTATUS"); err != nil {
		return errors.Trace(err)
	}
	for _, backup := range backups {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\n",
			backup.BackupID.String(),
			formatRepoSnapshotTime(backup.BackupID),
			backup.Status,
		); err != nil {
			return errors.Trace(err)
		}
	}
	return errors.Trace(tw.Flush())
}

func printRepoSnapshotMutationResult(
	cmd *cobra.Command,
	action string,
	inputBackupID repo.BackupID,
	backupID repo.BackupID,
	metadataDeleted, dataDeleted, pendingDeleted int,
) error {
	if backupID.IsZero() {
		backupID = inputBackupID
	}
	_, err := fmt.Fprintf(cmd.OutOrStdout(),
		"%s snapshot %s: metadata=%d data=%d pending=%d\n",
		action, backupID.String(), metadataDeleted, dataDeleted, pendingDeleted)
	return errors.Trace(err)
}

func formatRepoSnapshotTime(backupID repo.BackupID) string {
	return oracle.GetTimeFromTS(uint64(backupID)).Local().Format("2006-01-02 15:04:05.999999999 -0700")
}
