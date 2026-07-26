// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import "strings"

func taskStatusCommand(cmdName string) string {
	switch cmdName {
	case FullBackupCmd, FullRestoreCmd:
		return "full"
	case DBBackupCmd, DBRestoreCmd:
		return "db"
	case TableBackupCmd, TableRestoreCmd:
		return "table"
	default:
		return strings.ToLower(strings.ReplaceAll(cmdName, " ", "_"))
	}
}

func taskStatusScope(command string, explicitFilter bool, filterStr []string) string {
	if len(filterStr) == 0 {
		return "unknown"
	}
	switch command {
	case "full":
		if !explicitFilter || (len(filterStr) == 1 && filterStr[0] == "*.*") {
			return "cluster"
		}
		return "filter=" + strings.Join(filterStr, ",")
	case "db":
		return "db=" + strings.TrimSuffix(filterStr[0], ".*")
	case "table":
		return "table=" + filterStr[0]
	default:
		return "unknown"
	}
}
