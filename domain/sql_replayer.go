package domain

import (
	"os"
	"path/filepath"
	"strconv"
)

// GetSQLReplayerDirName returns sql replayer directory path.
// The path is related to the process id.
func GetSQLReplayerDirName() string {
	return filepath.Join(os.TempDir(), "sql_replayer", strconv.Itoa(os.Getpid()))
}
