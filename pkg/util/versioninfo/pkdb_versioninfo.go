package versioninfo

import (
	"fmt"
	"runtime"
)

// Version information for TiDBX.
var (
	TiDBXMode = false

	ReleaseVersion = "None"
	Profile        = "debug"
	RaceEnabled    bool

	TiDBXGitHash   = "None"
	TiDBXGitBranch = "None"

	TiKVGitHash        = "None"
	TiKVGitBranch      = "None"
	TiKVBuildTS        = "None"
	TiKVRustVersion    = "None"
	TiKVEnableFeatures = "None"
	TiKVProfile        = "None"

	PDGitHash   = "None"
	PDGitBranch = "None"

	ClientGOGitHash   = "None"
	ClientGOGitBranch = "None"
	goVersion         = runtime.Version()
)

// GetTiDBXInfo returns the git hash and build time of this tidbx-server binary.
func GetTiDBXInfo() string {
	return fmt.Sprintf("Release Version: %s\n"+
		"Edition: %s\n"+
		"Git Commit Hash: %s\n"+
		"Git Branch: %s\n"+
		"UTC Build Time: %s\n"+
		"Go Version: %s\n"+
		"Race Enabled: %v\n"+
		"Profile: %s\n"+
		"Submodule:\n"+
		"  TiDB:\n"+
		"    Git Commit: %s (%s)\n"+
		"  TiKV:\n"+
		"    Git Commit: %s (%s)\n"+
		"    UTC Build Time: %s\n"+
		"    Rust Version: %s\n"+
		"    Enable Features: %s\n"+
		"    Profile: %s\n"+
		"  PD:\n"+
		"    Git Commit: %s (%s)\n"+
		"  TiKV-Client-GO:\n"+
		"    Git Commit: %s (%s)",
		ReleaseVersion,
		TiDBEdition,
		TiDBXGitHash,
		TiDBXGitBranch,
		TiDBBuildTS,
		goVersion,
		RaceEnabled,
		Profile,
		TiDBGitHash,
		TiDBGitBranch,
		TiKVGitHash,
		TiKVGitBranch,
		TiKVBuildTS,
		TiKVRustVersion,
		TiKVEnableFeatures,
		TiKVProfile,
		PDGitHash,
		PDGitBranch,
		ClientGOGitHash,
		ClientGOGitBranch,
	)
}
