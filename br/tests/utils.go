// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/klauspost/compress/zstd"
	"github.com/spf13/cobra"
)

const (
	cmdValidateBackupFiles = "validateBackupFiles"
	extSST                 = ".sst"
	extLOG                 = ".log"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "utils",
		Short: "Utility commands for backup and restore",
	}

	validateCmd := &cobra.Command{
		Use:   cmdValidateBackupFiles,
		Short: "Validate backup files",
		Run:   runValidateBackupFiles,
	}

	validateCmd.Flags().String("command", "", "Backup or restore command")
	validateCmd.Flags().String("encryption", "", "Encryption argument")

	rootCmd.AddCommand(validateCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func runValidateBackupFiles(cmd *cobra.Command, args []string) {
	command, _ := cmd.Flags().GetString("command")
	encryptionArg, _ := cmd.Flags().GetString("encryption")

	if command == "" {
		fmt.Println("Please provide the full backup or restore command using --command flag")
		err := cmd.Usage()
		if err != nil {
			fmt.Println("Usage error")
			return
		}
		os.Exit(1)
	}

	storagePath, found := parseCommand(command)
	// doesn't need to validate if it's not doing backup/restore
	if !found {
		fmt.Println("No need to validate")
		return
	}

	fmt.Printf("Validating files in: %s\n", storagePath)
	if !checkCompressionAndEncryption(storagePath, encryptionArg) {
		fmt.Println("validation failed")
		os.Exit(1)
	}
}

// parseCommand parses the command and only returns the storage path if it's a full backup or restore point
// as full backup will have backup files ready in the storage path after returning from the command
// and log backup will not, so we can only use restore point to validate.
func parseCommand(cmd string) (string, bool) {
	// Create a temporary cobra command to parse the input
	tempCmd := &cobra.Command{}
	tempCmd.Flags().String("s", "", "Storage path (short)")
	tempCmd.Flags().String("storage", "", "Storage path (long)")

	// Split the command string into args
	args := strings.Fields(cmd)

	// Parse the args
	if err := tempCmd.Flags().Parse(args); err != nil {
		return "", false
	}

	// Check for backup or restore point command
	hasBackupOrRestorePoint := false
	for i, arg := range args {
		if arg == "backup" {
			hasBackupOrRestorePoint = true
			break
		}
		if i < len(args)-1 && arg == "restore" && args[i+1] == "point" {
			hasBackupOrRestorePoint = true
			break
		}
	}

	// Get the storage path from either -s or -storage flag
	storagePath, _ := tempCmd.Flags().GetString("s")
	if storagePath == "" {
		storagePath, _ = tempCmd.Flags().GetString("storage")
	}
	storagePath = strings.TrimPrefix(storagePath, "local://")

	if hasBackupOrRestorePoint && storagePath != "" {
		return storagePath, true
	}
	return "", false
}

func checkCompressionAndEncryption(dir string, encryptionArg string) bool {
	allEncrypted := true
	allUnencrypted := true
	totalFiles := 0

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		if strings.HasSuffix(path, extSST) {
			totalFiles++
			isValidSST, err := isLikelySSTFile(path)
			if err != nil {
				fmt.Printf("Error checking SST file %s: %v\n", path, err)
				return err
			}
			if isValidSST {
				allEncrypted = false
			} else {
				allUnencrypted = false
			}
		} else if strings.HasSuffix(path, extLOG) {
			totalFiles++
			isCompressed, err := isZstdCompressed(path)
			if err != nil {
				fmt.Printf("Error checking if file is encrypted %s: %v\n", path, err)
				return err
			}
			if isCompressed {
				allEncrypted = false
			} else {
				allUnencrypted = false
			}
		}

		return nil
	})

	if err != nil {
		fmt.Printf("Error walking through directory: %v\n", err)
		os.Exit(1)
	}

	// handle with encryption case
	if encryptionArg != "" {
		if allEncrypted {
			fmt.Printf("All files in %s are encrypted, as expected with encryption\n", dir)
			return true
		}
		fmt.Printf("Error: Some files in %s are not encrypted, which is unexpected with encryption\n", dir)
		return false
	}

	// handle without encryption case
	if allUnencrypted {
		fmt.Printf("All files in %s are not encrypted, as expected without encryption\n", dir)
		return true
	} else if allEncrypted {
		fmt.Printf("Error: All files in %s are encrypted, which is unexpected without encryption\n", dir)
		return false
	}
	fmt.Printf("Error: Mixed encryption in %s. Some files are encrypted, some are not.\n", dir)
	return false
}

func isZstdCompressed(filePath string) (bool, error) {
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0) //nolint:gosec
	if err != nil {
		return false, err
	}
	defer file.Close()

	decoder, err := zstd.NewReader(file)
	if err != nil {
		return false, nil // Not compressed or error in compression
	}
	defer decoder.Close()

	// Try to read a small amount of data
	_, err = decoder.Read(make([]byte, 1))
	if err != nil {
		return false, nil // Not compressed or error in decompression
	}

	return true, nil
}

func isLikelySSTFile(filePath string) (bool, error) {
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0) //nolint:gosec
	if err != nil {
		return false, err
	}
	defer file.Close()

	// Seek to 8 bytes from the end of the file
	_, err = file.Seek(-8, io.SeekEnd)
	if err != nil {
		return false, err
	}

	// Read the last 8 bytes
	footer := make([]byte, 8)
	_, err = file.Read(footer)
	if err != nil {
		return false, err
	}

	// Check for SST magic number (kLegacyBlockBasedTableMagicNumber)
	// or (kBlockBasedTableMagicNumber)
	magicNumber := binary.LittleEndian.Uint64(footer)
	return magicNumber == 0xdb4775248b80fb57 || magicNumber == 0x88e241b785f4cff7, nil
}
