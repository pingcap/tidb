// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/klauspost/compress/zstd"
)

const (
	cmdValidateBackupFiles = "validateBackupFiles"
	extSST                 = ".sst"
	extLOG                 = ".log"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run utils.go <command> [arguments]")
		fmt.Println("Available commands:")
		fmt.Println("  validateBackupFiles")
		os.Exit(1)
	}

	switch os.Args[1] {
	case cmdValidateBackupFiles:
		if !validateBackupFiles(os.Args[2:]) {
			fmt.Println("validation failed")
			os.Exit(1)
		}
	default:
		fmt.Printf("Unknown command: %s\n", os.Args[1])
		os.Exit(1)
	}
}

func validateBackupFiles(args []string) bool {
	validateCmd := flag.NewFlagSet(cmdValidateBackupFiles, flag.ExitOnError)
	cmd := validateCmd.String("command", "", "Backup or restore command")
	encryptionArg := validateCmd.String("encryption", "", "Encryption argument")

	err := validateCmd.Parse(args)
	if err != nil {
		fmt.Println("Failed to parse arguments")
		return false
	}

	if *cmd == "" {
		fmt.Println("Please provide the full backup or restore command using --command flag")
		validateCmd.PrintDefaults()
		return false
	}

	storagePath, found := parseCommand(*cmd)
	// doesn't need to validate if it's not doing backup/restore
	if !found {
		fmt.Println("No need to validate")
		return true
	}

	fmt.Printf("Validating files in: %s\n", storagePath)
	return checkCompressionAndEncryption(storagePath, *encryptionArg)
}

func parseCommand(cmd string) (string, bool) {
	args := strings.Fields(cmd)
	hasBackupOrRestore := false
	storagePath := ""

	for i, arg := range args {
		if arg == "backup" || arg == "restore" {
			hasBackupOrRestore = true
		}
		if arg == "-s" && i+1 < len(args) && strings.HasPrefix(args[i+1], "local://") {
			storagePath = strings.TrimPrefix(args[i+1], "local://")
		}
	}

	if hasBackupOrRestore && storagePath != "" {
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
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0)
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
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0)
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
