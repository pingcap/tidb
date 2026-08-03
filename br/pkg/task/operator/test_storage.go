// Copyright 2025 PingCAP, Inc. Licensed under Apache-2.0.

package operator

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/fatih/color"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
)

const (
	testFileName1       = "br-test-file-1.tmp"
	testFileName2       = "br-test-file-2.tmp"
	testFileNameRenamed = "br-test-file-renamed.tmp"
	testDirName         = "br-test-dir"
	defaultTestDataSize = 1024 * 1024 // 1MB test data
)

// TestResult represents the result of a single test operation.
type TestResult struct {
	Name     string        // Name of the test
	Passed   bool          // Whether the test passed
	Duration time.Duration // Time taken to execute the test
	Details  string        // Additional details about the test result
	Error    error         // Error if the test failed
}

// TestReport contains all test results and summary information.
type TestReport struct {
	StorageURI  string       // The storage URI being tested
	StartTime   time.Time    // When the test started
	EndTime     time.Time    // When the test ended
	TotalTests  int          // Total number of tests
	PassedTests int          // Number of tests that passed
	FailedTests int          // Number of tests that failed
	TestResults []TestResult // Individual test results
	TotalBytes  int64        // Total bytes read/written during tests
}

// AddResult adds a test result to the report.
func (r *TestReport) AddResult(result TestResult) {
	r.TestResults = append(r.TestResults, result)
	r.TotalTests++
	if result.Passed {
		r.PassedTests++
	} else {
		r.FailedTests++
	}
}

// Print outputs a formatted report to stdout.
func (r *TestReport) Print() {
	duration := r.EndTime.Sub(r.StartTime)

	fmt.Println()
	printStep("=" + repeatString("=", 70))
	printStep("STORAGE TEST REPORT")
	printStep("=" + repeatString("=", 70))
	fmt.Printf("Storage URI:     %s\n", r.StorageURI)
	fmt.Printf("Start Time:      %s\n", r.StartTime.Format("2006-01-02 15:04:05"))
	fmt.Printf("End Time:        %s\n", r.EndTime.Format("2006-01-02 15:04:05"))
	fmt.Printf("Total Duration:  %s\n", duration.Round(time.Millisecond))
	fmt.Printf("Total Tests:     %d\n", r.TotalTests)
	fmt.Printf("Passed:          %s\n", color.GreenString("%d", r.PassedTests))
	fmt.Printf("Failed:          %s\n", color.RedString("%d", r.FailedTests))
	fmt.Printf("Total Data:      %s\n", formatBytes(r.TotalBytes))

	printStep("-" + repeatString("-", 70))
	printStep("TEST DETAILS")
	printStep("-" + repeatString("-", 70))

	for i, result := range r.TestResults {
		status := "✓"
		statusColor := color.GreenString
		if !result.Passed {
			status = "✗"
			statusColor = color.RedString
		}

		fmt.Printf("%2d. %s %s (%s)\n",
			i+1,
			statusColor(status),
			result.Name,
			result.Duration.Round(time.Millisecond))

		if result.Details != "" {
			fmt.Printf("    %s\n", color.CyanString(result.Details))
		}

		if result.Error != nil {
			fmt.Printf("    %s\n", color.RedString("Error: %v", result.Error))
		}
	}

	fmt.Println()
	printStep("=" + repeatString("=", 70))
	if r.FailedTests == 0 {
		printSuccess("✅ ALL TESTS PASSED!")
	} else {
		printError("❌ %d TEST(S) FAILED", r.FailedTests)
	}
	printStep("=" + repeatString("=", 70))
}

func repeatString(s string, count int) string {
	result := ""
	for range count {
		result += s
	}
	return result
}

func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// TestStorageConfig is the configuration for testing external storage.
type TestStorageConfig struct {
	storage.BackendOptions
	StorageURI string
	// CleanupOnSuccess indicates whether to cleanup test files on success
	CleanupOnSuccess bool
	// PauseWhenFail pauses the test when a case fails for debugging
	PauseWhenFail bool
	// TestDataSize is the size of test data in bytes
	TestDataSize int64
}

// TestContext bundles context, report, and storage for test execution.
type TestContext struct {
	Ctx           context.Context
	Report        *TestReport
	Store         storage.ExternalStorage
	PauseWhenFail bool
}

// AddResult adds a test result and handles pause-on-fail logic.
func (tc *TestContext) AddResult(result TestResult) {
	tc.Report.AddResult(result)

	// If test failed and pause-on-fail is enabled, wait for user input
	if !result.Passed && tc.PauseWhenFail {
		fmt.Println()
		printError("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		printError("TEST FAILED: %s", result.Name)
		printError("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		if result.Error != nil {
			printError("Error: %v", result.Error)
		}
		fmt.Println()
		fmt.Println(color.YellowString("⏸  Test paused. You can now inspect the storage content."))
		fmt.Println(color.YellowString("   Storage URI: %s", tc.Report.StorageURI))
		fmt.Println()
		fmt.Print(color.CyanString("Press Enter to continue with remaining tests, or Ctrl+C to abort: "))

		// Wait for user input
		_, _ = fmt.Scanln()
		fmt.Println()
		printStep("Resuming tests...")
	}
}

// DefineFlagsForTestStorageConfig defines flags for test-storage command.
func DefineFlagsForTestStorageConfig(flags *pflag.FlagSet) {
	storage.DefineFlags(flags)
	flags.StringP(flagStorage, "s", "", "The external storage URI to test.")
	flags.Bool("cleanup", true, "Whether to cleanup test files on success.")
	flags.Bool("pause-when-fail", false, "Pause the test when a case fails for debugging.")
	flags.Int64("test-data-size", defaultTestDataSize, "The size of test data in bytes (default 1MB).")
}

// ParseFromFlags parses the config from flags.
func (cfg *TestStorageConfig) ParseFromFlags(flags *pflag.FlagSet) error {
	var err error
	err = cfg.BackendOptions.ParseFromFlags(flags)
	if err != nil {
		return err
	}
	cfg.StorageURI, err = flags.GetString(flagStorage)
	if err != nil {
		return err
	}
	cfg.CleanupOnSuccess, err = flags.GetBool("cleanup")
	if err != nil {
		return err
	}
	cfg.PauseWhenFail, err = flags.GetBool("pause-when-fail")
	if err != nil {
		return err
	}
	cfg.TestDataSize, err = flags.GetInt64("test-data-size")
	if err != nil {
		return err
	}
	if cfg.StorageURI == "" {
		return errors.New("storage URI cannot be empty, please specify with --storage")
	}
	if cfg.TestDataSize <= 0 {
		return errors.New("test data size must be greater than 0")
	}
	return nil
}

// RunTestStorage tests all operations of an external storage.
func RunTestStorage(ctx context.Context, cfg TestStorageConfig) error {
	logger := logutil.Logger(ctx)
	logger.Info("Starting external storage test", zap.String("storage", cfg.StorageURI), zap.Any("options", cfg.BackendOptions))

	// Initialize test report
	report := &TestReport{
		StorageURI:  cfg.StorageURI,
		StartTime:   time.Now(),
		TestResults: make([]TestResult, 0),
	}

	// Parse and create storage
	backend, err := storage.ParseBackend(cfg.StorageURI, &cfg.BackendOptions)
	if err != nil {
		return errors.Annotate(err, "failed to parse storage backend")
	}

	store, err := storage.New(ctx, backend, &storage.ExternalStorageOptions{
		SendCredentials: true,
	})
	if err != nil {
		return errors.Annotate(err, "failed to create external storage")
	}
	defer store.Close()

	printStep("Testing external storage: %s", store.URI())
	report.StorageURI = store.URI()

	// Create test context
	tc := &TestContext{
		Ctx:           ctx,
		Report:        report,
		Store:         store,
		PauseWhenFail: cfg.PauseWhenFail,
	}

	// Generate test data
	testData := make([]byte, cfg.TestDataSize)
	if _, err := rand.Read(testData); err != nil {
		return errors.Annotate(err, "failed to generate test data")
	}
	printStep("Generated test data: %s", formatBytes(cfg.TestDataSize))

	cleanup := func() {
		if !cfg.CleanupOnSuccess {
			return
		}
		printStep("Cleaning up test files...")
		_ = store.DeleteFile(ctx, testFileName1)
		_ = store.DeleteFile(ctx, testFileName2)
		_ = store.DeleteFile(ctx, testFileNameRenamed)
		_ = store.DeleteFile(ctx, filepath.Join(testDirName, testFileName1))
		_ = store.DeleteFile(ctx, filepath.Join(testDirName, testFileName2))
	}
	defer cleanup()

	// Run all tests
	testWriteFile(tc, testFileName1, testData)
	testFileExists(tc, testFileName1, true)
	testReadFile(tc, testFileName1, testData)
	testOpen(tc, testFileName1, testData)
	testOpenWithRange(tc, testFileName1, testData)
	testCreate(tc, testFileName2, testData)
	testRename(tc, testFileName2, testFileNameRenamed)
	testWalkDir(tc)
	testWalkDirWithSubDir(tc, testFileName1, testData)
	testWalkDirWithPagination(tc, testData)
	testDeleteFile(tc, testFileName1)
	testDeleteFiles(tc, testFileNameRenamed)
	testFileExists(tc, testFileName1, false)

	// Finalize report
	report.EndTime = time.Now()
	report.TotalBytes = int64(len(testData) * 4) // Approximate: write + read + create + range read

	// Print report
	report.Print()

	// Return error if any tests failed
	if report.FailedTests > 0 {
		return errors.New("storage test failed")
	}

	return nil
}

func testWriteFile(tc *TestContext, name string, data []byte) {
	testName := fmt.Sprintf("WriteFile(%s)", name)
	printStep("Test: %s", testName)
	start := time.Now()

	err := tc.Store.WriteFile(tc.Ctx, name, data)
	duration := time.Since(start)

	result := TestResult{
		Name:     testName,
		Duration: duration,
		Details:  fmt.Sprintf("Wrote %d bytes", len(data)),
	}

	if err != nil {
		printError("  ❌ Failed: %v", err)
		result.Passed = false
		result.Error = errors.Annotate(err, "WriteFile failed")
	} else {
		printSuccess("  ✓ Passed")
		result.Passed = true
	}

	tc.AddResult(result)
}

func testFileExists(tc *TestContext, name string, expected bool) {
	testName := fmt.Sprintf("FileExists(%s) - expecting %v", name, expected)
	printStep("Test: %s", testName)
	start := time.Now()

	exists, err := tc.Store.FileExists(tc.Ctx, name)
	duration := time.Since(start)

	result := TestResult{
		Name:     testName,
		Duration: duration,
	}

	if err != nil {
		printError("  ❌ Failed: %v", err)
		result.Passed = false
		result.Error = errors.Annotate(err, "FileExists failed")
	} else if exists != expected {
		printError("  ❌ Failed: expected %v, got %v", expected, exists)
		result.Passed = false
		result.Error = errors.Errorf("FileExists returned unexpected result: expected %v, got %v", expected, exists)
	} else {
		printSuccess("  ✓ Passed")
		result.Passed = true
		result.Details = fmt.Sprintf("File exists: %v", exists)
	}

	tc.AddResult(result)
}

func testReadFile(tc *TestContext, name string, expectedData []byte) {
	testName := fmt.Sprintf("ReadFile(%s)", name)
	printStep("Test: %s", testName)
	start := time.Now()

	data, err := tc.Store.ReadFile(tc.Ctx, name)
	duration := time.Since(start)

	result := TestResult{
		Name:     testName,
		Duration: duration,
	}

	if err != nil {
		printError("  ❌ Failed: %v", err)
		result.Passed = false
		result.Error = errors.Annotate(err, "ReadFile failed")
	} else if len(data) != len(expectedData) {
		printError("  ❌ Failed: expected size %d, got %d", len(expectedData), len(data))
		result.Passed = false
		result.Error = errors.Errorf("ReadFile returned wrong size: expected %d, got %d", len(expectedData), len(data))
	} else {
		// Verify data integrity
		dataValid := true
		for i := range data {
			if data[i] != expectedData[i] {
				printError("  ❌ Failed: data mismatch at offset %d", i)
				result.Passed = false
				result.Error = errors.Errorf("ReadFile returned corrupted data at offset %d", i)
				dataValid = false
				break
			}
		}
		if dataValid {
			printSuccess("  ✓ Passed (verified %d bytes)", len(data))
			result.Passed = true
			result.Details = fmt.Sprintf("Verified %d bytes", len(data))
		}
	}

	tc.AddResult(result)
}

func testOpen(tc *TestContext, name string, expectedData []byte) {
	testName := fmt.Sprintf("Open(%s) - streaming read", name)
	printStep("Test: %s", testName)
	start := time.Now()

	result := TestResult{
		Name: testName,
	}

	reader, err := tc.Store.Open(tc.Ctx, name, nil)
	if err != nil {
		printError("  ❌ Failed: %v", err)
		result.Duration = time.Since(start)
		result.Passed = false
		result.Error = errors.Annotate(err, "Open failed")
		tc.AddResult(result)
		return
	}
	defer reader.Close()

	// Check file size
	size, err := reader.GetFileSize()
	if err != nil {
		printError("  ❌ Failed to get file size: %v", err)
		result.Duration = time.Since(start)
		result.Passed = false
		result.Error = errors.Annotate(err, "GetFileSize failed")
		tc.AddResult(result)
		return
	}
	if size != int64(len(expectedData)) {
		printError("  ❌ Failed: expected size %d, got %d", len(expectedData), size)
		result.Duration = time.Since(start)
		result.Passed = false
		result.Error = errors.Errorf("GetFileSize returned wrong size: expected %d, got %d", len(expectedData), size)
		tc.AddResult(result)
		return
	}

	// Read all data
	data, err := io.ReadAll(reader)
	duration := time.Since(start)
	result.Duration = duration

	if err != nil {
		printError("  ❌ Failed to read: %v", err)
		result.Passed = false
		result.Error = errors.Annotate(err, "ReadAll failed")
	} else if len(data) != len(expectedData) {
		printError("  ❌ Failed: expected size %d, got %d", len(expectedData), len(data))
		result.Passed = false
		result.Error = errors.Errorf("Read returned wrong size: expected %d, got %d", len(expectedData), len(data))
	} else {
		printSuccess("  ✓ Passed (read %d bytes)", len(data))
		result.Passed = true
		result.Details = fmt.Sprintf("Read %d bytes", len(data))
	}

	tc.AddResult(result)
}

func testOpenWithRange(tc *TestContext, name string, expectedData []byte) {
	testName := fmt.Sprintf("Open(%s) - with range [100, 200)", name)
	printStep("Test: %s", testName)
	start := time.Now()

	startOffset := int64(100)
	endOffset := int64(200)
	reader, err := tc.Store.Open(tc.Ctx, name, &storage.ReaderOption{
		StartOffset: &startOffset,
		EndOffset:   &endOffset,
	})

	result := TestResult{
		Name: testName,
	}

	if err != nil {
		printError("  ❌ Failed: %v", err)
		result.Duration = time.Since(start)
		result.Passed = false
		result.Error = errors.Annotate(err, "Open with range failed")
		tc.AddResult(result)
		return
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	duration := time.Since(start)
	result.Duration = duration

	if err != nil {
		printError("  ❌ Failed to read: %v", err)
		result.Passed = false
		result.Error = errors.Annotate(err, "ReadAll failed")
		tc.AddResult(result)
		return
	}

	expectedSize := endOffset - startOffset
	if int64(len(data)) != expectedSize {
		printError("  ❌ Failed: expected size %d, got %d", expectedSize, len(data))
		result.Passed = false
		result.Error = errors.Errorf("Range read returned wrong size: expected %d, got %d", expectedSize, len(data))
	} else {
		// Verify data matches the expected range
		dataValid := true
		for i := range data {
			if data[i] != expectedData[startOffset+int64(i)] {
				printError("  ❌ Failed: data mismatch at offset %d", i)
				result.Passed = false
				result.Error = errors.Errorf("Range read returned corrupted data at offset %d", i)
				dataValid = false
				break
			}
		}
		if dataValid {
			printSuccess("  ✓ Passed (read %d bytes)", len(data))
			result.Passed = true
			result.Details = fmt.Sprintf("Read %d bytes from range [%d, %d)", len(data), startOffset, endOffset)
		}
	}

	tc.AddResult(result)
}

func testCreate(tc *TestContext, name string, data []byte) {
	testName := fmt.Sprintf("Create(%s) - streaming write", name)
	printStep("Test: %s", testName)
	start := time.Now()

	result := TestResult{
		Name: testName,
	}

	writer, err := tc.Store.Create(tc.Ctx, name, nil)
	if err != nil {
		printError("  ❌ Failed: %v", err)
		result.Duration = time.Since(start)
		result.Passed = false
		result.Error = errors.Annotate(err, "Create failed")
		tc.AddResult(result)
		return
	}

	// Write in chunks
	chunkSize := 64 * 1024 // 64KB chunks
	offset := 0
	for offset < len(data) {
		end := offset + chunkSize
		if end > len(data) {
			end = len(data)
		}
		n, err := writer.Write(tc.Ctx, data[offset:end])
		if err != nil {
			printError("  ❌ Failed to write: %v", err)
			_ = writer.Close(tc.Ctx)
			result.Duration = time.Since(start)
			result.Passed = false
			result.Error = errors.Annotate(err, "Write failed")
			tc.AddResult(result)
			return
		}
		offset += n
	}

	if err := writer.Close(tc.Ctx); err != nil {
		printError("  ❌ Failed to close: %v", err)
		result.Duration = time.Since(start)
		result.Passed = false
		result.Error = errors.Annotate(err, "Close writer failed")
		tc.AddResult(result)
		return
	}

	duration := time.Since(start)
	result.Duration = duration
	result.Passed = true
	result.Details = fmt.Sprintf("Wrote %d bytes in chunks", len(data))
	printSuccess("  ✓ Passed (wrote %d bytes)", len(data))

	tc.AddResult(result)
}

func testRename(tc *TestContext, oldName, newName string) {
	testName := fmt.Sprintf("Rename(%s -> %s)", oldName, newName)
	printStep("Test: %s", testName)
	start := time.Now()

	result := TestResult{
		Name: testName,
	}

	if err := tc.Store.Rename(tc.Ctx, oldName, newName); err != nil {
		printError("  ❌ Failed: %v", err)
		result.Duration = time.Since(start)
		result.Passed = false
		result.Error = errors.Annotate(err, "Rename failed")
		tc.AddResult(result)
		return
	}

	// Verify old file doesn't exist
	exists, err := tc.Store.FileExists(tc.Ctx, oldName)
	if err != nil {
		printError("  ❌ Failed to check old file: (%T) %v", err, err)
		result.Duration = time.Since(start)
		result.Passed = false
		result.Error = errors.Annotate(err, "FileExists check failed")
		tc.AddResult(result)
		return
	}
	if exists {
		printError("  ❌ Failed: old file still exists after rename")
		result.Duration = time.Since(start)
		result.Passed = false
		result.Error = errors.New("old file still exists after rename")
		tc.AddResult(result)
		return
	}

	// Verify new file exists
	exists, err = tc.Store.FileExists(tc.Ctx, newName)
	if err != nil {
		printError("  ❌ Failed to check new file: %v", err)
		result.Duration = time.Since(start)
		result.Passed = false
		result.Error = errors.Annotate(err, "FileExists check failed")
		tc.AddResult(result)
		return
	}
	if !exists {
		printError("  ❌ Failed: new file doesn't exist after rename")
		result.Duration = time.Since(start)
		result.Passed = false
		result.Error = errors.New("new file doesn't exist after rename")
		tc.AddResult(result)
		return
	}

	result.Duration = time.Since(start)
	result.Passed = true
	result.Details = "File renamed successfully"
	printSuccess("  ✓ Passed")

	tc.AddResult(result)
}

func testWalkDir(tc *TestContext) {
	testName := "WalkDir() - list all files"
	printStep("Test: %s", testName)
	start := time.Now()

	result := TestResult{
		Name: testName,
	}

	var fileCount int
	err := tc.Store.WalkDir(tc.Ctx, &storage.WalkOption{}, func(path string, size int64) error {
		fileCount++
		return nil
	})

	result.Duration = time.Since(start)

	if err != nil {
		printError("  ❌ Failed: %v", err)
		result.Passed = false
		result.Error = errors.Annotate(err, "WalkDir failed")
	} else {
		printSuccess("  ✓ Passed (found %d files)", fileCount)
		result.Passed = true
		result.Details = fmt.Sprintf("Found %d files", fileCount)
	}

	tc.AddResult(result)
}

func testWalkDirWithSubDir(tc *TestContext, testFile string, testData []byte) {
	testName := "WalkDir() - with subdirectory"
	printStep("Test: %s", testName)
	start := time.Now()

	result := TestResult{
		Name: testName,
	}

	// Create test files in subdirectory
	subFile1 := filepath.Join(testDirName, testFile)
	subFile2 := filepath.Join(testDirName, "file2.tmp")

	if err := tc.Store.WriteFile(tc.Ctx, subFile1, testData[:512]); err != nil {
		printError("  ❌ Failed to create test file in subdir: %v", err)
		result.Duration = time.Since(start)
		result.Passed = false
		result.Error = errors.Annotate(err, "WriteFile in subdir failed")
		tc.AddResult(result)
		return
	}
	if err := tc.Store.WriteFile(tc.Ctx, subFile2, testData[:256]); err != nil {
		printError("  ❌ Failed to create test file in subdir: %v", err)
		result.Duration = time.Since(start)
		result.Passed = false
		result.Error = errors.Annotate(err, "WriteFile in subdir failed")
		tc.AddResult(result)
		return
	}

	// Walk the subdirectory
	var fileCount int
	err := tc.Store.WalkDir(tc.Ctx, &storage.WalkOption{
		SubDir: testDirName,
	}, func(path string, size int64) error {
		fileCount++
		return nil
	})

	result.Duration = time.Since(start)

	if err != nil {
		printError("  ❌ Failed: %v", err)
		result.Passed = false
		result.Error = errors.Annotate(err, "WalkDir with subdir failed")
	} else if fileCount < 2 {
		printError("  ❌ Failed: expected at least 2 files in subdir, got %d", fileCount)
		result.Passed = false
		result.Error = errors.Errorf("WalkDir found insufficient files: expected at least 2, got %d", fileCount)
	} else {
		printSuccess("  ✓ Passed (found %d files in subdirectory)", fileCount)
		result.Passed = true
		result.Details = fmt.Sprintf("Found %d files in subdirectory '%s'", fileCount, testDirName)
	}

	tc.AddResult(result)
}

func testWalkDirWithPagination(tc *TestContext, testData []byte) {
	testName := "WalkDir() - with pagination (small ListCount)"
	printStep("Test: %s", testName)
	start := time.Now()

	result := TestResult{
		Name: testName,
	}

	// Create multiple test files to test pagination
	paginationTestDir := "br-test-pagination"
	numFiles := 10
	fileNames := make([]string, numFiles)

	for i := range numFiles {
		fileName := filepath.Join(paginationTestDir, fmt.Sprintf("pagefile-%03d.tmp", i))
		fileNames[i] = fileName
		if err := tc.Store.WriteFile(tc.Ctx, fileName, testData[:1024]); err != nil {
			printError("  ❌ Failed to create test file %s: %v", fileName, err)
			result.Duration = time.Since(start)
			result.Passed = false
			result.Error = errors.Annotatef(err, "WriteFile %s failed", fileName)
			tc.AddResult(result)
			return
		}
	}

	// Cleanup pagination test files
	defer func() {
		for _, name := range fileNames {
			_ = tc.Store.DeleteFile(tc.Ctx, name)
		}
	}()

	// Test pagination with small page size
	// This forces WalkDir to make multiple internal requests
	pageSize := int64(3)

	opt := &storage.WalkOption{
		SubDir:    paginationTestDir,
		ListCount: pageSize, // Small page size to force multiple internal pages
	}

	var collectedFiles []string
	err := tc.Store.WalkDir(tc.Ctx, opt, func(path string, size int64) error {
		collectedFiles = append(collectedFiles, path)
		return nil
	})

	result.Duration = time.Since(start)

	if err != nil {
		printError("  ❌ Failed: %v", err)
		result.Passed = false
		result.Error = errors.Annotate(err, "WalkDir with pagination failed")
		tc.AddResult(result)
		return
	}

	if len(collectedFiles) != numFiles {
		printError("  ❌ Failed: expected %d files total, got %d", numFiles, len(collectedFiles))
		result.Passed = false
		result.Error = errors.Errorf("WalkDir pagination returned wrong file count: expected %d, got %d", numFiles, len(collectedFiles))
		tc.AddResult(result)
		return
	}

	// Verify all files are unique
	fileSet := make(map[string]bool)
	for _, file := range collectedFiles {
		if fileSet[file] {
			printError("  ❌ Failed: duplicate file %s found", file)
			result.Passed = false
			result.Error = errors.Errorf("WalkDir pagination returned duplicate file: %s", file)
			tc.AddResult(result)
			return
		}
		fileSet[file] = true
	}

	printSuccess("  ✓ Passed (retrieved %d unique files with ListCount=%d)", len(collectedFiles), pageSize)
	result.Passed = true
	result.Details = fmt.Sprintf("Retrieved %d unique files with ListCount=%d", len(collectedFiles), pageSize)

	tc.AddResult(result)
}

func testDeleteFile(tc *TestContext, name string) {
	testName := fmt.Sprintf("DeleteFile(%s)", name)
	printStep("Test: %s", testName)
	start := time.Now()

	result := TestResult{
		Name: testName,
	}

	if err := tc.Store.DeleteFile(tc.Ctx, name); err != nil {
		printError("  ❌ Failed: %v", err)
		result.Duration = time.Since(start)
		result.Passed = false
		result.Error = errors.Annotate(err, "DeleteFile failed")
		tc.AddResult(result)
		return
	}

	// Verify file is deleted
	exists, err := tc.Store.FileExists(tc.Ctx, name)
	result.Duration = time.Since(start)

	if err != nil {
		printError("  ❌ Failed to verify deletion: %v", err)
		result.Passed = false
		result.Error = errors.Annotate(err, "FileExists check failed")
	} else if exists {
		printError("  ❌ Failed: file still exists after deletion")
		result.Passed = false
		result.Error = errors.New("file still exists after deletion")
	} else {
		printSuccess("  ✓ Passed")
		result.Passed = true
		result.Details = "File deleted successfully"
	}

	tc.AddResult(result)
}

func testDeleteFiles(tc *TestContext, names ...string) {
	testName := fmt.Sprintf("DeleteFiles() - batch delete %d files", len(names))
	printStep("Test: %s", testName)
	start := time.Now()

	result := TestResult{
		Name: testName,
	}

	if err := tc.Store.DeleteFiles(tc.Ctx, names); err != nil {
		printError("  ❌ Failed: %v", err)
		result.Duration = time.Since(start)
		result.Passed = false
		result.Error = errors.Annotate(err, "DeleteFiles failed")
		tc.AddResult(result)
		return
	}

	// Verify files are deleted
	for _, name := range names {
		exists, err := tc.Store.FileExists(tc.Ctx, name)
		if err != nil {
			printError("  ❌ Failed to verify deletion of %s: (%T) %v", name, err, err)
			result.Duration = time.Since(start)
			result.Passed = false
			result.Error = errors.Annotatef(err, "FileExists check failed for %s", name)
			tc.AddResult(result)
			return
		}
		if exists {
			printError("  ❌ Failed: file %s still exists after deletion", name)
			result.Duration = time.Since(start)
			result.Passed = false
			result.Error = errors.Errorf("file %s still exists after deletion", name)
			tc.AddResult(result)
			return
		}
	}

	result.Duration = time.Since(start)
	result.Passed = true
	result.Details = fmt.Sprintf("Deleted %d files", len(names))
	printSuccess("  ✓ Passed")

	tc.AddResult(result)
}

func printStep(format string, args ...any) {
	fmt.Fprintf(os.Stdout, color.CyanString("► "+format+"\n"), args...)
}

func printSuccess(format string, args ...any) {
	fmt.Fprintf(os.Stdout, color.GreenString(format+"\n"), args...)
}

func printError(format string, args ...any) {
	fmt.Fprintf(os.Stderr, color.RedString(format+"\n"), args...)
}
