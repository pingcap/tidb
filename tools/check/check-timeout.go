package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

var allowList = make(map[string]struct{})

func init() {
	tmp := []string{
		"TestT",
		"TestCluster",
		"testIntegrationSuite4.TestAddPartitionTooManyPartitions",
		"testIntegrationSuite5.TestBackwardCompatibility",
		"testIntegrationSuite1.TestDisableTablePartition",
		"testPartitionSuite.TestAddPartitionReplicaBiggerThanTiFlashStores",
		"testColumnTypeChangeSuite.TestColumnTypeChangeFromJsonToOthers",
		"testColumnTypeChangeSuite.TestColumnTypeChangeFromNumericToOthers",
		"testColumnTypeChangeSuite.TestColumnTypeChangeFromStringToOthers",
		"testCTCSerialSuiteWrapper.TestColumnTypeChangeFromJsonToOthers",
		"testCTCSerialSuiteWrapper.TestColumnTypeChangeFromNumericToOthers",
		"testCTCSerialSuiteWrapper.TestColumnTypeChangeFromStringToOthers",
		"testSerialDBSuite.TestCommitTxnWithIndexChange",
		"testSerialDBSuite.TestDuplicateErrorMessage",
		"testFailDBSuite.TestAddIndexFailed",
		"testFailDBSuite.TestAddIndexWorkerNum",
		"pkgTestSuite.TestAggPartialResultMapperB",
		"testFastAnalyze.TestFastAnalyzeRetryRowCount",
		"testSuite2.TestAddIndexPriority",
		"testSuite3.TestAdminCheckPartitionTableFailed",
		"testSuite.TestApplyColumnType",
		"testSuiteJoin1.TestIndexLookupJoin",
		"testSuiteJoin2.TestJoin",
		"testSuite8.TestAdminCheckTable",
		"testSuiteAgg.TestAggregation",
		"testSuiteJoin3.TestSubquery",
		"testSuiteJoin2.TestJoinCast",
		"testSuite2.TestExplainAnalyzeCTEMemoryAndDiskInfo",
		"testSuite7.TestSetWithCurrentTimestampAndNow",
		"testSuite5.TestPartitionTableIndexJoinAndIndexReader",
		"testSuite4.TestWriteListColumnsPartitionTable1",
		"testSuite2.TestLowResolutionTSORead",
		"testSuite1.TestPartitionTableRandomIndexMerge",
		"testSuite3.TestPartitionTableIndexJoinIndexLookUp",
		"testSuite4.TestWriteListPartitionTable2",
		"partitionTableSuite.TestDML",
		"partitionTableSuite.TestDirectReadingWithAgg",
		"partitionTableSuite.TestDirectReadingWithUnionScan",
		"partitionTableSuite.TestOrderByandLimit",
		"partitionTableSuite.TestParallelApply",
		"partitionTableSuite.TestSubqueries",
		"partitionTableSuite.TestView",
		"partitionTableSuite.TestUnsignedPartitionColumn",
		"partitionTableSuite.TestGlobalStatsAndSQLBinding",
		"partitionTableSuite.TestIdexMerge",
		"partitionTableSuite.TestUnion",
		"testRecoverTable.TestFlashbackTable",
		"testRecoverTable.TestRecoverTable",
		"tiflashTestSuite.TestCancelMppTasks",
		"tiflashTestSuite.TestMppApply",
		"tiflashTestSuite.TestMppExecution",
		"tiflashTestSuite.TestMppUnionAll",
		"tiflashTestSuite.TestPartitionTable",
		"testSerialSuite.TestPrepareStmtAfterIsolationReadChange",
		"testSerialSuite.TestSplitRegionTimeout",
		"testSerialSuite.TestAggInDisk",
		"testStaleTxnSerialSuite.TestSelectAsOf",
		"testEvaluatorSuite.TestSleepVectorized",
		"testSuite.TestFailNewSession",
		"testSuite.TestFailNewSession",
		"testPlanSerialSuite.TestPartitionTable",
		"testPlanSerialSuite.TestPartitionWithVariedDatasources",
		"HTTPHandlerTestSuite.TestZipInfoForSQL",
		"HTTPHandlerTestSuite.TestBinlogRecover",
		"ConnTestSuite.TestConnExecutionTimeout",
		"ConnTestSuite.TestTiFlashFallback",
		"tidbTestTopSQLSuite.TestTopSQLCPUProfile",
		"testPessimisticSuite.TestAmendForIndexChange",
		"testPessimisticSuite.TestGenerateColPointGet",
		"testPessimisticSuite.TestInnodbLockWaitTimeout",
		"testPessimisticSuite.TestPessimisticLockNonExistsKey",
		"testPessimisticSuite.TestSelectForUpdateNoWait",
		"testSessionSerialSuite.TestProcessInfoIssue22068",
		"testKVSuite.TestRetryOpenStore",
		"testBatchCopSuite.TestStoreErr",
		"testBatchCopSuite.TestStoreSwitchPeer",
		"testSequenceSuite.TestSequenceFunction",
		"testSuiteP2.TestUnion",
		"testVectorizeSuite1.TestVectorizedBuiltinTimeFuncGenerated",
		"testSuiteJoin3.TestVectorizedShuffleMergeJoin",
		"testSuite1.TestClusterIndexAnalyze",
		"testSuiteJoin3.TestVectorizedMergeJoin",
		"testFastAnalyze.TestAnalyzeFastSample",
		"testBypassSuite.TestLatch",
		"testIntegrationSuite2.TestPartitionCancelAddPrimaryKey",
		"testDBSuite1.TestAddIndexWithSplitTable",
		"testSerialDBSuite.TestAddIndexWithShardRowID",
	}
	for _, v := range tmp {
		allowList[v] = struct{}{}
	}
}

func inAllowList(testName string) bool {
	_, ok := allowList[testName]
	return ok
}

func parseLine(line string) (testName string, dur time.Duration, err error) {
	// The line looks like that:
	// PASS: sysvar_test.go:131: testSysVarSuite.TestIntValidation	0.000s
	// PASS: sysvar_test.go:454: testSysVarSuite.TestIsNoop	0.000s
	// PASS: sysvar_test.go:302: testSysVarSuite.TestMaxExecutionTime	0.000s
	// PASS: sysvar_test.go:429: testSysVarSuite.TestReadOnlyNoop	0.000s
	// --- PASS: TestSingle (0.26s)
	// --- PASS: TestCluster (4.20s)

	line = strings.TrimSpace(line)

	// Format type 1
	if strings.HasPrefix(line, "PASS") {
		return parseFormat1(line)
	}

	// Format type 2
	if strings.HasPrefix(line, "---") {
		return parseFormat2(line)
	}

	err = errors.New("unknown format: " + line)
	return
}

func parseFormat1(line string) (testName string, dur time.Duration, err error) {
	offset := strings.LastIndexByte(line, '\t')
	if offset < 0 {
		err = fmt.Errorf("get duration string error: %s", line)
		return
	}
	durStr := line[offset+1:]
	dur, err = time.ParseDuration(durStr)
	if err != nil {
		err = fmt.Errorf("parse duration string error: %s, %v", line, err)
		return
	}

	offset1 := strings.LastIndexByte(line[:offset], ' ')
	if offset1 < 0 {
		err = errors.New("parse line error: " + line)
		return
	}
	testName = line[offset1+1 : offset]
	return
}

func parseFormat2(line string) (testName string, dur time.Duration, err error) {
	offset := strings.LastIndexByte(line, ' ')
	if offset < 0 {
		err = fmt.Errorf("get duration string error: %s", line)
		return
	}
	durStr := line[offset+2 : len(line)-1]
	dur, err = time.ParseDuration(durStr)
	if err != nil {
		err = fmt.Errorf("parse duration string error: %s, %v", line, err)
		return
	}

	offset1 := strings.LastIndexByte(line[:offset], ' ')
	if offset1 < 0 {
		err = errors.New("parse line err: " + line)
		return
	}
	testName = line[offset1+1 : offset]
	return
}

func main() {
	lines := make([]string, 0, 100)
	// They are generated by "grep 'PASS:' gotest.log", and gotest.log is generated by
	// 'make gotest' from the Makefile which runs 'go test -v ./...' alike
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		testName, dur, err := parseLine(line)
		if err != nil {
			fmt.Println("parser line error:", err)
			os.Exit(-1)
		}
		if dur > 5*time.Second {
			if inAllowList(testName) {
				continue
			}
			lines = append(lines, line)
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading standard input:", err)
		os.Exit(-3)
	}

	if len(lines) != 0 {
		fmt.Println("The following test cases take too long to finish:")
		for _, line := range lines {
			fmt.Println(line)
		}
		os.Exit(-4)
	}
}
