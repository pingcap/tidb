package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"
)

var allowList = make(map[string]struct{})

func init() {
	tmp := []string{
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
		"testPrivilegeSuite.TestSecurityEnhancedModeInfoschema",
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
	}
	for _, v := range tmp {
		allowList[v] = struct{}{}
	}
}

func inAllowList(input string) bool {
	// The input looks like this.
	// PASS: sysvar_test.go:131: testSysVarSuite.TestIntValidation
	offset := strings.LastIndexByte(input, ' ')
	if offset < 0 {
		return false
	}
	testName := input[offset+1:]
	_, ok := allowList[testName]
	return ok
}

func main() {
	lines := make([]string, 0, 100)
	// The input looks like that:
	// PASS: sysvar_test.go:131: testSysVarSuite.TestIntValidation	0.000s
	// PASS: sysvar_test.go:454: testSysVarSuite.TestIsNoop	0.000s
	// PASS: sysvar_test.go:302: testSysVarSuite.TestMaxExecutionTime	0.000s
	// PASS: sysvar_test.go:429: testSysVarSuite.TestReadOnlyNoop	0.000s
	// PASS: sysvar_test.go:97: testSysVarSuite.TestRegistrationOfNewSysVar	0.000s
	// PASS: sysvar_test.go:269: testSysVarSuite.TestSQLModeVar	0.000s
	// PASS: sysvar_test.go:254: testSysVarSuite.TestSQLSelectLimit	0.000s
	// PASS: sysvar_test.go:224: testSysVarSuite.TestScope	0.000s
	// PASS: sysvar_test.go:439: testSysVarSuite.TestSkipInit	0.000s
	//
	// They are generated by "grep '^:PASS' gotest.log", and gotest.log is generated by
	// 'make gotest' from the Makefile which runs 'go test -v ./...' alike
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		offset := strings.LastIndexByte(line, '\t')
		if offset < 0 {
			fmt.Println("get duration string error:", line)
			os.Exit(-1)
		}
		durStr := line[offset+1:]
		dur, err := time.ParseDuration(durStr)
		if err != nil {
			fmt.Println("parse duration string error:", line, err)
			os.Exit(-2)
		}

		if dur > 4*time.Second {
			if inAllowList(line[:offset]) {
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
