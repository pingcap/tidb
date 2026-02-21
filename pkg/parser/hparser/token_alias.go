// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package hparser

// Token aliases mapping token constants to readable names.
// These values MUST match the constants in the generated parser.go.
//
// This file exists so hparser can reference token types without importing
// the parent parser package (which would be a circular dependency).
const (
	// Core literals and markers.
	tokBuiltins           = 58142
	tokIdentifier         = 57346
	tokRole               = 57877
	tokNever              = 57796
	tokSqlTsiDay          = 57917
	tokSqlTsiHour         = 57918
	tokSqlTsiMinute       = 57919
	tokSqlTsiMonth        = 57920
	tokSqlTsiQuarter      = 57921
	tokSqlTsiSecond       = 57922
	tokSqlTsiWeek         = 57923
	tokSqlTsiYear         = 57924
	tokUtcDate            = 57577
	tokUtcTime            = 57578
	tokUtcTimestamp       = 57579
	tokStringLit          = 57353
	tokSingleAtIdentifier = 57354
	tokDoubleAtIdentifier = 57355
	tokInvalid            = 57356
	tokHintComment        = 57357
	tokAndand             = 57358 // &&
	tokPipes              = 57359 // ||
	tokUnderscoreCS       = 57352
	tokParamMarker        = 58211
	tokIntLit             = 58197
	tokFloatLit           = 58195
	tokDecLit             = 58196
	tokHexLit             = 58198
	tokBitLit             = 58199

	// Operator tokens.
	tokEq           = 58202 // = (equality comparison)
	tokAssignmentEq = 58201
	tokGe           = 58203 // >=
	tokLe           = 58204 // <=
	tokNeq          = 58208 // !=
	tokNeqSynonym   = 58209 // <>
	tokNulleq       = 58210 // <=>
	tokLsh          = 58207 // <<
	tokRsh          = 58212 // >>
	tokJss          = 58205 // ->
	tokJuss         = 58206 // ->>
	tokNot2         = 58213 // NOT (high precedence)
	tokPipesAsOr    = 57835 // || when PIPES_AS_CONCAT is off

	// DML keywords.
	tokSelect    = 57540
	tokInsert    = 57453
	tokUpdate    = 57573
	tokDelete    = 57407
	tokReplace   = 57530
	tokFrom      = 57434
	tokWhere     = 57587
	tokSet       = 57541
	tokInto      = 57463
	tokValues    = 57580
	tokAs        = 57369
	tokOn        = 57505
	tokUsing     = 57576
	tokLimit     = 57477
	tokOffset    = 57811
	tokOrder     = 57510
	tokBy        = 57376
	tokGroup     = 57438
	tokHaving    = 57440
	tokDistinct  = 57411
	tokAll       = 57364
	tokDual      = 57416
	tokIgnore    = 57446
	tokSeparator = 57895

	// Join keywords.
	tokJoin     = 57466
	tokLeft     = 57475
	tokRight    = 57534
	tokInner    = 57451
	tokOuter    = 57512
	tokCross    = 57390
	tokNatural  = 57497
	tokNational = 57794
	tokNChar    = 57795
	tokNVarchar = 57809

	// Logical operators.
	tokAnd = 57367
	tokOr  = 57509
	tokNot = 57498
	tokXor = 57592
	tokIn  = 57448

	// Comparison / pattern operators.
	tokLike     = 57476
	tokBetween  = 57371
	tokRegexp   = 57526
	tokRlike    = 57535
	tokIs       = 57464
	tokExists   = 57422
	tokNull     = 57502
	tokTrue     = 57567
	tokFalse    = 57425
	tokDefault  = 57405
	tokCollate  = 57384
	tokInterval = 57462
	tokDiv      = 57413 // integer division
	tokMod      = 57496

	// Set operations.
	tokUnion     = 57568
	tokIntersect = 57461
	tokExcept    = 57421

	// CASE expression.
	tokCase = 57379
	tokWhen = 57586
	tokThen = 57559
	tokElse = 57417
	tokEnd  = 57701

	// Subquery modifiers.
	tokAny  = 57605
	tokSome = 57912

	// DDL keywords.
	tokCreate     = 57389
	tokDrop       = 57415
	tokAlter      = 57365
	tokTable      = 57556
	tokIndex      = 57449
	tokDatabase   = 57398
	tokKey        = 57467
	tokPrimary    = 57518
	tokUnique     = 57569
	tokForeign    = 57433
	tokReferences = 57525
	tokConstraint = 57386
	tokCheck      = 57383
	tokFulltext   = 57435
	tokSpatial    = 57544
	tokAdd        = 57363
	tokChange     = 57380
	tokModify     = 57791
	tokRename     = 57528
	tokAfter      = 57600
	tokFirst      = 57724
	tokUser       = 57975
	tokIdentified = 57743
	tokPrivileges = 57843

	tokAvgRowLength             = 57616
	tokCompression              = 57660
	tokEngine                   = 57703
	tokMaxRows                  = 57779
	tokMinRows                  = 57789
	tokRowFormat                = 57882
	tokChecksum                 = 57641
	tokDelayKeyWrite            = 57686
	tokPackKeys                 = 57820
	tokStatsAutoRecalc          = 57926
	tokStatsPersistent          = 57930
	tokStatsSamplePages         = 57931
	tokStatsBuckets             = 58183
	tokStatsTopN                = 58190
	tokStatsSampleRate          = 57932
	tokStatsColChoice           = 57927
	tokStatsColList             = 57928
	tokInsertMethod             = 57751
	tokEncryption               = 57698
	tokSecondaryEngine          = 57889
	tokStorage                  = 57934
	tokTablespace               = 57945
	tokNodegroup                = 57802
	tokData                     = 57679
	tokPreSplitRegions          = 57842
	tokPageChecksum             = 57822
	tokPageCompressed           = 57823
	tokPageCompressionLevel     = 57824
	tokTransactional            = 57961
	tokSecondaryEngineAttribute = 57890
	tokAutoextendSize           = 57610
	tokAutoIdCache              = 57611
	tokDirectory                = 57688

	// Window function keywords.
	tokWindow    = 57589
	tokOver      = 57514
	tokPartition = 57515
	tokRows      = 57537
	tokGroups    = 57439
	tokUnbounded = 57969
	tokPreceding = 57839
	tokFollowing = 57727

	// Sort keywords.
	tokAsc  = 57370
	tokDesc = 57409

	// Transaction keywords.
	tokBegin       = 57621
	tokCommit      = 57656
	tokRollback    = 57878
	tokChain       = 57638
	tokRelease     = 57527
	tokNo          = 57799
	tokSavepoint   = 57886
	tokStart       = 57925
	tokTransaction = 57960
	tokOnly        = 57816
	tokPessimistic = 58172
	tokOptimistic  = 58171
	tokConsistent  = 57667
	tokSnapshot    = 57911
	tokCausal      = 57637
	tokConsistency = 57666

	// Trivial statement keywords.
	tokDo       = 57693
	tokShutdown = 57904
	tokRestart  = 57871
	tokHelp     = 57737

	// Administrative.
	tokShow          = 57542
	tokUse           = 57575
	tokExplain       = 57424
	tokDescribe      = 57410
	tokPrepare       = 57840
	tokExecute       = 57715
	tokDeallocate    = 57683
	tokGrant         = 57437
	tokRevoke        = 57533
	tokDatabases     = 57399
	tokLock          = 57483
	tokUnlock        = 57570
	tokLoad          = 57480
	tokBinlog        = 57626
	tokFlush         = 57726
	tokSplit         = 58180
	tokRegion        = 58173
	tokRegions       = 58174
	tokNextRowID     = 58051
	tokDistributions = 58163
	tokCall          = 57377
	tokBatch         = 58123
	tokDuplicate     = 57694
	tokDry           = 58164
	tokRun           = 58176

	// Distribute Table tokens
	tokDistribute   = 58161
	tokRule         = 57884
	tokTimeout      = 57953
	tokDistribution = 58162
	// Common.
	tokFor = 57431
	tokIf  = 57445

	// Data Types.
	tokBit        = 57627
	tokBitAnd     = 58127 // builtinBitAnd in the parser (NOT 57628 which is 'block')
	tokTinyInt    = 57562
	tokSmallInt   = 57543
	tokMediumInt  = 57491
	tokInt        = 57454
	tokInt1       = 57455
	tokInt2       = 57456
	tokInt3       = 57457
	tokInt4       = 57458
	tokInt8       = 57459
	tokInteger    = 57460
	tokBigInt     = 57372
	tokReal       = 57523
	tokDouble     = 57414
	tokFloat      = 57428
	tokFloat4     = 57429
	tokFloat8     = 57430
	tokDecimal    = 57404
	tokNumeric    = 57503
	tokBool       = 57629
	tokBoolean    = 57630
	tokDate       = 57680
	tokDatetime   = 57681
	tokTimestamp  = 57954
	tokTime       = 57952
	tokYear       = 57991
	tokChar       = 57381
	tokVarchar    = 57582
	tokVarying    = 57584
	tokBinary     = 57373
	tokVarbinary  = 57581
	tokTinyBlob   = 57561
	tokBlob       = 57374
	tokExplore    = 57718
	tokMediumBlob = 57490
	tokMiddleInt  = 57493
	tokLongBlob   = 57485
	tokTinyText   = 57563
	tokText       = 57949
	tokMediumText = 57492
	tokLongText   = 57486
	tokLong       = 57484

	tokEnum     = 57706
	tokSetType  = 57541 // Same as tokSet keyword
	tokJson     = 57759 // jsonType
	tokGeometry = 57544 // spatial keyword used for GEOMETRY type
	tokPoint    = 57837
	// tokLinestring/tokPolygon are not defined as tokens in parser.go (parsed as identifiers)

	// Modifiers
	tokCharsetKwd = 57639
	tokPrecision  = 57517

	tokAutoInc   = 57612 // autoIncrement
	tokComment   = 57655
	tokGenerated = 57436
	tokAlways    = 57604
	tokEnforced  = 57702

	tokBtree        = 57631
	tokHash         = 57736
	tokInvisible    = 57753
	tokKeyBlockSize = 57760
	tokParser       = 57825
	tokVisible      = 57981
	tokClustered    = 57649
	tokNonClustered = 57805

	tokSigned   = 57905
	tokUnsigned = 57571
	tokZerofill = 57594

	tokWith      = 57590
	tokRecursive = 57524
	tokTo        = 57564
	tokMatch     = 57488
	tokAgainst   = 57601

	// Type keywords.
	tokBinaryType    = 57373
	tokCharType      = 57381
	tokIntType       = 57454
	tokVarcharType   = 57582
	tokDateType      = 57680
	tokDatetimeType  = 57681
	tokTimestampType = 57954
	tokTimeType      = 57952
	tokYearType      = 57991

	// Special function tokens.
	tokCast      = 58003
	tokConvert   = 57388
	tokExtract   = 58020
	tokTrim      = 58108
	tokPosition  = 58057
	tokGetFormat = 58027
	tokCurDate   = 58008
	tokCurTime   = 58009
	tokNow       = 58052
	tokAddDate   = 57992
	tokSubDate   = 58085
	tokDateAdd   = 58010
	tokDateSub   = 58011

	// Time functions.
	tokTimestampDiff = 58095
	tokEscape        = 57709

	// Priority modifiers.
	tokLowPriority  = 57487
	tokHighPriority = 57441
	tokDelayed      = 57406

	// SELECT modifiers.
	tokSQLCalcFoundRows = 57550
	tokStraightJoin     = 57555
	tokDistinctRow      = 57412
	tokSQLBigResult     = 57549
	tokSQLSmallResult   = 57551
	tokSQLBufferResult  = 57914
	tokSQLCache         = 57915
	tokSQLNoCache       = 57916

	// Current* pseudofunctions.
	tokCurrentDate = 57392
	tokCurrentTime = 57394
	tokCurrentTs   = 57395
	tokCurrentUser = 57396
	tokCurrentRole = 57393

	// Keyword-as-function tokens.
	tokCoalesce             = 57650
	tokTables               = 57944
	tokRead                 = 57522
	tokWrite                = 57591
	tokInfile               = 57450
	tokTerminated           = 57558
	tokEnclosed             = 57419
	tokEscaped              = 57420
	tokStarting             = 57553
	tokLines                = 57479
	tokFields               = 57722
	tokOptionally           = 57508
	tokOptionallyEnclosedBy = 57351
	tokDefined              = 58012
	tokColumn               = 57385
	tokFormat               = 57728

	// Additional keywords for LOAD DATA / IMPORT INTO / BRIE / REFRESH.
	tokRefresh = 57859
	tokJob     = 58166
	tokReload  = 57860 // Corrected from 57922
	tokTLS     = 58096
	tokJobs    = 58167
	tokError   = 57707

	// TimeUnit keywords (simple).
	tokMicrosecond = 57786
	tokSecond      = 57887
	tokMinute      = 57787
	tokHour        = 57741
	tokDay         = 57682
	tokWeek        = 57985
	tokMonth       = 57792
	tokQuarter     = 57850

	// TimeUnit keywords (compound).
	tokSecondMicrosecond = 57539
	tokMinuteMicrosecond = 57494
	tokMinuteSecond      = 57495
	tokHourMicrosecond   = 57442
	tokHourSecond        = 57444
	tokHourMinute        = 57443
	tokDayMicrosecond    = 57401
	tokDaySecond         = 57403
	tokDayMinute         = 57402
	tokDayHour           = 57400
	tokYearMonth         = 57593

	// CastType keywords (Consolidated above)

	// TrimDirection keywords.
	tokBoth     = 57375
	tokLeading  = 57473
	tokTrailing = 57565

	// DDL/Admin additional keywords.
	tokGlobal         = 57733
	tokSession        = 57899
	tokLocal          = 57770
	tokInstance       = 57752
	tokNames          = 57793
	tokPassword       = 57829
	tokConfig         = 57664
	tokIsolation      = 57757
	tokLevel          = 57767
	tokUncommitted    = 57970
	tokCommitted      = 57657
	tokRepeatable     = 57864
	tokSerializable   = 57898
	tokTemporary      = 57947
	tokView           = 57980
	tokColumns        = 57653
	tokVariables      = 57978
	tokStatus         = 57933
	tokWarnings       = 57984
	tokProcesslist    = 57845
	tokFull           = 57730
	tokCascade        = 57378
	tokRestrict       = 57532
	tokCharacter      = 57382
	tokAnalyze        = 57366
	tokTruncate       = 57963
	tokConnection     = 57665
	tokShardRowIDBits = 57901

	// LOCK option values.
	tokNone      = 57806
	tokShared    = 57903
	tokExclusive = 57714

	// STATS_EXTENDED and sub-options.
	tokStatsExtended = 58185
	tokCardinality   = 58154
	tokCorrelation   = 58157
	tokDependency    = 58159

	// TTL keywords.
	tokTTL            = 57965
	tokTTLEnable      = 57966
	tokTTLJobInterval = 57967
	tokRemove         = 57861

	// Partitioning keywords.
	tokLinear       = 57478
	tokPartitions   = 57828
	tokPartitioning = 57827
	tokAlgorithm    = 57603
	tokDefiner      = 57685
	tokSecurity     = 57893
	tokOption       = 57507
	tokMerge        = 57785
	tokTemptable    = 57948
	tokInvoker      = 57754
	tokUndefined    = 57971
	tokCascaded     = 57636
	tokRange        = 57520 // rangeKwd
	tokList         = 57768
	tokLess         = 57766
	tokThan         = 57950

	// SELECT lock keywords.
	tokShare  = 57902
	tokNowait = 57807
	tokSkip   = 57907
	tokLocked = 57772
	tokWait   = 57982

	// DELETE modifiers.
	tokQuick = 57853

	// Generated column keywords.
	tokVirtual = 57585
	tokStored  = 57554

	// VECTOR INDEX and COLUMNAR INDEX.
	tokVector   = 57979
	tokColumnar = 57652

	// Storage keywords.
	tokDisk   = 57692
	tokMemory = 57784

	// AUTO_RANDOM.
	tokAutoRandom = 57613

	// PLACEMENT, SEQUENCE, FLASHBACK, etc.
	tokFlashback       = 58021
	tokCluster         = 57648 // CLUSTER keyword in FLASHBACK CLUSTER
	tokToTimestamp     = 57348 // fused "TO TIMESTAMP" compound token
	tokToTSO           = 57349 // fused "TO TSO" compound token
	tokSerial          = 57897 // SERIAL pseudo-type (handled at ColumnDef level)
	tokOptimize        = 57506
	tokRepair          = 57863
	tokCompact         = 57658
	tokCancel          = 58153
	tokCapture         = 57635
	tokTraffic         = 58107
	tokPause           = 57831
	tokResume          = 57874
	tokStop            = 58082
	tokPurge           = 57849
	tokBR              = 58000
	tokReorganize      = 57862
	tokSecondaryLoad   = 57891
	tokSecondaryUnload = 57892
	tokStatsOptions    = 57929
	tokColumnFormat    = 57654
	tokType            = 57968
	tokRecover         = 57857

	// Import / Resource Group.
	tokImport     = 57746
	tokDiscard    = 57691
	tokEnable     = 57696
	tokDisable    = 57689
	tokExchange   = 57713
	tokWithout    = 57987
	tokValidation = 57976

	// CURRENT keyword.
	tokCurrent = 57677

	// Variable / Assignment.
	tokAt            = 57819
	tokSingleAtIdent = 57354

	// Plan replayer / Query watch.
	tokPlan            = 58056
	tokReplayer        = 58066
	tokFixed           = 57725
	tokQuery           = 57852
	tokWatch           = 58121
	tokAction          = 57596
	tokDump            = 58015
	tokOf              = 57504
	tokNoWriteToBinLog = 57499
	tokReplica         = 57865
	tokSql             = 57545
	tokStatsKwd        = 58182
	tokDigest          = 57687
	tokAsOf            = 57347
	tokTiFlash         = 58192

	// Misc admin / placement policy.
	tokDuration   = 58093
	tokRestore    = 57872
	tokBackup     = 57618
	tokPolicy     = 57838
	tokAdmin      = 58122
	tokSchema     = 57398 // alias for DATABASE
	tokRecommend  = 57856
	tokTrace      = 57958
	tokGrants     = 57734
	tokBinding    = 57623
	tokStatistics = 58181
	tokTidb       = 58191

	// Sequence tokens
	tokCalibrate     = 57634
	tokStartTime     = 58076
	tokEndTime       = 58016
	tokWorkload      = 57989
	tokSequence      = 57896
	tokIncrement     = 57748
	tokMinValue      = 57788
	tokMaxValue      = 57489
	tokCycle         = 57678
	tokCache         = 57633
	tokNoCache       = 57800
	tokNoCycle       = 57801
	tokPreserve      = 57841
	tokTableChecksum = 57946
	tokOutfile       = 57513
	tokNoMinValue    = 57804
	tokNoMaxValue    = 57803

	// Placement tokens
	tokPlacement           = 58054
	tokPrimaryRegion       = 58059
	tokFollowers           = 58024
	tokVoters              = 58119
	tokLearners            = 58043
	tokSchedule            = 58072
	tokConstraints         = 58005
	tokLeaderConstraints   = 58040
	tokFollowerConstraints = 58023
	tokVoterConstraints    = 58118
	tokLearnerConstraints  = 58042
	tokSurvivalPreferences = 58088

	// Resource Group tokens
	tokResourceGroup = 57869
	tokResource      = 57869
	tokRuPerSec      = 58070
	tokPriority      = 58060
	tokAffinity      = 57599
	tokIETFQuotes    = 57744
	tokBurstable     = 58002
	tokRate          = 58063
	tokLow           = 58045
	tokMedium        = 58047
	tokHigh          = 58029
	tokRunaway       = 58074
	tokQueryLim      = 58062
	tokUnlimited     = 58110
	tokBackground    = 57995
	tokBackoff       = 58190

	// Resource Group QUERY_LIMIT / BACKGROUND sub-option tokens
	tokCooldown         = 58006
	tokDryRun           = 58014
	tokExact            = 58017
	tokExecElapsed      = 58018
	tokKill             = 57469
	tokProcessedKeys    = 58061
	tokRU               = 58068
	tokSimilar          = 58073
	tokSwitchGroup      = 58089
	tokTaskTypes        = 58091
	tokModerated        = 58111
	tokOff              = 57810
	tokUtilizationLimit = 58113

	// Procedure tokens
	tokProcedure = 57519
	tokOut       = 57511
	tokWhile     = 57588
	tokRepeat    = 57529
	tokUntil     = 57572
	tokCursor    = 57397
	tokHandler   = 57735
	tokInout     = 57452
	tokFetch     = 57426
	tokOpen      = 57818
	tokClose     = 57647
	tokDeclare   = 57684
	tokLeave     = 57474
	tokExit      = 57423

	// SQL Standard / Other Keywords
	tokSqlWarning   = 57548
	tokSqlException = 57546
	tokSqlState     = 57547
	tokIterate      = 57465
	tokElseIf       = 57418
	tokContinue     = 57387
	tokForce        = 57432
	tokRow          = 57536
	tokMode         = 57790
	tokExpansion    = 57716
	tokLanguage     = 57762
	tokAssign       = 58201
	tokNulls        = 57808
	tokRespect      = 57870
	tokLast         = 57763
	tokNext         = 57797
	tokValue        = 57977

	// Missing keywords required by IsReserved
	tokArray          = 57368
	tokCumeDist       = 57391
	tokDenseRank      = 57408
	tokFirstValue     = 57427
	tokILike          = 57447
	tokKeys           = 57468
	tokLag            = 57470
	tokLastValue      = 57471
	tokLead           = 57472
	tokLocalTime      = 57481
	tokLocalTs        = 57482
	tokNthValue       = 57500
	tokNtile          = 57501
	tokPercentRank    = 57516
	tokRank           = 57521
	tokRequire        = 57531
	tokRLike          = 57535
	tokRowNumber      = 57538
	tokSQL            = 57545
	tokSSL            = 57552
	tokTableSample    = 57557
	tokTiDBCurrentTSO = 57560
	tokTrigger        = 57566
	tokUsage          = 57574
	tokUTCDate        = 57577
	tokUTCTime        = 57578
	tokUTCTimestamp   = 57579
	tokVarBinary      = 57581
	tokVarChar        = 57582
	tokVarCharacter   = 57583
	tokFunction       = 57731
	tokSubPartition   = 57937
	tokSubPartitions  = 57938
	tokSystemTime     = 57943
	tokHistory        = 57739

	// Aliases for casing mismatches
	tokInOut        = tokInout
	tokSQLWarning   = tokSqlWarning
	tokSQLException = tokSqlException
	tokSQLState     = tokSqlState

	// Missing keywords required by CREATE USER and JSON_SUM_CRC32
	tokAccount               = 57595
	tokAttribute             = 57608
	tokAttributes            = 57609
	tokCipher                = 57643
	tokExpire                = 57717
	tokFailedLoginAttempts   = 57720
	tokIssuer                = 57758
	tokJsonSumCrc32          = 58038
	tokMaxConnectionsPerHour = 57775
	tokMaxQueriesPerHour     = 57778
	tokMaxUpdatesPerHour     = 57780
	tokMaxUserConnections    = 57781
	tokOptional              = 57819
	tokPasswordLockTime      = 57830
	tokReuse                 = 57875
	tokSAN                   = 57885
	tokSubject               = 57936
	tokTokenIssuer           = 57955
	tokX509                  = 57990

	// Privilege-related keywords
	tokProcess     = 57844
	tokSuper       = 57939
	tokEvent       = 57710
	tokFile        = 57723
	tokReplication = 57867
	tokShowDB      = 57542 // reuses tokShow
	tokRoutine     = 57880

	// MEMBER OF keyword
	tokMember   = 57783
	tokMemberOf = 57350

	// Field-type modifiers
	tokUnicode = 57972 // unicodeSym: sets charset to ucs2
	tokAscii   = 57607 // ascii: sets charset to latin1
	tokByte    = 57632 // byteType: BYTE = BINARY in some contexts
)

const (
	// Index type keywords
	tokRebuild  = 57855
	tokRtree    = 57883
	tokHnsw     = 58050
	tokHypo     = 57742
	tokInverted = 58033

	// Columnar replica
	tokAddColumnarReplicaOnDemand = 57597

	// Table option keywords
	tokAutoRandomBase = 57614
	tokIndexes        = 57750

	// ALTER TABLE ALGORITHM options
	tokCopy    = 58007
	tokInplace = 58030
	tokInstant = 58031

	// ANALYZE option keywords
	tokBuckets    = 58124
	tokCMSketch   = 58155
	tokDepth      = 58160
	tokSampleRate = 58177
	tokSamples    = 58178
	tokTopN       = 58193
	tokWidth      = 58194
)
