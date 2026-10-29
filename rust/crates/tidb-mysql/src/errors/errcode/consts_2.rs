// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");

//! Error code constants, part 2 of 2 (see `errcode/mod.rs`).

#![allow(non_upper_case_globals)]

/// `ErrInnodbOnlineLogTooBig` (1799).
pub const ErrInnodbOnlineLogTooBig: u16 = 1799;
/// `ErrUnknownAlterAlgorithm` (1800).
pub const ErrUnknownAlterAlgorithm: u16 = 1800;
/// `ErrUnknownAlterLock` (1801).
pub const ErrUnknownAlterLock: u16 = 1801;
/// `ErrMtsChangeMasterCantRunWithGaps` (1802).
pub const ErrMtsChangeMasterCantRunWithGaps: u16 = 1802;
/// `ErrMtsRecoveryFailure` (1803).
pub const ErrMtsRecoveryFailure: u16 = 1803;
/// `ErrMtsResetWorkers` (1804).
pub const ErrMtsResetWorkers: u16 = 1804;
/// `ErrColCountDoesntMatchCorruptedV2` (1805).
pub const ErrColCountDoesntMatchCorruptedV2: u16 = 1805;
/// `ErrSlaveSilentRetryTransaction` (1806).
pub const ErrSlaveSilentRetryTransaction: u16 = 1806;
/// `ErrDiscardFkChecksRunning` (1807).
pub const ErrDiscardFkChecksRunning: u16 = 1807;
/// `ErrTableSchemaMismatch` (1808).
pub const ErrTableSchemaMismatch: u16 = 1808;
/// `ErrTableInSystemTablespace` (1809).
pub const ErrTableInSystemTablespace: u16 = 1809;
/// `ErrIoRead` (1810).
pub const ErrIoRead: u16 = 1810;
/// `ErrIoWrite` (1811).
pub const ErrIoWrite: u16 = 1811;
/// `ErrTablespaceMissing` (1812).
pub const ErrTablespaceMissing: u16 = 1812;
/// `ErrTablespaceExists` (1813).
pub const ErrTablespaceExists: u16 = 1813;
/// `ErrTablespaceDiscarded` (1814).
pub const ErrTablespaceDiscarded: u16 = 1814;
/// `ErrInternal` (1815).
pub const ErrInternal: u16 = 1815;
/// `ErrInnodbImport` (1816).
pub const ErrInnodbImport: u16 = 1816;
/// `ErrInnodbIndexCorrupt` (1817).
pub const ErrInnodbIndexCorrupt: u16 = 1817;
/// `ErrInvalidYearColumnLength` (1818).
pub const ErrInvalidYearColumnLength: u16 = 1818;
/// `ErrNotValidPassword` (1819).
pub const ErrNotValidPassword: u16 = 1819;
/// `ErrMustChangePassword` (1820).
pub const ErrMustChangePassword: u16 = 1820;
/// `ErrFkNoIndexChild` (1821).
pub const ErrFkNoIndexChild: u16 = 1821;
/// `ErrForeignKeyNoIndexInParent` (1822).
pub const ErrForeignKeyNoIndexInParent: u16 = 1822;
/// `ErrFkFailAddSystem` (1823).
pub const ErrFkFailAddSystem: u16 = 1823;
/// `ErrForeignKeyCannotOpenParent` (1824).
pub const ErrForeignKeyCannotOpenParent: u16 = 1824;
/// `ErrFkIncorrectOption` (1825).
pub const ErrFkIncorrectOption: u16 = 1825;
/// `ErrFkDupName` (1826).
pub const ErrFkDupName: u16 = 1826;
/// `ErrPasswordFormat` (1827).
pub const ErrPasswordFormat: u16 = 1827;
/// `ErrFkColumnCannotDrop` (1828).
pub const ErrFkColumnCannotDrop: u16 = 1828;
/// `ErrFkColumnCannotDropChild` (1829).
pub const ErrFkColumnCannotDropChild: u16 = 1829;
/// `ErrForeignKeyColumnNotNull` (1830).
pub const ErrForeignKeyColumnNotNull: u16 = 1830;
/// `ErrDupIndex` (1831).
pub const ErrDupIndex: u16 = 1831;
/// `ErrForeignKeyColumnCannotChange` (1832).
pub const ErrForeignKeyColumnCannotChange: u16 = 1832;
/// `ErrForeignKeyColumnCannotChangeChild` (1833).
pub const ErrForeignKeyColumnCannotChangeChild: u16 = 1833;
/// `ErrFkCannotDeleteParent` (1834).
pub const ErrFkCannotDeleteParent: u16 = 1834;
/// `ErrMalformedPacket` (1835).
pub const ErrMalformedPacket: u16 = 1835;
/// `ErrReadOnlyMode` (1836).
pub const ErrReadOnlyMode: u16 = 1836;
/// `ErrGtidNextTypeUndefinedGroup` (1837).
pub const ErrGtidNextTypeUndefinedGroup: u16 = 1837;
/// `ErrVariableNotSettableInSp` (1838).
pub const ErrVariableNotSettableInSp: u16 = 1838;
/// `ErrCantSetGtidPurgedWhenGtidModeIsOff` (1839).
pub const ErrCantSetGtidPurgedWhenGtidModeIsOff: u16 = 1839;
/// `ErrCantSetGtidPurgedWhenGtidExecutedIsNotEmpty` (1840).
pub const ErrCantSetGtidPurgedWhenGtidExecutedIsNotEmpty: u16 = 1840;
/// `ErrCantSetGtidPurgedWhenOwnedGtidsIsNotEmpty` (1841).
pub const ErrCantSetGtidPurgedWhenOwnedGtidsIsNotEmpty: u16 = 1841;
/// `ErrGtidPurgedWasChanged` (1842).
pub const ErrGtidPurgedWasChanged: u16 = 1842;
/// `ErrGtidExecutedWasChanged` (1843).
pub const ErrGtidExecutedWasChanged: u16 = 1843;
/// `ErrBinlogStmtModeAndNoReplTables` (1844).
pub const ErrBinlogStmtModeAndNoReplTables: u16 = 1844;
/// `ErrAlterOperationNotSupported` (1845).
pub const ErrAlterOperationNotSupported: u16 = 1845;
/// `ErrAlterOperationNotSupportedReason` (1846).
pub const ErrAlterOperationNotSupportedReason: u16 = 1846;
/// `ErrAlterOperationNotSupportedReasonCopy` (1847).
pub const ErrAlterOperationNotSupportedReasonCopy: u16 = 1847;
/// `ErrAlterOperationNotSupportedReasonPartition` (1848).
pub const ErrAlterOperationNotSupportedReasonPartition: u16 = 1848;
/// `ErrAlterOperationNotSupportedReasonFkRename` (1849).
pub const ErrAlterOperationNotSupportedReasonFkRename: u16 = 1849;
/// `ErrAlterOperationNotSupportedReasonColumnType` (1850).
pub const ErrAlterOperationNotSupportedReasonColumnType: u16 = 1850;
/// `ErrAlterOperationNotSupportedReasonFkCheck` (1851).
pub const ErrAlterOperationNotSupportedReasonFkCheck: u16 = 1851;
/// `ErrAlterOperationNotSupportedReasonIgnore` (1852).
pub const ErrAlterOperationNotSupportedReasonIgnore: u16 = 1852;
/// `ErrAlterOperationNotSupportedReasonNopk` (1853).
pub const ErrAlterOperationNotSupportedReasonNopk: u16 = 1853;
/// `ErrAlterOperationNotSupportedReasonAutoinc` (1854).
pub const ErrAlterOperationNotSupportedReasonAutoinc: u16 = 1854;
/// `ErrAlterOperationNotSupportedReasonHiddenFts` (1855).
pub const ErrAlterOperationNotSupportedReasonHiddenFts: u16 = 1855;
/// `ErrAlterOperationNotSupportedReasonChangeFts` (1856).
pub const ErrAlterOperationNotSupportedReasonChangeFts: u16 = 1856;
/// `ErrAlterOperationNotSupportedReasonFts` (1857).
pub const ErrAlterOperationNotSupportedReasonFts: u16 = 1857;
/// `ErrSQLSlaveSkipCounterNotSettableInGtidMode` (1858).
pub const ErrSQLSlaveSkipCounterNotSettableInGtidMode: u16 = 1858;
/// `ErrDupUnknownInIndex` (1859).
pub const ErrDupUnknownInIndex: u16 = 1859;
/// `ErrIdentCausesTooLongPath` (1860).
pub const ErrIdentCausesTooLongPath: u16 = 1860;
/// `ErrAlterOperationNotSupportedReasonNotNull` (1861).
pub const ErrAlterOperationNotSupportedReasonNotNull: u16 = 1861;
/// `ErrMustChangePasswordLogin` (1862).
pub const ErrMustChangePasswordLogin: u16 = 1862;
/// `ErrRowInWrongPartition` (1863).
pub const ErrRowInWrongPartition: u16 = 1863;
/// `ErrErrorLast` (1863).
pub const ErrErrorLast: u16 = 1863;
/// `ErrInvalidFieldSize` (3013).
pub const ErrInvalidFieldSize: u16 = 3013;
/// `ErrPasswordExpireAnonymousUser` (3016).
pub const ErrPasswordExpireAnonymousUser: u16 = 3016;
/// `ErrMaxExecTimeExceeded` (3024).
pub const ErrMaxExecTimeExceeded: u16 = 3024;
/// `ErrIncorrectType` (3064).
pub const ErrIncorrectType: u16 = 3064;
/// `ErrInvalidJSONData` (3069).
pub const ErrInvalidJSONData: u16 = 3069;
/// `ErrGeneratedColumnFunctionIsNotAllowed` (3102).
pub const ErrGeneratedColumnFunctionIsNotAllowed: u16 = 3102;
/// `ErrUnsupportedAlterInplaceOnVirtualColumn` (3103).
pub const ErrUnsupportedAlterInplaceOnVirtualColumn: u16 = 3103;
/// `ErrWrongFKOptionForGeneratedColumn` (3104).
pub const ErrWrongFKOptionForGeneratedColumn: u16 = 3104;
/// `ErrBadGeneratedColumn` (3105).
pub const ErrBadGeneratedColumn: u16 = 3105;
/// `ErrUnsupportedOnGeneratedColumn` (3106).
pub const ErrUnsupportedOnGeneratedColumn: u16 = 3106;
/// `ErrGeneratedColumnNonPrior` (3107).
pub const ErrGeneratedColumnNonPrior: u16 = 3107;
/// `ErrDependentByGeneratedColumn` (3108).
pub const ErrDependentByGeneratedColumn: u16 = 3108;
/// `ErrGeneratedColumnRefAutoInc` (3109).
pub const ErrGeneratedColumnRefAutoInc: u16 = 3109;
/// `ErrInvalidJSONText` (3140).
pub const ErrInvalidJSONText: u16 = 3140;
/// `ErrInvalidJSONTextInParam` (3141).
pub const ErrInvalidJSONTextInParam: u16 = 3141;
/// `ErrInvalidJSONPath` (3143).
pub const ErrInvalidJSONPath: u16 = 3143;
/// `ErrInvalidJSONCharset` (3144).
pub const ErrInvalidJSONCharset: u16 = 3144;
/// `ErrInvalidTypeForJSON` (3146).
pub const ErrInvalidTypeForJSON: u16 = 3146;
/// `ErrInvalidJSONPathWildcard` (3149).
pub const ErrInvalidJSONPathWildcard: u16 = 3149;
/// `ErrInvalidJSONContainsPathType` (3150).
pub const ErrInvalidJSONContainsPathType: u16 = 3150;
/// `ErrJSONUsedAsKey` (3152).
pub const ErrJSONUsedAsKey: u16 = 3152;
/// `ErrJSONVacuousPath` (3153).
pub const ErrJSONVacuousPath: u16 = 3153;
/// `ErrJSONBadOneOrAllArg` (3154).
pub const ErrJSONBadOneOrAllArg: u16 = 3154;
/// `ErrJSONDocumentTooDeep` (3157).
pub const ErrJSONDocumentTooDeep: u16 = 3157;
/// `ErrJSONDocumentNULLKey` (3158).
pub const ErrJSONDocumentNULLKey: u16 = 3158;
/// `ErrBadUser` (3162).
pub const ErrBadUser: u16 = 3162;
/// `ErrUserAlreadyExists` (3163).
pub const ErrUserAlreadyExists: u16 = 3163;
/// `ErrInvalidJSONPathArrayCell` (3165).
pub const ErrInvalidJSONPathArrayCell: u16 = 3165;
/// `ErrInvalidEncryptionOption` (3184).
pub const ErrInvalidEncryptionOption: u16 = 3184;
/// `ErrRoleNotGranted` (3530).
pub const ErrRoleNotGranted: u16 = 3530;
/// `ErrLockAcquireFailAndNoWaitSet` (3572).
pub const ErrLockAcquireFailAndNoWaitSet: u16 = 3572;
/// `ErrWindowNoSuchWindow` (3579).
pub const ErrWindowNoSuchWindow: u16 = 3579;
/// `ErrWindowCircularityInWindowGraph` (3580).
pub const ErrWindowCircularityInWindowGraph: u16 = 3580;
/// `ErrWindowNoChildPartitioning` (3581).
pub const ErrWindowNoChildPartitioning: u16 = 3581;
/// `ErrWindowNoInherentFrame` (3582).
pub const ErrWindowNoInherentFrame: u16 = 3582;
/// `ErrWindowNoRedefineOrderBy` (3583).
pub const ErrWindowNoRedefineOrderBy: u16 = 3583;
/// `ErrWindowFrameStartIllegal` (3584).
pub const ErrWindowFrameStartIllegal: u16 = 3584;
/// `ErrWindowFrameEndIllegal` (3585).
pub const ErrWindowFrameEndIllegal: u16 = 3585;
/// `ErrWindowFrameIllegal` (3586).
pub const ErrWindowFrameIllegal: u16 = 3586;
/// `ErrWindowRangeFrameOrderType` (3587).
pub const ErrWindowRangeFrameOrderType: u16 = 3587;
/// `ErrWindowRangeFrameTemporalType` (3588).
pub const ErrWindowRangeFrameTemporalType: u16 = 3588;
/// `ErrWindowRangeFrameNumericType` (3589).
pub const ErrWindowRangeFrameNumericType: u16 = 3589;
/// `ErrWindowRangeBoundNotConstant` (3590).
pub const ErrWindowRangeBoundNotConstant: u16 = 3590;
/// `ErrWindowDuplicateName` (3591).
pub const ErrWindowDuplicateName: u16 = 3591;
/// `ErrWindowIllegalOrderBy` (3592).
pub const ErrWindowIllegalOrderBy: u16 = 3592;
/// `ErrWindowInvalidWindowFuncUse` (3593).
pub const ErrWindowInvalidWindowFuncUse: u16 = 3593;
/// `ErrWindowInvalidWindowFuncAliasUse` (3594).
pub const ErrWindowInvalidWindowFuncAliasUse: u16 = 3594;
/// `ErrWindowNestedWindowFuncUseInWindowSpec` (3595).
pub const ErrWindowNestedWindowFuncUseInWindowSpec: u16 = 3595;
/// `ErrWindowRowsIntervalUse` (3596).
pub const ErrWindowRowsIntervalUse: u16 = 3596;
/// `ErrWindowNoGroupOrderUnused` (3597).
pub const ErrWindowNoGroupOrderUnused: u16 = 3597;
/// `ErrWindowExplainJson` (3598).
pub const ErrWindowExplainJson: u16 = 3598;
/// `ErrWindowFunctionIgnoresFrame` (3599).
pub const ErrWindowFunctionIgnoresFrame: u16 = 3599;
/// `ErrDataTruncatedFunctionalIndex` (3751).
pub const ErrDataTruncatedFunctionalIndex: u16 = 3751;
/// `ErrDataOutOfRangeFunctionalIndex` (3752).
pub const ErrDataOutOfRangeFunctionalIndex: u16 = 3752;
/// `ErrFunctionalIndexOnJsonOrGeometryFunction` (3753).
pub const ErrFunctionalIndexOnJsonOrGeometryFunction: u16 = 3753;
/// `ErrFunctionalIndexRefAutoIncrement` (3754).
pub const ErrFunctionalIndexRefAutoIncrement: u16 = 3754;
/// `ErrCannotDropColumnFunctionalIndex` (3755).
pub const ErrCannotDropColumnFunctionalIndex: u16 = 3755;
/// `ErrFunctionalIndexPrimaryKey` (3756).
pub const ErrFunctionalIndexPrimaryKey: u16 = 3756;
/// `ErrFunctionalIndexOnLob` (3757).
pub const ErrFunctionalIndexOnLob: u16 = 3757;
/// `ErrFunctionalIndexFunctionIsNotAllowed` (3758).
pub const ErrFunctionalIndexFunctionIsNotAllowed: u16 = 3758;
/// `ErrFulltextFunctionalIndex` (3759).
pub const ErrFulltextFunctionalIndex: u16 = 3759;
/// `ErrSpatialFunctionalIndex` (3760).
pub const ErrSpatialFunctionalIndex: u16 = 3760;
/// `ErrWrongKeyColumnFunctionalIndex` (3761).
pub const ErrWrongKeyColumnFunctionalIndex: u16 = 3761;
/// `ErrFunctionalIndexOnField` (3762).
pub const ErrFunctionalIndexOnField: u16 = 3762;
/// `ErrFKIncompatibleColumns` (3780).
pub const ErrFKIncompatibleColumns: u16 = 3780;
/// `ErrFunctionalIndexRowValueIsNotAllowed` (3800).
pub const ErrFunctionalIndexRowValueIsNotAllowed: u16 = 3800;
/// `ErrDependentByFunctionalIndex` (3837).
pub const ErrDependentByFunctionalIndex: u16 = 3837;
/// `ErrInvalidJSONType` (3853).
pub const ErrInvalidJSONType: u16 = 3853;
/// `ErrInvalidJsonValueForFuncIndex` (3903).
pub const ErrInvalidJsonValueForFuncIndex: u16 = 3903;
/// `ErrJsonValueOutOfRangeForFuncIndex` (3904).
pub const ErrJsonValueOutOfRangeForFuncIndex: u16 = 3904;
/// `ErrFunctionalIndexDataIsTooLong` (3907).
pub const ErrFunctionalIndexDataIsTooLong: u16 = 3907;
/// `ErrFunctionalIndexNotApplicable` (3909).
pub const ErrFunctionalIndexNotApplicable: u16 = 3909;
/// `ErrOnlyOneDefaultPartionAllowed` (4030).
pub const ErrOnlyOneDefaultPartionAllowed: u16 = 4030;
/// `ErrWrongPartitionTypeExpectedSystemTime` (4113).
pub const ErrWrongPartitionTypeExpectedSystemTime: u16 = 4113;
/// `ErrSystemVersioningWrongPartitions` (4128).
pub const ErrSystemVersioningWrongPartitions: u16 = 4128;
/// `ErrSequenceRunOut` (4135).
pub const ErrSequenceRunOut: u16 = 4135;
/// `ErrSequenceInvalidData` (4136).
pub const ErrSequenceInvalidData: u16 = 4136;
/// `ErrSequenceAccessFail` (4137).
pub const ErrSequenceAccessFail: u16 = 4137;
/// `ErrNotSequence` (4138).
pub const ErrNotSequence: u16 = 4138;
/// `ErrUnknownSequence` (4139).
pub const ErrUnknownSequence: u16 = 4139;
/// `ErrWrongInsertIntoSequence` (4140).
pub const ErrWrongInsertIntoSequence: u16 = 4140;
/// `ErrSequenceInvalidTableStructure` (4141).
pub const ErrSequenceInvalidTableStructure: u16 = 4141;
/// `ErrWarnOptimizerHintUnsupportedHint` (8061).
pub const ErrWarnOptimizerHintUnsupportedHint: u16 = 8061;
/// `ErrWarnOptimizerHintInvalidToken` (8062).
pub const ErrWarnOptimizerHintInvalidToken: u16 = 8062;
/// `ErrWarnMemoryQuotaOverflow` (8063).
pub const ErrWarnMemoryQuotaOverflow: u16 = 8063;
/// `ErrWarnOptimizerHintParseError` (8064).
pub const ErrWarnOptimizerHintParseError: u16 = 8064;
/// `ErrWarnOptimizerHintInvalidInteger` (8065).
pub const ErrWarnOptimizerHintInvalidInteger: u16 = 8065;
/// `ErrWarnOptimizerHintWrongPos` (8066).
pub const ErrWarnOptimizerHintWrongPos: u16 = 8066;
