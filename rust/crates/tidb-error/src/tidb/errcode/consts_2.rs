// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");

//! Error code constants, part 2 of 2 (see `errcode/mod.rs`).

#![allow(non_upper_case_globals)]

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
/// `ErrForeignKeyCascadeDepthExceeded` (3008).
pub const ErrForeignKeyCascadeDepthExceeded: u16 = 3008;
/// `ErrInvalidFieldSize` (3013).
pub const ErrInvalidFieldSize: u16 = 3013;
/// `ErrPasswordExpireAnonymousUser` (3016).
pub const ErrPasswordExpireAnonymousUser: u16 = 3016;
/// `ErrInvalidArgumentForLogarithm` (3020).
pub const ErrInvalidArgumentForLogarithm: u16 = 3020;
/// `ErrMaxExecTimeExceeded` (3024).
pub const ErrMaxExecTimeExceeded: u16 = 3024;
/// `ErrAggregateOrderNonAggQuery` (3029).
pub const ErrAggregateOrderNonAggQuery: u16 = 3029;
/// `ErrUserLockWrongName` (3057).
pub const ErrUserLockWrongName: u16 = 3057;
/// `ErrUserLockDeadlock` (3058).
pub const ErrUserLockDeadlock: u16 = 3058;
/// `ErrIncorrectType` (3064).
pub const ErrIncorrectType: u16 = 3064;
/// `ErrFieldInOrderNotSelect` (3065).
pub const ErrFieldInOrderNotSelect: u16 = 3065;
/// `ErrAggregateInOrderNotSelect` (3066).
pub const ErrAggregateInOrderNotSelect: u16 = 3066;
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
/// `ErrAccountHasBeenLocked` (3118).
pub const ErrAccountHasBeenLocked: u16 = 3118;
/// `ErrWarnConflictingHint` (3126).
pub const ErrWarnConflictingHint: u16 = 3126;
/// `ErrUnresolvedHintName` (3128).
pub const ErrUnresolvedHintName: u16 = 3128;
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
/// `ErrInvalidJSONPathMultipleSelection` (3149).
pub const ErrInvalidJSONPathMultipleSelection: u16 = 3149;
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
/// `ErrSecureTransportRequired` (3159).
pub const ErrSecureTransportRequired: u16 = 3159;
/// `ErrBadUser` (3162).
pub const ErrBadUser: u16 = 3162;
/// `ErrUserAlreadyExists` (3163).
pub const ErrUserAlreadyExists: u16 = 3163;
/// `ErrInvalidJSONPathArrayCell` (3165).
pub const ErrInvalidJSONPathArrayCell: u16 = 3165;
/// `ErrInvalidEncryptionOption` (3184).
pub const ErrInvalidEncryptionOption: u16 = 3184;
/// `ErrTooLongValueForType` (3505).
pub const ErrTooLongValueForType: u16 = 3505;
/// `ErrPKIndexCantBeInvisible` (3522).
pub const ErrPKIndexCantBeInvisible: u16 = 3522;
/// `ErrGrantRole` (3523).
pub const ErrGrantRole: u16 = 3523;
/// `ErrRoleNotGranted` (3530).
pub const ErrRoleNotGranted: u16 = 3530;
/// `ErrLockAcquireFailAndNoWaitSet` (3572).
pub const ErrLockAcquireFailAndNoWaitSet: u16 = 3572;
/// `ErrCTERecursiveRequiresUnion` (3573).
pub const ErrCTERecursiveRequiresUnion: u16 = 3573;
/// `ErrCTERecursiveRequiresNonRecursiveFirst` (3574).
pub const ErrCTERecursiveRequiresNonRecursiveFirst: u16 = 3574;
/// `ErrCTERecursiveForbidsAggregation` (3575).
pub const ErrCTERecursiveForbidsAggregation: u16 = 3575;
/// `ErrCTERecursiveForbiddenJoinOrder` (3576).
pub const ErrCTERecursiveForbiddenJoinOrder: u16 = 3576;
/// `ErrInvalidRequiresSingleReference` (3577).
pub const ErrInvalidRequiresSingleReference: u16 = 3577;
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
/// `ErrWindowExplainJSON` (3598).
pub const ErrWindowExplainJSON: u16 = 3598;
/// `ErrWindowFunctionIgnoresFrame` (3599).
pub const ErrWindowFunctionIgnoresFrame: u16 = 3599;
/// `ErrInvalidNumberOfArgs` (3601).
pub const ErrInvalidNumberOfArgs: u16 = 3601;
/// `ErrFieldInGroupingNotGroupBy` (3602).
pub const ErrFieldInGroupingNotGroupBy: u16 = 3602;
/// `ErrIllegalPrivilegeLevel` (3619).
pub const ErrIllegalPrivilegeLevel: u16 = 3619;
/// `ErrCTEMaxRecursionDepth` (3636).
pub const ErrCTEMaxRecursionDepth: u16 = 3636;
/// `ErrNotHintUpdatable` (3637).
pub const ErrNotHintUpdatable: u16 = 3637;
/// `ErrExistsInHistoryPassword` (3638).
pub const ErrExistsInHistoryPassword: u16 = 3638;
/// `ErrInvalidDefaultUTF8MB4Collation` (3721).
pub const ErrInvalidDefaultUTF8MB4Collation: u16 = 3721;
/// `ErrForeignKeyCannotDropParent` (3730).
pub const ErrForeignKeyCannotDropParent: u16 = 3730;
/// `ErrForeignKeyCannotUseVirtualColumn` (3733).
pub const ErrForeignKeyCannotUseVirtualColumn: u16 = 3733;
/// `ErrForeignKeyNoColumnInParent` (3734).
pub const ErrForeignKeyNoColumnInParent: u16 = 3734;
/// `ErrDataTruncatedFunctionalIndex` (3751).
pub const ErrDataTruncatedFunctionalIndex: u16 = 3751;
/// `ErrDataOutOfRangeFunctionalIndex` (3752).
pub const ErrDataOutOfRangeFunctionalIndex: u16 = 3752;
/// `ErrFunctionalIndexOnJSONOrGeometryFunction` (3753).
pub const ErrFunctionalIndexOnJSONOrGeometryFunction: u16 = 3753;
/// `ErrFunctionalIndexRefAutoIncrement` (3754).
pub const ErrFunctionalIndexRefAutoIncrement: u16 = 3754;
/// `ErrCannotDropColumnFunctionalIndex` (3755).
pub const ErrCannotDropColumnFunctionalIndex: u16 = 3755;
/// `ErrFunctionalIndexPrimaryKey` (3756).
pub const ErrFunctionalIndexPrimaryKey: u16 = 3756;
/// `ErrFunctionalIndexOnBlob` (3757).
pub const ErrFunctionalIndexOnBlob: u16 = 3757;
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
/// `ErrGeneratedColumnRowValueIsNotAllowed` (3764).
pub const ErrGeneratedColumnRowValueIsNotAllowed: u16 = 3764;
/// `ErrDefValGeneratedNamedFunctionIsNotAllowed` (3770).
pub const ErrDefValGeneratedNamedFunctionIsNotAllowed: u16 = 3770;
/// `ErrFKIncompatibleColumns` (3780).
pub const ErrFKIncompatibleColumns: u16 = 3780;
/// `ErrFunctionalIndexRowValueIsNotAllowed` (3800).
pub const ErrFunctionalIndexRowValueIsNotAllowed: u16 = 3800;
/// `ErrInvalidLateralJoin` (3809).
pub const ErrInvalidLateralJoin: u16 = 3809;
/// `ErrNonBooleanExprForCheckConstraint` (3812).
pub const ErrNonBooleanExprForCheckConstraint: u16 = 3812;
/// `ErrColumnCheckConstraintReferencesOtherColumn` (3813).
pub const ErrColumnCheckConstraintReferencesOtherColumn: u16 = 3813;
/// `ErrCheckConstraintNamedFunctionIsNotAllowed` (3814).
pub const ErrCheckConstraintNamedFunctionIsNotAllowed: u16 = 3814;
/// `ErrCheckConstraintFunctionIsNotAllowed` (3815).
pub const ErrCheckConstraintFunctionIsNotAllowed: u16 = 3815;
/// `ErrCheckConstraintVariables` (3816).
pub const ErrCheckConstraintVariables: u16 = 3816;
/// `ErrCheckConstraintRefersAutoIncrementColumn` (3818).
pub const ErrCheckConstraintRefersAutoIncrementColumn: u16 = 3818;
/// `ErrCheckConstraintViolated` (3819).
pub const ErrCheckConstraintViolated: u16 = 3819;
/// `ErrTableCheckConstraintReferUnknown` (3820).
pub const ErrTableCheckConstraintReferUnknown: u16 = 3820;
/// `ErrCheckConstraintDupName` (3822).
pub const ErrCheckConstraintDupName: u16 = 3822;
/// `ErrCheckConstraintClauseUsingFKReferActionColumn` (3823).
pub const ErrCheckConstraintClauseUsingFKReferActionColumn: u16 = 3823;
/// `ErrDependentByFunctionalIndex` (3837).
pub const ErrDependentByFunctionalIndex: u16 = 3837;
/// `ErrInvalidJSONType` (3853).
pub const ErrInvalidJSONType: u16 = 3853;
/// `ErrCannotConvertString` (3854).
pub const ErrCannotConvertString: u16 = 3854;
/// `ErrDependentByPartitionFunctional` (3855).
pub const ErrDependentByPartitionFunctional: u16 = 3855;
/// `ErrInvalidJSONValueForFuncIndex` (3903).
pub const ErrInvalidJSONValueForFuncIndex: u16 = 3903;
/// `ErrJSONValueOutOfRangeForFuncIndex` (3904).
pub const ErrJSONValueOutOfRangeForFuncIndex: u16 = 3904;
/// `ErrFunctionalIndexDataIsTooLong` (3907).
pub const ErrFunctionalIndexDataIsTooLong: u16 = 3907;
/// `ErrFunctionalIndexNotApplicable` (3909).
pub const ErrFunctionalIndexNotApplicable: u16 = 3909;
/// `ErrDynamicPrivilegeNotRegistered` (3929).
pub const ErrDynamicPrivilegeNotRegistered: u16 = 3929;
/// `ErrConstraintNotFound` (3940).
pub const ErrConstraintNotFound: u16 = 3940;
/// `ErUserAccessDeniedForUserAccountBlockedByPasswordLock` (3955).
pub const ErUserAccessDeniedForUserAccountBlockedByPasswordLock: u16 = 3955;
/// `ErrDependentByCheckConstraint` (3959).
pub const ErrDependentByCheckConstraint: u16 = 3959;
/// `ErrEngineAttributeNotSupported` (3981).
pub const ErrEngineAttributeNotSupported: u16 = 3981;
/// `ErrJSONInBooleanContext` (3986).
pub const ErrJSONInBooleanContext: u16 = 3986;
/// `ErrTableWithoutPrimaryKey` (3750).
pub const ErrTableWithoutPrimaryKey: u16 = 3750;
/// `ErrSecondPasswordCannotBeEmpty` (3878).
pub const ErrSecondPasswordCannotBeEmpty: u16 = 3878;
/// `ErrPasswordCannotBeRetainedOnPluginChange` (3894).
pub const ErrPasswordCannotBeRetainedOnPluginChange: u16 = 3894;
/// `ErrCurrentPasswordCannotBeRetained` (3895).
pub const ErrCurrentPasswordCannotBeRetained: u16 = 3895;
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
/// `ErrMemExceedThreshold` (8001).
pub const ErrMemExceedThreshold: u16 = 8001;
/// `ErrForUpdateCantRetry` (8002).
pub const ErrForUpdateCantRetry: u16 = 8002;
/// `ErrAdminCheckTable` (8003).
pub const ErrAdminCheckTable: u16 = 8003;
/// `ErrTxnTooLarge` (8004).
pub const ErrTxnTooLarge: u16 = 8004;
/// `ErrWriteConflictInTiDB` (8005).
pub const ErrWriteConflictInTiDB: u16 = 8005;
/// `ErrOptOnTemporaryTable` (8006).
pub const ErrOptOnTemporaryTable: u16 = 8006;
/// `ErrDropTableOnTemporaryTable` (8007).
pub const ErrDropTableOnTemporaryTable: u16 = 8007;
/// `ErrUnsupportedReloadPlugin` (8018).
pub const ErrUnsupportedReloadPlugin: u16 = 8018;
/// `ErrUnsupportedReloadPluginVar` (8019).
pub const ErrUnsupportedReloadPluginVar: u16 = 8019;
/// `ErrTableLocked` (8020).
pub const ErrTableLocked: u16 = 8020;
/// `ErrNotExist` (8021).
pub const ErrNotExist: u16 = 8021;
/// `ErrTxnRetryable` (8022).
pub const ErrTxnRetryable: u16 = 8022;
/// `ErrCannotSetNilValue` (8023).
pub const ErrCannotSetNilValue: u16 = 8023;
/// `ErrInvalidTxn` (8024).
pub const ErrInvalidTxn: u16 = 8024;
/// `ErrEntryTooLarge` (8025).
pub const ErrEntryTooLarge: u16 = 8025;
/// `ErrNotImplemented` (8026).
pub const ErrNotImplemented: u16 = 8026;
/// `ErrInfoSchemaExpired` (8027).
pub const ErrInfoSchemaExpired: u16 = 8027;
/// `ErrInfoSchemaChanged` (8028).
pub const ErrInfoSchemaChanged: u16 = 8028;
/// `ErrBadNumber` (8029).
pub const ErrBadNumber: u16 = 8029;
/// `ErrCastAsSignedOverflow` (8030).
pub const ErrCastAsSignedOverflow: u16 = 8030;
/// `ErrCastNegIntAsUnsigned` (8031).
pub const ErrCastNegIntAsUnsigned: u16 = 8031;
/// `ErrInvalidYearFormat` (8032).
pub const ErrInvalidYearFormat: u16 = 8032;
/// `ErrInvalidYear` (8033).
pub const ErrInvalidYear: u16 = 8033;
/// `ErrIncorrectDatetimeValue` (8034).
pub const ErrIncorrectDatetimeValue: u16 = 8034;
/// `ErrInvalidTimeFormat` (8036).
pub const ErrInvalidTimeFormat: u16 = 8036;
/// `ErrInvalidWeekModeFormat` (8037).
pub const ErrInvalidWeekModeFormat: u16 = 8037;
/// `ErrFieldGetDefaultFailed` (8038).
pub const ErrFieldGetDefaultFailed: u16 = 8038;
/// `ErrIndexOutBound` (8039).
pub const ErrIndexOutBound: u16 = 8039;
/// `ErrUnsupportedOp` (8040).
pub const ErrUnsupportedOp: u16 = 8040;
/// `ErrRowNotFound` (8041).
pub const ErrRowNotFound: u16 = 8041;
/// `ErrTableStateCantNone` (8042).
pub const ErrTableStateCantNone: u16 = 8042;
/// `ErrColumnStateNonPublic` (8043).
pub const ErrColumnStateNonPublic: u16 = 8043;
/// `ErrIndexStateCantNone` (8044).
pub const ErrIndexStateCantNone: u16 = 8044;
/// `ErrInvalidRecordKey` (8045).
pub const ErrInvalidRecordKey: u16 = 8045;
/// `ErrColumnStateCantNone` (8046).
pub const ErrColumnStateCantNone: u16 = 8046;
/// `ErrUnsupportedValueForVar` (8047).
pub const ErrUnsupportedValueForVar: u16 = 8047;
/// `ErrUnsupportedIsolationLevel` (8048).
pub const ErrUnsupportedIsolationLevel: u16 = 8048;
/// `ErrLoadPrivilege` (8049).
pub const ErrLoadPrivilege: u16 = 8049;
/// `ErrInvalidPrivilegeType` (8050).
pub const ErrInvalidPrivilegeType: u16 = 8050;
/// `ErrUnknownFieldType` (8051).
pub const ErrUnknownFieldType: u16 = 8051;
/// `ErrInvalidSequence` (8052).
pub const ErrInvalidSequence: u16 = 8052;
/// `ErrCantGetValidID` (8053).
pub const ErrCantGetValidID: u16 = 8053;
/// `ErrCantSetToNull` (8054).
pub const ErrCantSetToNull: u16 = 8054;
/// `ErrSnapshotTooOld` (8055).
pub const ErrSnapshotTooOld: u16 = 8055;
/// `ErrInvalidTableID` (8056).
pub const ErrInvalidTableID: u16 = 8056;
/// `ErrInvalidType` (8057).
pub const ErrInvalidType: u16 = 8057;
/// `ErrUnknownAllocatorType` (8058).
pub const ErrUnknownAllocatorType: u16 = 8058;
/// `ErrAutoRandReadFailed` (8059).
pub const ErrAutoRandReadFailed: u16 = 8059;
/// `ErrInvalidIncrementAndOffset` (8060).
pub const ErrInvalidIncrementAndOffset: u16 = 8060;
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
/// `ErrUnsupportedSecondArgumentType` (8067).
pub const ErrUnsupportedSecondArgumentType: u16 = 8067;
/// `ErrColumnNotMatched` (8068).
pub const ErrColumnNotMatched: u16 = 8068;
/// `ErrInvalidPluginID` (8101).
pub const ErrInvalidPluginID: u16 = 8101;
/// `ErrInvalidPluginManifest` (8102).
pub const ErrInvalidPluginManifest: u16 = 8102;
/// `ErrInvalidPluginName` (8103).
pub const ErrInvalidPluginName: u16 = 8103;
/// `ErrInvalidPluginVersion` (8104).
pub const ErrInvalidPluginVersion: u16 = 8104;
/// `ErrDuplicatePlugin` (8105).
pub const ErrDuplicatePlugin: u16 = 8105;
/// `ErrInvalidPluginSysVarName` (8106).
pub const ErrInvalidPluginSysVarName: u16 = 8106;
/// `ErrRequireVersionCheckFail` (8107).
pub const ErrRequireVersionCheckFail: u16 = 8107;
/// `ErrUnsupportedType` (8108).
pub const ErrUnsupportedType: u16 = 8108;
/// `ErrAnalyzeMissIndex` (8109).
pub const ErrAnalyzeMissIndex: u16 = 8109;
/// `ErrCartesianProductUnsupported` (8110).
pub const ErrCartesianProductUnsupported: u16 = 8110;
/// `ErrPreparedStmtNotFound` (8111).
pub const ErrPreparedStmtNotFound: u16 = 8111;
/// `ErrWrongParamCount` (8112).
pub const ErrWrongParamCount: u16 = 8112;
/// `ErrSchemaChanged` (8113).
pub const ErrSchemaChanged: u16 = 8113;
/// `ErrUnknownPlan` (8114).
pub const ErrUnknownPlan: u16 = 8114;
/// `ErrPrepareMulti` (8115).
pub const ErrPrepareMulti: u16 = 8115;
/// `ErrPrepareDDL` (8116).
pub const ErrPrepareDDL: u16 = 8116;
/// `ErrResultIsEmpty` (8117).
pub const ErrResultIsEmpty: u16 = 8117;
/// `ErrBuildExecutor` (8118).
pub const ErrBuildExecutor: u16 = 8118;
/// `ErrBatchInsertFail` (8119).
pub const ErrBatchInsertFail: u16 = 8119;
/// `ErrGetStartTS` (8120).
pub const ErrGetStartTS: u16 = 8120;
/// `ErrPrivilegeCheckFail` (8121).
pub const ErrPrivilegeCheckFail: u16 = 8121;
/// `ErrInvalidWildCard` (8122).
pub const ErrInvalidWildCard: u16 = 8122;
/// `ErrMixOfGroupFuncAndFieldsIncompatible` (8123).
pub const ErrMixOfGroupFuncAndFieldsIncompatible: u16 = 8123;
/// `ErrBRIEBackupFailed` (8124).
pub const ErrBRIEBackupFailed: u16 = 8124;
/// `ErrBRIERestoreFailed` (8125).
pub const ErrBRIERestoreFailed: u16 = 8125;
/// `ErrBRIEImportFailed` (8126).
pub const ErrBRIEImportFailed: u16 = 8126;
/// `ErrBRIEExportFailed` (8127).
pub const ErrBRIEExportFailed: u16 = 8127;
/// `ErrInvalidTableSample` (8128).
pub const ErrInvalidTableSample: u16 = 8128;
/// `ErrJSONObjectKeyTooLong` (8129).
pub const ErrJSONObjectKeyTooLong: u16 = 8129;
/// `ErrMultiStatementDisabled` (8130).
pub const ErrMultiStatementDisabled: u16 = 8130;
/// `ErrPartitionStatsMissing` (8131).
pub const ErrPartitionStatsMissing: u16 = 8131;
/// `ErrNotSupportedWithSem` (8132).
pub const ErrNotSupportedWithSem: u16 = 8132;
/// `ErrDataInconsistentMismatchCount` (8133).
pub const ErrDataInconsistentMismatchCount: u16 = 8133;
/// `ErrDataInconsistentMismatchIndex` (8134).
pub const ErrDataInconsistentMismatchIndex: u16 = 8134;
/// `ErrAsOf` (8135).
pub const ErrAsOf: u16 = 8135;
/// `ErrVariableNoLongerSupported` (8136).
pub const ErrVariableNoLongerSupported: u16 = 8136;
/// `ErrAnalyzeMissColumn` (8137).
pub const ErrAnalyzeMissColumn: u16 = 8137;
/// `ErrInconsistentRowValue` (8138).
pub const ErrInconsistentRowValue: u16 = 8138;
/// `ErrInconsistentHandle` (8139).
pub const ErrInconsistentHandle: u16 = 8139;
/// `ErrInconsistentIndexedValue` (8140).
pub const ErrInconsistentIndexedValue: u16 = 8140;
/// `ErrAssertionFailed` (8141).
pub const ErrAssertionFailed: u16 = 8141;
/// `ErrInstanceScope` (8142).
pub const ErrInstanceScope: u16 = 8142;
/// `ErrNonTransactionalJobFailure` (8143).
pub const ErrNonTransactionalJobFailure: u16 = 8143;
/// `ErrSettingNoopVariable` (8144).
pub const ErrSettingNoopVariable: u16 = 8144;
/// `ErrGettingNoopVariable` (8145).
pub const ErrGettingNoopVariable: u16 = 8145;
/// `ErrCannotMigrateSession` (8146).
pub const ErrCannotMigrateSession: u16 = 8146;
/// `ErrLazyUniquenessCheckFailure` (8147).
pub const ErrLazyUniquenessCheckFailure: u16 = 8147;
/// `ErrUnsupportedColumnInTTLConfig` (8148).
pub const ErrUnsupportedColumnInTTLConfig: u16 = 8148;
/// `ErrTTLColumnCannotDrop` (8149).
pub const ErrTTLColumnCannotDrop: u16 = 8149;
/// `ErrSetTTLOptionForNonTTLTable` (8150).
pub const ErrSetTTLOptionForNonTTLTable: u16 = 8150;
/// `ErrTempTableNotAllowedWithTTL` (8151).
pub const ErrTempTableNotAllowedWithTTL: u16 = 8151;
/// `ErrUnsupportedTTLReferencedByFK` (8152).
pub const ErrUnsupportedTTLReferencedByFK: u16 = 8152;
/// `ErrUnsupportedPrimaryKeyTypeWithTTL` (8153).
pub const ErrUnsupportedPrimaryKeyTypeWithTTL: u16 = 8153;
/// `ErrLoadDataFromServerDisk` (8154).
pub const ErrLoadDataFromServerDisk: u16 = 8154;
/// `ErrLoadParquetFromLocal` (8155).
pub const ErrLoadParquetFromLocal: u16 = 8155;
/// `ErrLoadDataEmptyPath` (8156).
pub const ErrLoadDataEmptyPath: u16 = 8156;
/// `ErrLoadDataUnsupportedFormat` (8157).
pub const ErrLoadDataUnsupportedFormat: u16 = 8157;
/// `ErrLoadDataInvalidURI` (8158).
pub const ErrLoadDataInvalidURI: u16 = 8158;
/// `ErrLoadDataCantAccess` (8159).
pub const ErrLoadDataCantAccess: u16 = 8159;
/// `ErrLoadDataCantRead` (8160).
pub const ErrLoadDataCantRead: u16 = 8160;
/// `ErrLoadDataWrongFormatConfig` (8162).
pub const ErrLoadDataWrongFormatConfig: u16 = 8162;
/// `ErrUnknownOption` (8163).
pub const ErrUnknownOption: u16 = 8163;
/// `ErrInvalidOptionVal` (8164).
pub const ErrInvalidOptionVal: u16 = 8164;
/// `ErrDuplicateOption` (8165).
pub const ErrDuplicateOption: u16 = 8165;
/// `ErrLoadDataUnsupportedOption` (8166).
pub const ErrLoadDataUnsupportedOption: u16 = 8166;
/// `ErrLoadDataDuplicateKeyConflict` (8167).
pub const ErrLoadDataDuplicateKeyConflict: u16 = 8167;
/// `ErrLoadDataJobNotFound` (8170).
pub const ErrLoadDataJobNotFound: u16 = 8170;
/// `ErrLoadDataInvalidOperation` (8171).
pub const ErrLoadDataInvalidOperation: u16 = 8171;
/// `ErrLoadDataLocalUnsupportedOption` (8172).
pub const ErrLoadDataLocalUnsupportedOption: u16 = 8172;
/// `ErrLoadDataPreCheckFailed` (8173).
pub const ErrLoadDataPreCheckFailed: u16 = 8173;
/// `ErrBRJobNotFound` (8174).
pub const ErrBRJobNotFound: u16 = 8174;
/// `ErrMemoryExceedForQuery` (8175).
pub const ErrMemoryExceedForQuery: u16 = 8175;
/// `ErrMemoryExceedForInstance` (8176).
pub const ErrMemoryExceedForInstance: u16 = 8176;
/// `ErrDeleteNotFoundColumn` (8177).
pub const ErrDeleteNotFoundColumn: u16 = 8177;
/// `ErrKeyTooLarge` (8178).
pub const ErrKeyTooLarge: u16 = 8178;
/// `ErrTimeStampInDSTTransition` (8179).
pub const ErrTimeStampInDSTTransition: u16 = 8179;
/// `ErrQueryExecStopped` (8180).
pub const ErrQueryExecStopped: u16 = 8180;
/// `ErrUnsupportedDDLOperation` (8200).
pub const ErrUnsupportedDDLOperation: u16 = 8200;
/// `ErrNotOwner` (8201).
pub const ErrNotOwner: u16 = 8201;
/// `ErrCantDecodeRecord` (8202).
pub const ErrCantDecodeRecord: u16 = 8202;
/// `ErrInvalidDDLWorker` (8203).
pub const ErrInvalidDDLWorker: u16 = 8203;
/// `ErrInvalidDDLJob` (8204).
pub const ErrInvalidDDLJob: u16 = 8204;
/// `ErrInvalidDDLJobFlag` (8205).
pub const ErrInvalidDDLJobFlag: u16 = 8205;
/// `ErrWaitReorgTimeout` (8206).
pub const ErrWaitReorgTimeout: u16 = 8206;
/// `ErrInvalidStoreVersion` (8207).
pub const ErrInvalidStoreVersion: u16 = 8207;
/// `ErrUnknownTypeLength` (8208).
pub const ErrUnknownTypeLength: u16 = 8208;
/// `ErrUnknownFractionLength` (8209).
pub const ErrUnknownFractionLength: u16 = 8209;
/// `ErrInvalidDDLState` (8210).
pub const ErrInvalidDDLState: u16 = 8210;
/// `ErrReorgPanic` (8211).
pub const ErrReorgPanic: u16 = 8211;
/// `ErrInvalidSplitRegionRanges` (8212).
pub const ErrInvalidSplitRegionRanges: u16 = 8212;
/// `ErrInvalidDDLJobVersion` (8213).
pub const ErrInvalidDDLJobVersion: u16 = 8213;
/// `ErrCancelledDDLJob` (8214).
pub const ErrCancelledDDLJob: u16 = 8214;
/// `ErrRepairTable` (8215).
pub const ErrRepairTable: u16 = 8215;
/// `ErrInvalidAutoRandom` (8216).
pub const ErrInvalidAutoRandom: u16 = 8216;
/// `ErrInvalidHashKeyFlag` (8217).
pub const ErrInvalidHashKeyFlag: u16 = 8217;
/// `ErrInvalidListIndex` (8218).
pub const ErrInvalidListIndex: u16 = 8218;
/// `ErrInvalidListMetaData` (8219).
pub const ErrInvalidListMetaData: u16 = 8219;
/// `ErrWriteOnSnapshot` (8220).
pub const ErrWriteOnSnapshot: u16 = 8220;
/// `ErrInvalidKey` (8221).
pub const ErrInvalidKey: u16 = 8221;
/// `ErrInvalidIndexKey` (8222).
pub const ErrInvalidIndexKey: u16 = 8222;
/// `ErrDataInconsistent` (8223).
pub const ErrDataInconsistent: u16 = 8223;
/// `ErrDDLJobNotFound` (8224).
pub const ErrDDLJobNotFound: u16 = 8224;
/// `ErrCancelFinishedDDLJob` (8225).
pub const ErrCancelFinishedDDLJob: u16 = 8225;
/// `ErrCannotCancelDDLJob` (8226).
pub const ErrCannotCancelDDLJob: u16 = 8226;
/// `ErrSequenceUnsupportedTableOption` (8227).
pub const ErrSequenceUnsupportedTableOption: u16 = 8227;
/// `ErrColumnTypeUnsupportedNextValue` (8228).
pub const ErrColumnTypeUnsupportedNextValue: u16 = 8228;
/// `ErrLockExpire` (8229).
pub const ErrLockExpire: u16 = 8229;
/// `ErrAddColumnWithSequenceAsDefault` (8230).
pub const ErrAddColumnWithSequenceAsDefault: u16 = 8230;
/// `ErrUnsupportedConstraintCheck` (8231).
pub const ErrUnsupportedConstraintCheck: u16 = 8231;
/// `ErrTableOptionUnionUnsupported` (8232).
pub const ErrTableOptionUnionUnsupported: u16 = 8232;
/// `ErrTableOptionInsertMethodUnsupported` (8233).
pub const ErrTableOptionInsertMethodUnsupported: u16 = 8233;
/// `ErrDDLReorgElementNotExist` (8235).
pub const ErrDDLReorgElementNotExist: u16 = 8235;
/// `ErrPlacementPolicyCheck` (8236).
pub const ErrPlacementPolicyCheck: u16 = 8236;
/// `ErrInvalidAttributesSpec` (8237).
pub const ErrInvalidAttributesSpec: u16 = 8237;
/// `ErrPlacementPolicyExists` (8238).
pub const ErrPlacementPolicyExists: u16 = 8238;
/// `ErrPlacementPolicyNotExists` (8239).
pub const ErrPlacementPolicyNotExists: u16 = 8239;
/// `ErrPlacementPolicyWithDirectOption` (8240).
pub const ErrPlacementPolicyWithDirectOption: u16 = 8240;
/// `ErrPlacementPolicyInUse` (8241).
pub const ErrPlacementPolicyInUse: u16 = 8241;
/// `ErrOptOnCacheTable` (8242).
pub const ErrOptOnCacheTable: u16 = 8242;
/// `ErrHTTPServiceError` (8243).
pub const ErrHTTPServiceError: u16 = 8243;
/// `ErrPartitionColumnStatsMissing` (8244).
pub const ErrPartitionColumnStatsMissing: u16 = 8244;
/// `ErrColumnInChange` (8245).
pub const ErrColumnInChange: u16 = 8245;
/// `ErrDDLSetting` (8246).
pub const ErrDDLSetting: u16 = 8246;
/// `ErrIngestFailed` (8247).
pub const ErrIngestFailed: u16 = 8247;
/// `ErrIngestCheckEnvFailed` (8256).
pub const ErrIngestCheckEnvFailed: u16 = 8256;
/// `ErrProtectedTableMode` (8258).
pub const ErrProtectedTableMode: u16 = 8258;
/// `ErrInvalidTableModeSet` (8259).
pub const ErrInvalidTableModeSet: u16 = 8259;
/// `ErrCannotPauseDDLJob` (8260).
pub const ErrCannotPauseDDLJob: u16 = 8260;
/// `ErrCannotResumeDDLJob` (8261).
pub const ErrCannotResumeDDLJob: u16 = 8261;
/// `ErrPausedDDLJob` (8262).
pub const ErrPausedDDLJob: u16 = 8262;
/// `ErrBDRRestrictedDDL` (8263).
pub const ErrBDRRestrictedDDL: u16 = 8263;
/// `ErrGlobalIndexNotExplicitlySet` (8264).
pub const ErrGlobalIndexNotExplicitlySet: u16 = 8264;
/// `ErrWarnGlobalIndexNeedManuallyAnalyze` (8265).
pub const ErrWarnGlobalIndexNeedManuallyAnalyze: u16 = 8265;
/// `ErrInvalidAffinityOption` (8266).
pub const ErrInvalidAffinityOption: u16 = 8266;
/// `ErrForbiddenDDL` (8267).
pub const ErrForbiddenDDL: u16 = 8267;
/// `ErrMaskingPolicyExists` (8268).
pub const ErrMaskingPolicyExists: u16 = 8268;
/// `ErrMaskingPolicyNotExists` (8269).
pub const ErrMaskingPolicyNotExists: u16 = 8269;
/// `ErrMaskingPolicyExprInvalidColumn` (8275).
pub const ErrMaskingPolicyExprInvalidColumn: u16 = 8275;
/// `ErrDDLAutoPausedByKVDiskFull` (8276).
pub const ErrDDLAutoPausedByKVDiskFull: u16 = 8276;
/// `ErrResourceGroupExists` (8248).
pub const ErrResourceGroupExists: u16 = 8248;
/// `ErrResourceGroupNotExists` (8249).
pub const ErrResourceGroupNotExists: u16 = 8249;
/// `ErrResourceGroupSupportDisabled` (8250).
pub const ErrResourceGroupSupportDisabled: u16 = 8250;
/// `ErrResourceGroupConfigUnavailable` (8251).
pub const ErrResourceGroupConfigUnavailable: u16 = 8251;
/// `ErrResourceGroupThrottled` (8252).
pub const ErrResourceGroupThrottled: u16 = 8252;
/// `ErrResourceGroupQueryRunawayInterrupted` (8253).
pub const ErrResourceGroupQueryRunawayInterrupted: u16 = 8253;
/// `ErrResourceGroupQueryRunawayQuarantine` (8254).
pub const ErrResourceGroupQueryRunawayQuarantine: u16 = 8254;
/// `ErrResourceGroupInvalidBackgroundTaskName` (8255).
pub const ErrResourceGroupInvalidBackgroundTaskName: u16 = 8255;
/// `ErrResourceGroupInvalidForRole` (8257).
pub const ErrResourceGroupInvalidForRole: u16 = 8257;
/// `ErrEngineAttributeInvalidFormat` (8270).
pub const ErrEngineAttributeInvalidFormat: u16 = 8270;
/// `ErrStorageClassInvalidSpec` (8271).
pub const ErrStorageClassInvalidSpec: u16 = 8271;
/// `ErrModifyColumnReferencedByPartialCondition` (8272).
pub const ErrModifyColumnReferencedByPartialCondition: u16 = 8272;
/// `ErrCheckPartialIndexWithoutFastCheck` (8273).
pub const ErrCheckPartialIndexWithoutFastCheck: u16 = 8273;
/// `ErrMaxKeysReadExceeded` (8274).
pub const ErrMaxKeysReadExceeded: u16 = 8274;
/// `ErrPDServerTimeout` (9001).
pub const ErrPDServerTimeout: u16 = 9001;
/// `ErrTiKVServerTimeout` (9002).
pub const ErrTiKVServerTimeout: u16 = 9002;
/// `ErrTiKVServerBusy` (9003).
pub const ErrTiKVServerBusy: u16 = 9003;
/// `ErrResolveLockTimeout` (9004).
pub const ErrResolveLockTimeout: u16 = 9004;
/// `ErrRegionUnavailable` (9005).
pub const ErrRegionUnavailable: u16 = 9005;
/// `ErrTxnAbortedByGC` (9006).
pub const ErrTxnAbortedByGC: u16 = 9006;
/// `ErrWriteConflict` (9007).
pub const ErrWriteConflict: u16 = 9007;
/// `ErrTiKVStoreLimit` (9008).
pub const ErrTiKVStoreLimit: u16 = 9008;
/// `ErrPrometheusAddrIsNotSet` (9009).
pub const ErrPrometheusAddrIsNotSet: u16 = 9009;
/// `ErrTiKVStaleCommand` (9010).
pub const ErrTiKVStaleCommand: u16 = 9010;
/// `ErrTiKVMaxTimestampNotSynced` (9011).
pub const ErrTiKVMaxTimestampNotSynced: u16 = 9011;
/// `ErrTiFlashServerTimeout` (9012).
pub const ErrTiFlashServerTimeout: u16 = 9012;
/// `ErrTiFlashServerBusy` (9013).
pub const ErrTiFlashServerBusy: u16 = 9013;
/// `ErrTiFlashBackfillIndex` (9014).
pub const ErrTiFlashBackfillIndex: u16 = 9014;
/// `ErrUserPrefixMismatch` (20003).
pub const ErrUserPrefixMismatch: u16 = 20003;
