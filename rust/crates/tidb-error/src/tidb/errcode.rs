// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");

//! Direct constants from `pkg/errno/errcode.go`.

#![allow(non_upper_case_globals)]

/// `ErrErrorFirst` (1000).
pub const ErrErrorFirst: u16 = 1000;
/// `ErrHashchk` (1000).
pub const ErrHashchk: u16 = 1000;
/// `ErrNisamchk` (1001).
pub const ErrNisamchk: u16 = 1001;
/// `ErrNo` (1002).
pub const ErrNo: u16 = 1002;
/// `ErrYes` (1003).
pub const ErrYes: u16 = 1003;
/// `ErrCantCreateFile` (1004).
pub const ErrCantCreateFile: u16 = 1004;
/// `ErrCantCreateTable` (1005).
pub const ErrCantCreateTable: u16 = 1005;
/// `ErrCantCreateDB` (1006).
pub const ErrCantCreateDB: u16 = 1006;
/// `ErrDBCreateExists` (1007).
pub const ErrDBCreateExists: u16 = 1007;
/// `ErrDBDropExists` (1008).
pub const ErrDBDropExists: u16 = 1008;
/// `ErrDBDropDelete` (1009).
pub const ErrDBDropDelete: u16 = 1009;
/// `ErrDBDropRmdir` (1010).
pub const ErrDBDropRmdir: u16 = 1010;
/// `ErrCantDeleteFile` (1011).
pub const ErrCantDeleteFile: u16 = 1011;
/// `ErrCantFindSystemRec` (1012).
pub const ErrCantFindSystemRec: u16 = 1012;
/// `ErrCantGetStat` (1013).
pub const ErrCantGetStat: u16 = 1013;
/// `ErrCantGetWd` (1014).
pub const ErrCantGetWd: u16 = 1014;
/// `ErrCantLock` (1015).
pub const ErrCantLock: u16 = 1015;
/// `ErrCantOpenFile` (1016).
pub const ErrCantOpenFile: u16 = 1016;
/// `ErrFileNotFound` (1017).
pub const ErrFileNotFound: u16 = 1017;
/// `ErrCantReadDir` (1018).
pub const ErrCantReadDir: u16 = 1018;
/// `ErrCantSetWd` (1019).
pub const ErrCantSetWd: u16 = 1019;
/// `ErrCheckread` (1020).
pub const ErrCheckread: u16 = 1020;
/// `ErrDiskFull` (1021).
pub const ErrDiskFull: u16 = 1021;
/// `ErrDupKey` (1022).
pub const ErrDupKey: u16 = 1022;
/// `ErrErrorOnClose` (1023).
pub const ErrErrorOnClose: u16 = 1023;
/// `ErrErrorOnRead` (1024).
pub const ErrErrorOnRead: u16 = 1024;
/// `ErrErrorOnRename` (1025).
pub const ErrErrorOnRename: u16 = 1025;
/// `ErrErrorOnWrite` (1026).
pub const ErrErrorOnWrite: u16 = 1026;
/// `ErrFileUsed` (1027).
pub const ErrFileUsed: u16 = 1027;
/// `ErrFilsortAbort` (1028).
pub const ErrFilsortAbort: u16 = 1028;
/// `ErrFormNotFound` (1029).
pub const ErrFormNotFound: u16 = 1029;
/// `ErrGetErrno` (1030).
pub const ErrGetErrno: u16 = 1030;
/// `ErrIllegalHa` (1031).
pub const ErrIllegalHa: u16 = 1031;
/// `ErrKeyNotFound` (1032).
pub const ErrKeyNotFound: u16 = 1032;
/// `ErrNotFormFile` (1033).
pub const ErrNotFormFile: u16 = 1033;
/// `ErrNotKeyFile` (1034).
pub const ErrNotKeyFile: u16 = 1034;
/// `ErrOldKeyFile` (1035).
pub const ErrOldKeyFile: u16 = 1035;
/// `ErrOpenAsReadonly` (1036).
pub const ErrOpenAsReadonly: u16 = 1036;
/// `ErrOutofMemory` (1037).
pub const ErrOutofMemory: u16 = 1037;
/// `ErrOutOfSortMemory` (1038).
pub const ErrOutOfSortMemory: u16 = 1038;
/// `ErrUnexpectedEOF` (1039).
pub const ErrUnexpectedEOF: u16 = 1039;
/// `ErrConCount` (1040).
pub const ErrConCount: u16 = 1040;
/// `ErrOutOfResources` (1041).
pub const ErrOutOfResources: u16 = 1041;
/// `ErrBadHost` (1042).
pub const ErrBadHost: u16 = 1042;
/// `ErrHandshake` (1043).
pub const ErrHandshake: u16 = 1043;
/// `ErrDBaccessDenied` (1044).
pub const ErrDBaccessDenied: u16 = 1044;
/// `ErrAccessDenied` (1045).
pub const ErrAccessDenied: u16 = 1045;
/// `ErrNoDB` (1046).
pub const ErrNoDB: u16 = 1046;
/// `ErrUnknownCom` (1047).
pub const ErrUnknownCom: u16 = 1047;
/// `ErrBadNull` (1048).
pub const ErrBadNull: u16 = 1048;
/// `ErrBadDB` (1049).
pub const ErrBadDB: u16 = 1049;
/// `ErrTableExists` (1050).
pub const ErrTableExists: u16 = 1050;
/// `ErrBadTable` (1051).
pub const ErrBadTable: u16 = 1051;
/// `ErrNonUniq` (1052).
pub const ErrNonUniq: u16 = 1052;
/// `ErrServerShutdown` (1053).
pub const ErrServerShutdown: u16 = 1053;
/// `ErrBadField` (1054).
pub const ErrBadField: u16 = 1054;
/// `ErrFieldNotInGroupBy` (1055).
pub const ErrFieldNotInGroupBy: u16 = 1055;
/// `ErrWrongGroupField` (1056).
pub const ErrWrongGroupField: u16 = 1056;
/// `ErrWrongSumSelect` (1057).
pub const ErrWrongSumSelect: u16 = 1057;
/// `ErrWrongValueCount` (1058).
pub const ErrWrongValueCount: u16 = 1058;
/// `ErrTooLongIdent` (1059).
pub const ErrTooLongIdent: u16 = 1059;
/// `ErrDupFieldName` (1060).
pub const ErrDupFieldName: u16 = 1060;
/// `ErrDupKeyName` (1061).
pub const ErrDupKeyName: u16 = 1061;
/// `ErrDupEntry` (1062).
pub const ErrDupEntry: u16 = 1062;
/// `ErrWrongFieldSpec` (1063).
pub const ErrWrongFieldSpec: u16 = 1063;
/// `ErrParse` (1064).
pub const ErrParse: u16 = 1064;
/// `ErrEmptyQuery` (1065).
pub const ErrEmptyQuery: u16 = 1065;
/// `ErrNonuniqTable` (1066).
pub const ErrNonuniqTable: u16 = 1066;
/// `ErrInvalidDefault` (1067).
pub const ErrInvalidDefault: u16 = 1067;
/// `ErrMultiplePriKey` (1068).
pub const ErrMultiplePriKey: u16 = 1068;
/// `ErrTooManyKeys` (1069).
pub const ErrTooManyKeys: u16 = 1069;
/// `ErrTooManyKeyParts` (1070).
pub const ErrTooManyKeyParts: u16 = 1070;
/// `ErrTooLongKey` (1071).
pub const ErrTooLongKey: u16 = 1071;
/// `ErrKeyColumnDoesNotExits` (1072).
pub const ErrKeyColumnDoesNotExits: u16 = 1072;
/// `ErrBlobUsedAsKey` (1073).
pub const ErrBlobUsedAsKey: u16 = 1073;
/// `ErrTooBigFieldlength` (1074).
pub const ErrTooBigFieldlength: u16 = 1074;
/// `ErrWrongAutoKey` (1075).
pub const ErrWrongAutoKey: u16 = 1075;
/// `ErrReady` (1076).
pub const ErrReady: u16 = 1076;
/// `ErrNormalShutdown` (1077).
pub const ErrNormalShutdown: u16 = 1077;
/// `ErrGotSignal` (1078).
pub const ErrGotSignal: u16 = 1078;
/// `ErrShutdownComplete` (1079).
pub const ErrShutdownComplete: u16 = 1079;
/// `ErrForcingClose` (1080).
pub const ErrForcingClose: u16 = 1080;
/// `ErrIpsock` (1081).
pub const ErrIpsock: u16 = 1081;
/// `ErrNoSuchIndex` (1082).
pub const ErrNoSuchIndex: u16 = 1082;
/// `ErrWrongFieldTerminators` (1083).
pub const ErrWrongFieldTerminators: u16 = 1083;
/// `ErrBlobsAndNoTerminated` (1084).
pub const ErrBlobsAndNoTerminated: u16 = 1084;
/// `ErrTextFileNotReadable` (1085).
pub const ErrTextFileNotReadable: u16 = 1085;
/// `ErrFileExists` (1086).
pub const ErrFileExists: u16 = 1086;
/// `ErrLoadInfo` (1087).
pub const ErrLoadInfo: u16 = 1087;
/// `ErrAlterInfo` (1088).
pub const ErrAlterInfo: u16 = 1088;
/// `ErrWrongSubKey` (1089).
pub const ErrWrongSubKey: u16 = 1089;
/// `ErrCantRemoveAllFields` (1090).
pub const ErrCantRemoveAllFields: u16 = 1090;
/// `ErrCantDropFieldOrKey` (1091).
pub const ErrCantDropFieldOrKey: u16 = 1091;
/// `ErrInsertInfo` (1092).
pub const ErrInsertInfo: u16 = 1092;
/// `ErrUpdateTableUsed` (1093).
pub const ErrUpdateTableUsed: u16 = 1093;
/// `ErrNoSuchThread` (1094).
pub const ErrNoSuchThread: u16 = 1094;
/// `ErrKillDenied` (1095).
pub const ErrKillDenied: u16 = 1095;
/// `ErrNoTablesUsed` (1096).
pub const ErrNoTablesUsed: u16 = 1096;
/// `ErrTooBigSet` (1097).
pub const ErrTooBigSet: u16 = 1097;
/// `ErrNoUniqueLogFile` (1098).
pub const ErrNoUniqueLogFile: u16 = 1098;
/// `ErrTableNotLockedForWrite` (1099).
pub const ErrTableNotLockedForWrite: u16 = 1099;
/// `ErrTableNotLocked` (1100).
pub const ErrTableNotLocked: u16 = 1100;
/// `ErrBlobCantHaveDefault` (1101).
pub const ErrBlobCantHaveDefault: u16 = 1101;
/// `ErrWrongDBName` (1102).
pub const ErrWrongDBName: u16 = 1102;
/// `ErrWrongTableName` (1103).
pub const ErrWrongTableName: u16 = 1103;
/// `ErrTooBigSelect` (1104).
pub const ErrTooBigSelect: u16 = 1104;
/// `ErrUnknown` (1105).
pub const ErrUnknown: u16 = 1105;
/// `ErrUnknownProcedure` (1106).
pub const ErrUnknownProcedure: u16 = 1106;
/// `ErrWrongParamcountToProcedure` (1107).
pub const ErrWrongParamcountToProcedure: u16 = 1107;
/// `ErrWrongParametersToProcedure` (1108).
pub const ErrWrongParametersToProcedure: u16 = 1108;
/// `ErrUnknownTable` (1109).
pub const ErrUnknownTable: u16 = 1109;
/// `ErrFieldSpecifiedTwice` (1110).
pub const ErrFieldSpecifiedTwice: u16 = 1110;
/// `ErrInvalidGroupFuncUse` (1111).
pub const ErrInvalidGroupFuncUse: u16 = 1111;
/// `ErrUnsupportedExtension` (1112).
pub const ErrUnsupportedExtension: u16 = 1112;
/// `ErrTableMustHaveColumns` (1113).
pub const ErrTableMustHaveColumns: u16 = 1113;
/// `ErrRecordFileFull` (1114).
pub const ErrRecordFileFull: u16 = 1114;
/// `ErrUnknownCharacterSet` (1115).
pub const ErrUnknownCharacterSet: u16 = 1115;
/// `ErrTooManyTables` (1116).
pub const ErrTooManyTables: u16 = 1116;
/// `ErrTooManyFields` (1117).
pub const ErrTooManyFields: u16 = 1117;
/// `ErrTooBigRowsize` (1118).
pub const ErrTooBigRowsize: u16 = 1118;
/// `ErrStackOverrun` (1119).
pub const ErrStackOverrun: u16 = 1119;
/// `ErrWrongOuterJoin` (1120).
pub const ErrWrongOuterJoin: u16 = 1120;
/// `ErrNullColumnInIndex` (1121).
pub const ErrNullColumnInIndex: u16 = 1121;
/// `ErrCantFindUdf` (1122).
pub const ErrCantFindUdf: u16 = 1122;
/// `ErrCantInitializeUdf` (1123).
pub const ErrCantInitializeUdf: u16 = 1123;
/// `ErrUdfNoPaths` (1124).
pub const ErrUdfNoPaths: u16 = 1124;
/// `ErrUdfExists` (1125).
pub const ErrUdfExists: u16 = 1125;
/// `ErrCantOpenLibrary` (1126).
pub const ErrCantOpenLibrary: u16 = 1126;
/// `ErrCantFindDlEntry` (1127).
pub const ErrCantFindDlEntry: u16 = 1127;
/// `ErrFunctionNotDefined` (1128).
pub const ErrFunctionNotDefined: u16 = 1128;
/// `ErrHostIsBlocked` (1129).
pub const ErrHostIsBlocked: u16 = 1129;
/// `ErrHostNotPrivileged` (1130).
pub const ErrHostNotPrivileged: u16 = 1130;
/// `ErrPasswordAnonymousUser` (1131).
pub const ErrPasswordAnonymousUser: u16 = 1131;
/// `ErrPasswordNotAllowed` (1132).
pub const ErrPasswordNotAllowed: u16 = 1132;
/// `ErrPasswordNoMatch` (1133).
pub const ErrPasswordNoMatch: u16 = 1133;
/// `ErrUpdateInfo` (1134).
pub const ErrUpdateInfo: u16 = 1134;
/// `ErrCantCreateThread` (1135).
pub const ErrCantCreateThread: u16 = 1135;
/// `ErrWrongValueCountOnRow` (1136).
pub const ErrWrongValueCountOnRow: u16 = 1136;
/// `ErrCantReopenTable` (1137).
pub const ErrCantReopenTable: u16 = 1137;
/// `ErrInvalidUseOfNull` (1138).
pub const ErrInvalidUseOfNull: u16 = 1138;
/// `ErrRegexp` (1139).
pub const ErrRegexp: u16 = 1139;
/// `ErrMixOfGroupFuncAndFields` (1140).
pub const ErrMixOfGroupFuncAndFields: u16 = 1140;
/// `ErrNonexistingGrant` (1141).
pub const ErrNonexistingGrant: u16 = 1141;
/// `ErrTableaccessDenied` (1142).
pub const ErrTableaccessDenied: u16 = 1142;
/// `ErrColumnaccessDenied` (1143).
pub const ErrColumnaccessDenied: u16 = 1143;
/// `ErrIllegalGrantForTable` (1144).
pub const ErrIllegalGrantForTable: u16 = 1144;
/// `ErrGrantWrongHostOrUser` (1145).
pub const ErrGrantWrongHostOrUser: u16 = 1145;
/// `ErrNoSuchTable` (1146).
pub const ErrNoSuchTable: u16 = 1146;
/// `ErrNonexistingTableGrant` (1147).
pub const ErrNonexistingTableGrant: u16 = 1147;
/// `ErrNotAllowedCommand` (1148).
pub const ErrNotAllowedCommand: u16 = 1148;
/// `ErrSyntax` (1149).
pub const ErrSyntax: u16 = 1149;
/// `ErrDelayedCantChangeLock` (1150).
pub const ErrDelayedCantChangeLock: u16 = 1150;
/// `ErrTooManyDelayedThreads` (1151).
pub const ErrTooManyDelayedThreads: u16 = 1151;
/// `ErrAbortingConnection` (1152).
pub const ErrAbortingConnection: u16 = 1152;
/// `ErrNetPacketTooLarge` (1153).
pub const ErrNetPacketTooLarge: u16 = 1153;
/// `ErrNetReadErrorFromPipe` (1154).
pub const ErrNetReadErrorFromPipe: u16 = 1154;
/// `ErrNetFcntl` (1155).
pub const ErrNetFcntl: u16 = 1155;
/// `ErrNetPacketsOutOfOrder` (1156).
pub const ErrNetPacketsOutOfOrder: u16 = 1156;
/// `ErrNetUncompress` (1157).
pub const ErrNetUncompress: u16 = 1157;
/// `ErrNetRead` (1158).
pub const ErrNetRead: u16 = 1158;
/// `ErrNetReadInterrupted` (1159).
pub const ErrNetReadInterrupted: u16 = 1159;
/// `ErrNetErrorOnWrite` (1160).
pub const ErrNetErrorOnWrite: u16 = 1160;
/// `ErrNetWriteInterrupted` (1161).
pub const ErrNetWriteInterrupted: u16 = 1161;
/// `ErrTooLongString` (1162).
pub const ErrTooLongString: u16 = 1162;
/// `ErrTableCantHandleBlob` (1163).
pub const ErrTableCantHandleBlob: u16 = 1163;
/// `ErrTableCantHandleAutoIncrement` (1164).
pub const ErrTableCantHandleAutoIncrement: u16 = 1164;
/// `ErrDelayedInsertTableLocked` (1165).
pub const ErrDelayedInsertTableLocked: u16 = 1165;
/// `ErrWrongColumnName` (1166).
pub const ErrWrongColumnName: u16 = 1166;
/// `ErrWrongKeyColumn` (1167).
pub const ErrWrongKeyColumn: u16 = 1167;
/// `ErrWrongMrgTable` (1168).
pub const ErrWrongMrgTable: u16 = 1168;
/// `ErrDupUnique` (1169).
pub const ErrDupUnique: u16 = 1169;
/// `ErrBlobKeyWithoutLength` (1170).
pub const ErrBlobKeyWithoutLength: u16 = 1170;
/// `ErrPrimaryCantHaveNull` (1171).
pub const ErrPrimaryCantHaveNull: u16 = 1171;
/// `ErrTooManyRows` (1172).
pub const ErrTooManyRows: u16 = 1172;
/// `ErrRequiresPrimaryKey` (1173).
pub const ErrRequiresPrimaryKey: u16 = 1173;
/// `ErrNoRaidCompiled` (1174).
pub const ErrNoRaidCompiled: u16 = 1174;
/// `ErrUpdateWithoutKeyInSafeMode` (1175).
pub const ErrUpdateWithoutKeyInSafeMode: u16 = 1175;
/// `ErrKeyDoesNotExist` (1176).
pub const ErrKeyDoesNotExist: u16 = 1176;
/// `ErrCheckNoSuchTable` (1177).
pub const ErrCheckNoSuchTable: u16 = 1177;
/// `ErrCheckNotImplemented` (1178).
pub const ErrCheckNotImplemented: u16 = 1178;
/// `ErrCantDoThisDuringAnTransaction` (1179).
pub const ErrCantDoThisDuringAnTransaction: u16 = 1179;
/// `ErrErrorDuringCommit` (1180).
pub const ErrErrorDuringCommit: u16 = 1180;
/// `ErrErrorDuringRollback` (1181).
pub const ErrErrorDuringRollback: u16 = 1181;
/// `ErrErrorDuringFlushLogs` (1182).
pub const ErrErrorDuringFlushLogs: u16 = 1182;
/// `ErrErrorDuringCheckpoint` (1183).
pub const ErrErrorDuringCheckpoint: u16 = 1183;
/// `ErrNewAbortingConnection` (1184).
pub const ErrNewAbortingConnection: u16 = 1184;
/// `ErrDumpNotImplemented` (1185).
pub const ErrDumpNotImplemented: u16 = 1185;
/// `ErrIndexRebuild` (1187).
pub const ErrIndexRebuild: u16 = 1187;
/// `ErrFtMatchingKeyNotFound` (1191).
pub const ErrFtMatchingKeyNotFound: u16 = 1191;
/// `ErrLockOrActiveTransaction` (1192).
pub const ErrLockOrActiveTransaction: u16 = 1192;
/// `ErrUnknownSystemVariable` (1193).
pub const ErrUnknownSystemVariable: u16 = 1193;
/// `ErrCrashedOnUsage` (1194).
pub const ErrCrashedOnUsage: u16 = 1194;
/// `ErrCrashedOnRepair` (1195).
pub const ErrCrashedOnRepair: u16 = 1195;
/// `ErrWarningNotCompleteRollback` (1196).
pub const ErrWarningNotCompleteRollback: u16 = 1196;
/// `ErrTransCacheFull` (1197).
pub const ErrTransCacheFull: u16 = 1197;
/// `ErrTooManyUserConnections` (1203).
pub const ErrTooManyUserConnections: u16 = 1203;
/// `ErrSetConstantsOnly` (1204).
pub const ErrSetConstantsOnly: u16 = 1204;
/// `ErrLockWaitTimeout` (1205).
pub const ErrLockWaitTimeout: u16 = 1205;
/// `ErrLockTableFull` (1206).
pub const ErrLockTableFull: u16 = 1206;
/// `ErrReadOnlyTransaction` (1207).
pub const ErrReadOnlyTransaction: u16 = 1207;
/// `ErrDropDBWithReadLock` (1208).
pub const ErrDropDBWithReadLock: u16 = 1208;
/// `ErrCreateDBWithReadLock` (1209).
pub const ErrCreateDBWithReadLock: u16 = 1209;
/// `ErrWrongArguments` (1210).
pub const ErrWrongArguments: u16 = 1210;
/// `ErrNoPermissionToCreateUser` (1211).
pub const ErrNoPermissionToCreateUser: u16 = 1211;
/// `ErrUnionTablesInDifferentDir` (1212).
pub const ErrUnionTablesInDifferentDir: u16 = 1212;
/// `ErrLockDeadlock` (1213).
pub const ErrLockDeadlock: u16 = 1213;
/// `ErrTableCantHandleFt` (1214).
pub const ErrTableCantHandleFt: u16 = 1214;
/// `ErrCannotAddForeign` (1215).
pub const ErrCannotAddForeign: u16 = 1215;
/// `ErrNoReferencedRow` (1216).
pub const ErrNoReferencedRow: u16 = 1216;
/// `ErrRowIsReferenced` (1217).
pub const ErrRowIsReferenced: u16 = 1217;
/// `ErrErrorWhenExecutingCommand` (1220).
pub const ErrErrorWhenExecutingCommand: u16 = 1220;
/// `ErrWrongUsage` (1221).
pub const ErrWrongUsage: u16 = 1221;
/// `ErrWrongNumberOfColumnsInSelect` (1222).
pub const ErrWrongNumberOfColumnsInSelect: u16 = 1222;
/// `ErrCantUpdateWithReadlock` (1223).
pub const ErrCantUpdateWithReadlock: u16 = 1223;
/// `ErrMixingNotAllowed` (1224).
pub const ErrMixingNotAllowed: u16 = 1224;
/// `ErrDupArgument` (1225).
pub const ErrDupArgument: u16 = 1225;
/// `ErrUserLimitReached` (1226).
pub const ErrUserLimitReached: u16 = 1226;
/// `ErrSpecificAccessDenied` (1227).
pub const ErrSpecificAccessDenied: u16 = 1227;
/// `ErrLocalVariable` (1228).
pub const ErrLocalVariable: u16 = 1228;
/// `ErrGlobalVariable` (1229).
pub const ErrGlobalVariable: u16 = 1229;
/// `ErrNoDefault` (1230).
pub const ErrNoDefault: u16 = 1230;
/// `ErrWrongValueForVar` (1231).
pub const ErrWrongValueForVar: u16 = 1231;
/// `ErrWrongTypeForVar` (1232).
pub const ErrWrongTypeForVar: u16 = 1232;
/// `ErrVarCantBeRead` (1233).
pub const ErrVarCantBeRead: u16 = 1233;
/// `ErrCantUseOptionHere` (1234).
pub const ErrCantUseOptionHere: u16 = 1234;
/// `ErrNotSupportedYet` (1235).
pub const ErrNotSupportedYet: u16 = 1235;
/// `ErrIncorrectGlobalLocalVar` (1238).
pub const ErrIncorrectGlobalLocalVar: u16 = 1238;
/// `ErrWrongFkDef` (1239).
pub const ErrWrongFkDef: u16 = 1239;
/// `ErrKeyRefDoNotMatchTableRef` (1240).
pub const ErrKeyRefDoNotMatchTableRef: u16 = 1240;
/// `ErrOperandColumns` (1241).
pub const ErrOperandColumns: u16 = 1241;
/// `ErrSubqueryNo1Row` (1242).
pub const ErrSubqueryNo1Row: u16 = 1242;
/// `ErrUnknownStmtHandler` (1243).
pub const ErrUnknownStmtHandler: u16 = 1243;
/// `ErrCorruptHelpDB` (1244).
pub const ErrCorruptHelpDB: u16 = 1244;
/// `ErrCyclicReference` (1245).
pub const ErrCyclicReference: u16 = 1245;
/// `ErrAutoConvert` (1246).
pub const ErrAutoConvert: u16 = 1246;
/// `ErrIllegalReference` (1247).
pub const ErrIllegalReference: u16 = 1247;
/// `ErrDerivedMustHaveAlias` (1248).
pub const ErrDerivedMustHaveAlias: u16 = 1248;
/// `ErrSelectReduced` (1249).
pub const ErrSelectReduced: u16 = 1249;
/// `ErrTablenameNotAllowedHere` (1250).
pub const ErrTablenameNotAllowedHere: u16 = 1250;
/// `ErrNotSupportedAuthMode` (1251).
pub const ErrNotSupportedAuthMode: u16 = 1251;
/// `ErrSpatialCantHaveNull` (1252).
pub const ErrSpatialCantHaveNull: u16 = 1252;
/// `ErrCollationCharsetMismatch` (1253).
pub const ErrCollationCharsetMismatch: u16 = 1253;
/// `ErrTooBigForUncompress` (1256).
pub const ErrTooBigForUncompress: u16 = 1256;
/// `ErrZlibZMem` (1257).
pub const ErrZlibZMem: u16 = 1257;
/// `ErrZlibZBuf` (1258).
pub const ErrZlibZBuf: u16 = 1258;
/// `ErrZlibZData` (1259).
pub const ErrZlibZData: u16 = 1259;
/// `ErrCutValueGroupConcat` (1260).
pub const ErrCutValueGroupConcat: u16 = 1260;
/// `ErrWarnTooFewRecords` (1261).
pub const ErrWarnTooFewRecords: u16 = 1261;
/// `ErrWarnTooManyRecords` (1262).
pub const ErrWarnTooManyRecords: u16 = 1262;
/// `ErrWarnNullToNotnull` (1263).
pub const ErrWarnNullToNotnull: u16 = 1263;
/// `ErrWarnDataOutOfRange` (1264).
pub const ErrWarnDataOutOfRange: u16 = 1264;
/// `WarnDataTruncated` (1265).
pub const WarnDataTruncated: u16 = 1265;
/// `ErrWarnUsingOtherHandler` (1266).
pub const ErrWarnUsingOtherHandler: u16 = 1266;
/// `ErrCantAggregate2collations` (1267).
pub const ErrCantAggregate2collations: u16 = 1267;
/// `ErrDropUser` (1268).
pub const ErrDropUser: u16 = 1268;
/// `ErrRevokeGrants` (1269).
pub const ErrRevokeGrants: u16 = 1269;
/// `ErrCantAggregate3collations` (1270).
pub const ErrCantAggregate3collations: u16 = 1270;
/// `ErrCantAggregateNcollations` (1271).
pub const ErrCantAggregateNcollations: u16 = 1271;
/// `ErrVariableIsNotStruct` (1272).
pub const ErrVariableIsNotStruct: u16 = 1272;
/// `ErrUnknownCollation` (1273).
pub const ErrUnknownCollation: u16 = 1273;
/// `ErrServerIsInSecureAuthMode` (1275).
pub const ErrServerIsInSecureAuthMode: u16 = 1275;
/// `ErrWarnFieldResolved` (1276).
pub const ErrWarnFieldResolved: u16 = 1276;
/// `ErrUntilCondIgnored` (1279).
pub const ErrUntilCondIgnored: u16 = 1279;
/// `ErrWrongNameForIndex` (1280).
pub const ErrWrongNameForIndex: u16 = 1280;
/// `ErrWrongNameForCatalog` (1281).
pub const ErrWrongNameForCatalog: u16 = 1281;
/// `ErrWarnQcResize` (1282).
pub const ErrWarnQcResize: u16 = 1282;
/// `ErrBadFtColumn` (1283).
pub const ErrBadFtColumn: u16 = 1283;
/// `ErrUnknownKeyCache` (1284).
pub const ErrUnknownKeyCache: u16 = 1284;
/// `ErrWarnHostnameWontWork` (1285).
pub const ErrWarnHostnameWontWork: u16 = 1285;
/// `ErrUnknownStorageEngine` (1286).
pub const ErrUnknownStorageEngine: u16 = 1286;
/// `ErrWarnDeprecatedSyntax` (1287).
pub const ErrWarnDeprecatedSyntax: u16 = 1287;
/// `ErrNonUpdatableTable` (1288).
pub const ErrNonUpdatableTable: u16 = 1288;
/// `ErrFeatureDisabled` (1289).
pub const ErrFeatureDisabled: u16 = 1289;
/// `ErrOptionPreventsStatement` (1290).
pub const ErrOptionPreventsStatement: u16 = 1290;
/// `ErrDuplicatedValueInType` (1291).
pub const ErrDuplicatedValueInType: u16 = 1291;
/// `ErrTruncatedWrongValue` (1292).
pub const ErrTruncatedWrongValue: u16 = 1292;
/// `ErrTooMuchAutoTimestampCols` (1293).
pub const ErrTooMuchAutoTimestampCols: u16 = 1293;
/// `ErrInvalidOnUpdate` (1294).
pub const ErrInvalidOnUpdate: u16 = 1294;
/// `ErrUnsupportedPs` (1295).
pub const ErrUnsupportedPs: u16 = 1295;
/// `ErrGetErrmsg` (1296).
pub const ErrGetErrmsg: u16 = 1296;
/// `ErrGetTemporaryErrmsg` (1297).
pub const ErrGetTemporaryErrmsg: u16 = 1297;
/// `ErrUnknownTimeZone` (1298).
pub const ErrUnknownTimeZone: u16 = 1298;
/// `ErrWarnInvalidTimestamp` (1299).
pub const ErrWarnInvalidTimestamp: u16 = 1299;
/// `ErrInvalidCharacterString` (1300).
pub const ErrInvalidCharacterString: u16 = 1300;
/// `ErrWarnAllowedPacketOverflowed` (1301).
pub const ErrWarnAllowedPacketOverflowed: u16 = 1301;
/// `ErrConflictingDeclarations` (1302).
pub const ErrConflictingDeclarations: u16 = 1302;
/// `ErrSpNoRecursiveCreate` (1303).
pub const ErrSpNoRecursiveCreate: u16 = 1303;
/// `ErrSpAlreadyExists` (1304).
pub const ErrSpAlreadyExists: u16 = 1304;
/// `ErrSpDoesNotExist` (1305).
pub const ErrSpDoesNotExist: u16 = 1305;
/// `ErrSpDropFailed` (1306).
pub const ErrSpDropFailed: u16 = 1306;
/// `ErrSpStoreFailed` (1307).
pub const ErrSpStoreFailed: u16 = 1307;
/// `ErrSpLilabelMismatch` (1308).
pub const ErrSpLilabelMismatch: u16 = 1308;
/// `ErrSpLabelRedefine` (1309).
pub const ErrSpLabelRedefine: u16 = 1309;
/// `ErrSpLabelMismatch` (1310).
pub const ErrSpLabelMismatch: u16 = 1310;
/// `ErrSpUninitVar` (1311).
pub const ErrSpUninitVar: u16 = 1311;
/// `ErrSpBadselect` (1312).
pub const ErrSpBadselect: u16 = 1312;
/// `ErrSpBadreturn` (1313).
pub const ErrSpBadreturn: u16 = 1313;
/// `ErrSpBadstatement` (1314).
pub const ErrSpBadstatement: u16 = 1314;
/// `ErrUpdateLogDeprecatedIgnored` (1315).
pub const ErrUpdateLogDeprecatedIgnored: u16 = 1315;
/// `ErrUpdateLogDeprecatedTranslated` (1316).
pub const ErrUpdateLogDeprecatedTranslated: u16 = 1316;
/// `ErrQueryInterrupted` (1317).
pub const ErrQueryInterrupted: u16 = 1317;
/// `ErrSpWrongNoOfArgs` (1318).
pub const ErrSpWrongNoOfArgs: u16 = 1318;
/// `ErrSpCondMismatch` (1319).
pub const ErrSpCondMismatch: u16 = 1319;
/// `ErrSpNoreturn` (1320).
pub const ErrSpNoreturn: u16 = 1320;
/// `ErrSpNoreturnend` (1321).
pub const ErrSpNoreturnend: u16 = 1321;
/// `ErrSpBadCursorQuery` (1322).
pub const ErrSpBadCursorQuery: u16 = 1322;
/// `ErrSpBadCursorSelect` (1323).
pub const ErrSpBadCursorSelect: u16 = 1323;
/// `ErrSpCursorMismatch` (1324).
pub const ErrSpCursorMismatch: u16 = 1324;
/// `ErrSpCursorAlreadyOpen` (1325).
pub const ErrSpCursorAlreadyOpen: u16 = 1325;
/// `ErrSpCursorNotOpen` (1326).
pub const ErrSpCursorNotOpen: u16 = 1326;
/// `ErrSpUndeclaredVar` (1327).
pub const ErrSpUndeclaredVar: u16 = 1327;
/// `ErrSpWrongNoOfFetchArgs` (1328).
pub const ErrSpWrongNoOfFetchArgs: u16 = 1328;
/// `ErrSpFetchNoData` (1329).
pub const ErrSpFetchNoData: u16 = 1329;
/// `ErrSpDupParam` (1330).
pub const ErrSpDupParam: u16 = 1330;
/// `ErrSpDupVar` (1331).
pub const ErrSpDupVar: u16 = 1331;
/// `ErrSpDupCond` (1332).
pub const ErrSpDupCond: u16 = 1332;
/// `ErrSpDupCurs` (1333).
pub const ErrSpDupCurs: u16 = 1333;
/// `ErrSpCantAlter` (1334).
pub const ErrSpCantAlter: u16 = 1334;
/// `ErrSpSubselectNyi` (1335).
pub const ErrSpSubselectNyi: u16 = 1335;
/// `ErrStmtNotAllowedInSfOrTrg` (1336).
pub const ErrStmtNotAllowedInSfOrTrg: u16 = 1336;
/// `ErrSpVarcondAfterCurshndlr` (1337).
pub const ErrSpVarcondAfterCurshndlr: u16 = 1337;
/// `ErrSpCursorAfterHandler` (1338).
pub const ErrSpCursorAfterHandler: u16 = 1338;
/// `ErrSpCaseNotFound` (1339).
pub const ErrSpCaseNotFound: u16 = 1339;
/// `ErrFparserTooBigFile` (1340).
pub const ErrFparserTooBigFile: u16 = 1340;
/// `ErrFparserBadHeader` (1341).
pub const ErrFparserBadHeader: u16 = 1341;
/// `ErrFparserEOFInComment` (1342).
pub const ErrFparserEOFInComment: u16 = 1342;
/// `ErrFparserErrorInParameter` (1343).
pub const ErrFparserErrorInParameter: u16 = 1343;
/// `ErrFparserEOFInUnknownParameter` (1344).
pub const ErrFparserEOFInUnknownParameter: u16 = 1344;
/// `ErrViewNoExplain` (1345).
pub const ErrViewNoExplain: u16 = 1345;
/// `ErrFrmUnknownType` (1346).
pub const ErrFrmUnknownType: u16 = 1346;
/// `ErrWrongObject` (1347).
pub const ErrWrongObject: u16 = 1347;
/// `ErrNonupdateableColumn` (1348).
pub const ErrNonupdateableColumn: u16 = 1348;
/// `ErrViewSelectDerived` (1349).
pub const ErrViewSelectDerived: u16 = 1349;
/// `ErrViewSelectClause` (1350).
pub const ErrViewSelectClause: u16 = 1350;
/// `ErrViewSelectVariable` (1351).
pub const ErrViewSelectVariable: u16 = 1351;
/// `ErrViewSelectTmptable` (1352).
pub const ErrViewSelectTmptable: u16 = 1352;
/// `ErrViewWrongList` (1353).
pub const ErrViewWrongList: u16 = 1353;
/// `ErrWarnViewMerge` (1354).
pub const ErrWarnViewMerge: u16 = 1354;
/// `ErrWarnViewWithoutKey` (1355).
pub const ErrWarnViewWithoutKey: u16 = 1355;
/// `ErrViewInvalid` (1356).
pub const ErrViewInvalid: u16 = 1356;
/// `ErrSpNoDropSp` (1357).
pub const ErrSpNoDropSp: u16 = 1357;
/// `ErrSpGotoInHndlr` (1358).
pub const ErrSpGotoInHndlr: u16 = 1358;
/// `ErrTrgAlreadyExists` (1359).
pub const ErrTrgAlreadyExists: u16 = 1359;
/// `ErrTrgDoesNotExist` (1360).
pub const ErrTrgDoesNotExist: u16 = 1360;
/// `ErrTrgOnViewOrTempTable` (1361).
pub const ErrTrgOnViewOrTempTable: u16 = 1361;
/// `ErrTrgCantChangeRow` (1362).
pub const ErrTrgCantChangeRow: u16 = 1362;
/// `ErrTrgNoSuchRowInTrg` (1363).
pub const ErrTrgNoSuchRowInTrg: u16 = 1363;
/// `ErrNoDefaultForField` (1364).
pub const ErrNoDefaultForField: u16 = 1364;
/// `ErrDivisionByZero` (1365).
pub const ErrDivisionByZero: u16 = 1365;
/// `ErrTruncatedWrongValueForField` (1366).
pub const ErrTruncatedWrongValueForField: u16 = 1366;
/// `ErrIllegalValueForType` (1367).
pub const ErrIllegalValueForType: u16 = 1367;
/// `ErrViewNonupdCheck` (1368).
pub const ErrViewNonupdCheck: u16 = 1368;
/// `ErrViewCheckFailed` (1369).
pub const ErrViewCheckFailed: u16 = 1369;
/// `ErrProcaccessDenied` (1370).
pub const ErrProcaccessDenied: u16 = 1370;
/// `ErrRelayLogFail` (1371).
pub const ErrRelayLogFail: u16 = 1371;
/// `ErrPasswdLength` (1372).
pub const ErrPasswdLength: u16 = 1372;
/// `ErrUnknownTargetBinlog` (1373).
pub const ErrUnknownTargetBinlog: u16 = 1373;
/// `ErrIoErrLogIndexRead` (1374).
pub const ErrIoErrLogIndexRead: u16 = 1374;
/// `ErrBinlogPurgeProhibited` (1375).
pub const ErrBinlogPurgeProhibited: u16 = 1375;
/// `ErrFseekFail` (1376).
pub const ErrFseekFail: u16 = 1376;
/// `ErrBinlogPurgeFatalErr` (1377).
pub const ErrBinlogPurgeFatalErr: u16 = 1377;
/// `ErrLogInUse` (1378).
pub const ErrLogInUse: u16 = 1378;
/// `ErrLogPurgeUnknownErr` (1379).
pub const ErrLogPurgeUnknownErr: u16 = 1379;
/// `ErrRelayLogInit` (1380).
pub const ErrRelayLogInit: u16 = 1380;
/// `ErrNoBinaryLogging` (1381).
pub const ErrNoBinaryLogging: u16 = 1381;
/// `ErrReservedSyntax` (1382).
pub const ErrReservedSyntax: u16 = 1382;
/// `ErrWsasFailed` (1383).
pub const ErrWsasFailed: u16 = 1383;
/// `ErrDiffGroupsProc` (1384).
pub const ErrDiffGroupsProc: u16 = 1384;
/// `ErrNoGroupForProc` (1385).
pub const ErrNoGroupForProc: u16 = 1385;
/// `ErrOrderWithProc` (1386).
pub const ErrOrderWithProc: u16 = 1386;
/// `ErrLoggingProhibitChangingOf` (1387).
pub const ErrLoggingProhibitChangingOf: u16 = 1387;
/// `ErrNoFileMapping` (1388).
pub const ErrNoFileMapping: u16 = 1388;
/// `ErrWrongMagic` (1389).
pub const ErrWrongMagic: u16 = 1389;
/// `ErrPsManyParam` (1390).
pub const ErrPsManyParam: u16 = 1390;
/// `ErrKeyPart0` (1391).
pub const ErrKeyPart0: u16 = 1391;
/// `ErrViewChecksum` (1392).
pub const ErrViewChecksum: u16 = 1392;
/// `ErrViewMultiupdate` (1393).
pub const ErrViewMultiupdate: u16 = 1393;
/// `ErrViewNoInsertFieldList` (1394).
pub const ErrViewNoInsertFieldList: u16 = 1394;
/// `ErrViewDeleteMergeView` (1395).
pub const ErrViewDeleteMergeView: u16 = 1395;
/// `ErrCannotUser` (1396).
pub const ErrCannotUser: u16 = 1396;
/// `ErrXaerNota` (1397).
pub const ErrXaerNota: u16 = 1397;
/// `ErrXaerInval` (1398).
pub const ErrXaerInval: u16 = 1398;
/// `ErrXaerRmfail` (1399).
pub const ErrXaerRmfail: u16 = 1399;
/// `ErrXaerOutside` (1400).
pub const ErrXaerOutside: u16 = 1400;
/// `ErrXaerRmerr` (1401).
pub const ErrXaerRmerr: u16 = 1401;
/// `ErrXaRbrollback` (1402).
pub const ErrXaRbrollback: u16 = 1402;
/// `ErrNonexistingProcGrant` (1403).
pub const ErrNonexistingProcGrant: u16 = 1403;
/// `ErrProcAutoGrantFail` (1404).
pub const ErrProcAutoGrantFail: u16 = 1404;
/// `ErrProcAutoRevokeFail` (1405).
pub const ErrProcAutoRevokeFail: u16 = 1405;
/// `ErrDataTooLong` (1406).
pub const ErrDataTooLong: u16 = 1406;
/// `ErrSpBadSQLstate` (1407).
pub const ErrSpBadSQLstate: u16 = 1407;
/// `ErrStartup` (1408).
pub const ErrStartup: u16 = 1408;
/// `ErrLoadFromFixedSizeRowsToVar` (1409).
pub const ErrLoadFromFixedSizeRowsToVar: u16 = 1409;
/// `ErrCantCreateUserWithGrant` (1410).
pub const ErrCantCreateUserWithGrant: u16 = 1410;
/// `ErrWrongValueForType` (1411).
pub const ErrWrongValueForType: u16 = 1411;
/// `ErrTableDefChanged` (1412).
pub const ErrTableDefChanged: u16 = 1412;
/// `ErrSpDupHandler` (1413).
pub const ErrSpDupHandler: u16 = 1413;
/// `ErrSpNotVarArg` (1414).
pub const ErrSpNotVarArg: u16 = 1414;
/// `ErrSpNoRetset` (1415).
pub const ErrSpNoRetset: u16 = 1415;
/// `ErrCantCreateGeometryObject` (1416).
pub const ErrCantCreateGeometryObject: u16 = 1416;
/// `ErrFailedRoutineBreakBinlog` (1417).
pub const ErrFailedRoutineBreakBinlog: u16 = 1417;
/// `ErrBinlogUnsafeRoutine` (1418).
pub const ErrBinlogUnsafeRoutine: u16 = 1418;
/// `ErrBinlogCreateRoutineNeedSuper` (1419).
pub const ErrBinlogCreateRoutineNeedSuper: u16 = 1419;
/// `ErrExecStmtWithOpenCursor` (1420).
pub const ErrExecStmtWithOpenCursor: u16 = 1420;
/// `ErrStmtHasNoOpenCursor` (1421).
pub const ErrStmtHasNoOpenCursor: u16 = 1421;
/// `ErrCommitNotAllowedInSfOrTrg` (1422).
pub const ErrCommitNotAllowedInSfOrTrg: u16 = 1422;
/// `ErrNoDefaultForViewField` (1423).
pub const ErrNoDefaultForViewField: u16 = 1423;
/// `ErrSpNoRecursion` (1424).
pub const ErrSpNoRecursion: u16 = 1424;
/// `ErrTooBigScale` (1425).
pub const ErrTooBigScale: u16 = 1425;
/// `ErrTooBigPrecision` (1426).
pub const ErrTooBigPrecision: u16 = 1426;
/// `ErrMBiggerThanD` (1427).
pub const ErrMBiggerThanD: u16 = 1427;
/// `ErrWrongLockOfSystemTable` (1428).
pub const ErrWrongLockOfSystemTable: u16 = 1428;
/// `ErrConnectToForeignDataSource` (1429).
pub const ErrConnectToForeignDataSource: u16 = 1429;
/// `ErrQueryOnForeignDataSource` (1430).
pub const ErrQueryOnForeignDataSource: u16 = 1430;
/// `ErrForeignDataSourceDoesntExist` (1431).
pub const ErrForeignDataSourceDoesntExist: u16 = 1431;
/// `ErrForeignDataStringInvalidCantCreate` (1432).
pub const ErrForeignDataStringInvalidCantCreate: u16 = 1432;
/// `ErrForeignDataStringInvalid` (1433).
pub const ErrForeignDataStringInvalid: u16 = 1433;
/// `ErrCantCreateFederatedTable` (1434).
pub const ErrCantCreateFederatedTable: u16 = 1434;
/// `ErrTrgInWrongSchema` (1435).
pub const ErrTrgInWrongSchema: u16 = 1435;
/// `ErrStackOverrunNeedMore` (1436).
pub const ErrStackOverrunNeedMore: u16 = 1436;
/// `ErrTooLongBody` (1437).
pub const ErrTooLongBody: u16 = 1437;
/// `ErrWarnCantDropDefaultKeycache` (1438).
pub const ErrWarnCantDropDefaultKeycache: u16 = 1438;
/// `ErrTooBigDisplaywidth` (1439).
pub const ErrTooBigDisplaywidth: u16 = 1439;
/// `ErrXaerDupid` (1440).
pub const ErrXaerDupid: u16 = 1440;
/// `ErrDatetimeFunctionOverflow` (1441).
pub const ErrDatetimeFunctionOverflow: u16 = 1441;
/// `ErrCantUpdateUsedTableInSfOrTrg` (1442).
pub const ErrCantUpdateUsedTableInSfOrTrg: u16 = 1442;
/// `ErrViewPreventUpdate` (1443).
pub const ErrViewPreventUpdate: u16 = 1443;
/// `ErrPsNoRecursion` (1444).
pub const ErrPsNoRecursion: u16 = 1444;
/// `ErrSpCantSetAutocommit` (1445).
pub const ErrSpCantSetAutocommit: u16 = 1445;
/// `ErrMalformedDefiner` (1446).
pub const ErrMalformedDefiner: u16 = 1446;
/// `ErrViewFrmNoUser` (1447).
pub const ErrViewFrmNoUser: u16 = 1447;
/// `ErrViewOtherUser` (1448).
pub const ErrViewOtherUser: u16 = 1448;
/// `ErrNoSuchUser` (1449).
pub const ErrNoSuchUser: u16 = 1449;
/// `ErrForbidSchemaChange` (1450).
pub const ErrForbidSchemaChange: u16 = 1450;
/// `ErrRowIsReferenced2` (1451).
pub const ErrRowIsReferenced2: u16 = 1451;
/// `ErrNoReferencedRow2` (1452).
pub const ErrNoReferencedRow2: u16 = 1452;
/// `ErrSpBadVarShadow` (1453).
pub const ErrSpBadVarShadow: u16 = 1453;
/// `ErrTrgNoDefiner` (1454).
pub const ErrTrgNoDefiner: u16 = 1454;
/// `ErrOldFileFormat` (1455).
pub const ErrOldFileFormat: u16 = 1455;
/// `ErrSpRecursionLimit` (1456).
pub const ErrSpRecursionLimit: u16 = 1456;
/// `ErrSpProcTableCorrupt` (1457).
pub const ErrSpProcTableCorrupt: u16 = 1457;
/// `ErrSpWrongName` (1458).
pub const ErrSpWrongName: u16 = 1458;
/// `ErrTableNeedsUpgrade` (1459).
pub const ErrTableNeedsUpgrade: u16 = 1459;
/// `ErrSpNoAggregate` (1460).
pub const ErrSpNoAggregate: u16 = 1460;
/// `ErrMaxPreparedStmtCountReached` (1461).
pub const ErrMaxPreparedStmtCountReached: u16 = 1461;
/// `ErrViewRecursive` (1462).
pub const ErrViewRecursive: u16 = 1462;
/// `ErrNonGroupingFieldUsed` (1463).
pub const ErrNonGroupingFieldUsed: u16 = 1463;
/// `ErrTableCantHandleSpkeys` (1464).
pub const ErrTableCantHandleSpkeys: u16 = 1464;
/// `ErrNoTriggersOnSystemSchema` (1465).
pub const ErrNoTriggersOnSystemSchema: u16 = 1465;
/// `ErrRemovedSpaces` (1466).
pub const ErrRemovedSpaces: u16 = 1466;
/// `ErrAutoincReadFailed` (1467).
pub const ErrAutoincReadFailed: u16 = 1467;
/// `ErrUsername` (1468).
pub const ErrUsername: u16 = 1468;
/// `ErrHostname` (1469).
pub const ErrHostname: u16 = 1469;
/// `ErrWrongStringLength` (1470).
pub const ErrWrongStringLength: u16 = 1470;
/// `ErrNonInsertableTable` (1471).
pub const ErrNonInsertableTable: u16 = 1471;
/// `ErrAdminWrongMrgTable` (1472).
pub const ErrAdminWrongMrgTable: u16 = 1472;
/// `ErrTooHighLevelOfNestingForSelect` (1473).
pub const ErrTooHighLevelOfNestingForSelect: u16 = 1473;
/// `ErrNameBecomesEmpty` (1474).
pub const ErrNameBecomesEmpty: u16 = 1474;
/// `ErrAmbiguousFieldTerm` (1475).
pub const ErrAmbiguousFieldTerm: u16 = 1475;
/// `ErrForeignServerExists` (1476).
pub const ErrForeignServerExists: u16 = 1476;
/// `ErrForeignServerDoesntExist` (1477).
pub const ErrForeignServerDoesntExist: u16 = 1477;
/// `ErrIllegalHaCreateOption` (1478).
pub const ErrIllegalHaCreateOption: u16 = 1478;
/// `ErrPartitionRequiresValues` (1479).
pub const ErrPartitionRequiresValues: u16 = 1479;
/// `ErrPartitionWrongValues` (1480).
pub const ErrPartitionWrongValues: u16 = 1480;
/// `ErrPartitionMaxvalue` (1481).
pub const ErrPartitionMaxvalue: u16 = 1481;
/// `ErrPartitionSubpartition` (1482).
pub const ErrPartitionSubpartition: u16 = 1482;
/// `ErrPartitionSubpartMix` (1483).
pub const ErrPartitionSubpartMix: u16 = 1483;
/// `ErrPartitionWrongNoPart` (1484).
pub const ErrPartitionWrongNoPart: u16 = 1484;
/// `ErrPartitionWrongNoSubpart` (1485).
pub const ErrPartitionWrongNoSubpart: u16 = 1485;
/// `ErrWrongExprInPartitionFunc` (1486).
pub const ErrWrongExprInPartitionFunc: u16 = 1486;
/// `ErrNoConstExprInRangeOrList` (1487).
pub const ErrNoConstExprInRangeOrList: u16 = 1487;
/// `ErrFieldNotFoundPart` (1488).
pub const ErrFieldNotFoundPart: u16 = 1488;
/// `ErrListOfFieldsOnlyInHash` (1489).
pub const ErrListOfFieldsOnlyInHash: u16 = 1489;
/// `ErrInconsistentPartitionInfo` (1490).
pub const ErrInconsistentPartitionInfo: u16 = 1490;
/// `ErrPartitionFuncNotAllowed` (1491).
pub const ErrPartitionFuncNotAllowed: u16 = 1491;
/// `ErrPartitionsMustBeDefined` (1492).
pub const ErrPartitionsMustBeDefined: u16 = 1492;
/// `ErrRangeNotIncreasing` (1493).
pub const ErrRangeNotIncreasing: u16 = 1493;
/// `ErrInconsistentTypeOfFunctions` (1494).
pub const ErrInconsistentTypeOfFunctions: u16 = 1494;
/// `ErrMultipleDefConstInListPart` (1495).
pub const ErrMultipleDefConstInListPart: u16 = 1495;
/// `ErrPartitionEntry` (1496).
pub const ErrPartitionEntry: u16 = 1496;
/// `ErrMixHandler` (1497).
pub const ErrMixHandler: u16 = 1497;
/// `ErrPartitionNotDefined` (1498).
pub const ErrPartitionNotDefined: u16 = 1498;
/// `ErrTooManyPartitions` (1499).
pub const ErrTooManyPartitions: u16 = 1499;
/// `ErrSubpartition` (1500).
pub const ErrSubpartition: u16 = 1500;
/// `ErrCantCreateHandlerFile` (1501).
pub const ErrCantCreateHandlerFile: u16 = 1501;
/// `ErrBlobFieldInPartFunc` (1502).
pub const ErrBlobFieldInPartFunc: u16 = 1502;
/// `ErrUniqueKeyNeedAllFieldsInPf` (1503).
pub const ErrUniqueKeyNeedAllFieldsInPf: u16 = 1503;
/// `ErrNoParts` (1504).
pub const ErrNoParts: u16 = 1504;
/// `ErrPartitionMgmtOnNonpartitioned` (1505).
pub const ErrPartitionMgmtOnNonpartitioned: u16 = 1505;
/// `ErrForeignKeyOnPartitioned` (1506).
pub const ErrForeignKeyOnPartitioned: u16 = 1506;
/// `ErrDropPartitionNonExistent` (1507).
pub const ErrDropPartitionNonExistent: u16 = 1507;
/// `ErrDropLastPartition` (1508).
pub const ErrDropLastPartition: u16 = 1508;
/// `ErrCoalesceOnlyOnHashPartition` (1509).
pub const ErrCoalesceOnlyOnHashPartition: u16 = 1509;
/// `ErrReorgHashOnlyOnSameNo` (1510).
pub const ErrReorgHashOnlyOnSameNo: u16 = 1510;
/// `ErrReorgNoParam` (1511).
pub const ErrReorgNoParam: u16 = 1511;
/// `ErrOnlyOnRangeListPartition` (1512).
pub const ErrOnlyOnRangeListPartition: u16 = 1512;
/// `ErrAddPartitionSubpart` (1513).
pub const ErrAddPartitionSubpart: u16 = 1513;
/// `ErrAddPartitionNoNewPartition` (1514).
pub const ErrAddPartitionNoNewPartition: u16 = 1514;
/// `ErrCoalescePartitionNoPartition` (1515).
pub const ErrCoalescePartitionNoPartition: u16 = 1515;
/// `ErrReorgPartitionNotExist` (1516).
pub const ErrReorgPartitionNotExist: u16 = 1516;
/// `ErrSameNamePartition` (1517).
pub const ErrSameNamePartition: u16 = 1517;
/// `ErrNoBinlog` (1518).
pub const ErrNoBinlog: u16 = 1518;
/// `ErrConsecutiveReorgPartitions` (1519).
pub const ErrConsecutiveReorgPartitions: u16 = 1519;
/// `ErrReorgOutsideRange` (1520).
pub const ErrReorgOutsideRange: u16 = 1520;
/// `ErrPartitionFunctionFailure` (1521).
pub const ErrPartitionFunctionFailure: u16 = 1521;
/// `ErrPartState` (1522).
pub const ErrPartState: u16 = 1522;
/// `ErrLimitedPartRange` (1523).
pub const ErrLimitedPartRange: u16 = 1523;
/// `ErrPluginIsNotLoaded` (1524).
pub const ErrPluginIsNotLoaded: u16 = 1524;
/// `ErrWrongValue` (1525).
pub const ErrWrongValue: u16 = 1525;
/// `ErrNoPartitionForGivenValue` (1526).
pub const ErrNoPartitionForGivenValue: u16 = 1526;
/// `ErrFilegroupOptionOnlyOnce` (1527).
pub const ErrFilegroupOptionOnlyOnce: u16 = 1527;
/// `ErrCreateFilegroupFailed` (1528).
pub const ErrCreateFilegroupFailed: u16 = 1528;
/// `ErrDropFilegroupFailed` (1529).
pub const ErrDropFilegroupFailed: u16 = 1529;
/// `ErrTablespaceAutoExtend` (1530).
pub const ErrTablespaceAutoExtend: u16 = 1530;
/// `ErrWrongSizeNumber` (1531).
pub const ErrWrongSizeNumber: u16 = 1531;
/// `ErrSizeOverflow` (1532).
pub const ErrSizeOverflow: u16 = 1532;
/// `ErrAlterFilegroupFailed` (1533).
pub const ErrAlterFilegroupFailed: u16 = 1533;
/// `ErrBinlogRowLoggingFailed` (1534).
pub const ErrBinlogRowLoggingFailed: u16 = 1534;
/// `ErrEventAlreadyExists` (1537).
pub const ErrEventAlreadyExists: u16 = 1537;
/// `ErrEventStoreFailed` (1538).
pub const ErrEventStoreFailed: u16 = 1538;
/// `ErrEventDoesNotExist` (1539).
pub const ErrEventDoesNotExist: u16 = 1539;
/// `ErrEventCantAlter` (1540).
pub const ErrEventCantAlter: u16 = 1540;
/// `ErrEventDropFailed` (1541).
pub const ErrEventDropFailed: u16 = 1541;
/// `ErrEventIntervalNotPositiveOrTooBig` (1542).
pub const ErrEventIntervalNotPositiveOrTooBig: u16 = 1542;
/// `ErrEventEndsBeforeStarts` (1543).
pub const ErrEventEndsBeforeStarts: u16 = 1543;
/// `ErrEventExecTimeInThePast` (1544).
pub const ErrEventExecTimeInThePast: u16 = 1544;
/// `ErrEventOpenTableFailed` (1545).
pub const ErrEventOpenTableFailed: u16 = 1545;
/// `ErrEventNeitherMExprNorMAt` (1546).
pub const ErrEventNeitherMExprNorMAt: u16 = 1546;
/// `ErrObsoleteColCountDoesntMatchCorrupted` (1547).
pub const ErrObsoleteColCountDoesntMatchCorrupted: u16 = 1547;
/// `ErrObsoleteCannotLoadFromTable` (1548).
pub const ErrObsoleteCannotLoadFromTable: u16 = 1548;
/// `ErrEventCannotDelete` (1549).
pub const ErrEventCannotDelete: u16 = 1549;
/// `ErrEventCompile` (1550).
pub const ErrEventCompile: u16 = 1550;
/// `ErrEventSameName` (1551).
pub const ErrEventSameName: u16 = 1551;
/// `ErrEventDataTooLong` (1552).
pub const ErrEventDataTooLong: u16 = 1552;
/// `ErrDropIndexNeededInForeignKey` (1553).
pub const ErrDropIndexNeededInForeignKey: u16 = 1553;
/// `ErrWarnDeprecatedSyntaxWithVer` (1554).
pub const ErrWarnDeprecatedSyntaxWithVer: u16 = 1554;
/// `ErrCantWriteLockLogTable` (1555).
pub const ErrCantWriteLockLogTable: u16 = 1555;
/// `ErrCantLockLogTable` (1556).
pub const ErrCantLockLogTable: u16 = 1556;
/// `ErrForeignDuplicateKeyOldUnused` (1557).
pub const ErrForeignDuplicateKeyOldUnused: u16 = 1557;
/// `ErrColCountDoesntMatchPleaseUpdate` (1558).
pub const ErrColCountDoesntMatchPleaseUpdate: u16 = 1558;
/// `ErrTempTablePreventsSwitchOutOfRbr` (1559).
pub const ErrTempTablePreventsSwitchOutOfRbr: u16 = 1559;
/// `ErrStoredFunctionPreventsSwitchBinlogFormat` (1560).
pub const ErrStoredFunctionPreventsSwitchBinlogFormat: u16 = 1560;
/// `ErrNdbCantSwitchBinlogFormat` (1561).
pub const ErrNdbCantSwitchBinlogFormat: u16 = 1561;
/// `ErrPartitionNoTemporary` (1562).
pub const ErrPartitionNoTemporary: u16 = 1562;
/// `ErrPartitionConstDomain` (1563).
pub const ErrPartitionConstDomain: u16 = 1563;
/// `ErrPartitionFunctionIsNotAllowed` (1564).
pub const ErrPartitionFunctionIsNotAllowed: u16 = 1564;
/// `ErrDdlLog` (1565).
pub const ErrDdlLog: u16 = 1565;
/// `ErrNullInValuesLessThan` (1566).
pub const ErrNullInValuesLessThan: u16 = 1566;
/// `ErrWrongPartitionName` (1567).
pub const ErrWrongPartitionName: u16 = 1567;
/// `ErrCantChangeTxCharacteristics` (1568).
pub const ErrCantChangeTxCharacteristics: u16 = 1568;
/// `ErrDupEntryAutoincrementCase` (1569).
pub const ErrDupEntryAutoincrementCase: u16 = 1569;
/// `ErrEventModifyQueue` (1570).
pub const ErrEventModifyQueue: u16 = 1570;
/// `ErrEventSetVar` (1571).
pub const ErrEventSetVar: u16 = 1571;
/// `ErrPartitionMerge` (1572).
pub const ErrPartitionMerge: u16 = 1572;
/// `ErrCantActivateLog` (1573).
pub const ErrCantActivateLog: u16 = 1573;
/// `ErrRbrNotAvailable` (1574).
pub const ErrRbrNotAvailable: u16 = 1574;
/// `ErrBase64Decode` (1575).
pub const ErrBase64Decode: u16 = 1575;
/// `ErrEventRecursionForbidden` (1576).
pub const ErrEventRecursionForbidden: u16 = 1576;
/// `ErrEventsDB` (1577).
pub const ErrEventsDB: u16 = 1577;
/// `ErrOnlyIntegersAllowed` (1578).
pub const ErrOnlyIntegersAllowed: u16 = 1578;
/// `ErrUnsuportedLogEngine` (1579).
pub const ErrUnsuportedLogEngine: u16 = 1579;
/// `ErrBadLogStatement` (1580).
pub const ErrBadLogStatement: u16 = 1580;
/// `ErrCantRenameLogTable` (1581).
pub const ErrCantRenameLogTable: u16 = 1581;
/// `ErrWrongParamcountToNativeFct` (1582).
pub const ErrWrongParamcountToNativeFct: u16 = 1582;
/// `ErrWrongParametersToNativeFct` (1583).
pub const ErrWrongParametersToNativeFct: u16 = 1583;
/// `ErrWrongParametersToStoredFct` (1584).
pub const ErrWrongParametersToStoredFct: u16 = 1584;
/// `ErrNativeFctNameCollision` (1585).
pub const ErrNativeFctNameCollision: u16 = 1585;
/// `ErrDupEntryWithKeyName` (1586).
pub const ErrDupEntryWithKeyName: u16 = 1586;
/// `ErrBinlogPurgeEmFile` (1587).
pub const ErrBinlogPurgeEmFile: u16 = 1587;
/// `ErrEventCannotCreateInThePast` (1588).
pub const ErrEventCannotCreateInThePast: u16 = 1588;
/// `ErrEventCannotAlterInThePast` (1589).
pub const ErrEventCannotAlterInThePast: u16 = 1589;
/// `ErrNoPartitionForGivenValueSilent` (1591).
pub const ErrNoPartitionForGivenValueSilent: u16 = 1591;
/// `ErrBinlogUnsafeStatement` (1592).
pub const ErrBinlogUnsafeStatement: u16 = 1592;
/// `ErrBinlogLoggingImpossible` (1598).
pub const ErrBinlogLoggingImpossible: u16 = 1598;
/// `ErrViewNoCreationCtx` (1599).
pub const ErrViewNoCreationCtx: u16 = 1599;
/// `ErrViewInvalidCreationCtx` (1600).
pub const ErrViewInvalidCreationCtx: u16 = 1600;
/// `ErrSrInvalidCreationCtx` (1601).
pub const ErrSrInvalidCreationCtx: u16 = 1601;
/// `ErrTrgCorruptedFile` (1602).
pub const ErrTrgCorruptedFile: u16 = 1602;
/// `ErrTrgNoCreationCtx` (1603).
pub const ErrTrgNoCreationCtx: u16 = 1603;
/// `ErrTrgInvalidCreationCtx` (1604).
pub const ErrTrgInvalidCreationCtx: u16 = 1604;
/// `ErrEventInvalidCreationCtx` (1605).
pub const ErrEventInvalidCreationCtx: u16 = 1605;
/// `ErrTrgCantOpenTable` (1606).
pub const ErrTrgCantOpenTable: u16 = 1606;
/// `ErrCantCreateSroutine` (1607).
pub const ErrCantCreateSroutine: u16 = 1607;
/// `ErrNoFormatDescriptionEventBeforeBinlogStatement` (1609).
pub const ErrNoFormatDescriptionEventBeforeBinlogStatement: u16 = 1609;
/// `ErrLoadDataInvalidColumn` (1611).
pub const ErrLoadDataInvalidColumn: u16 = 1611;
/// `ErrLogPurgeNoFile` (1612).
pub const ErrLogPurgeNoFile: u16 = 1612;
/// `ErrXaRbtimeout` (1613).
pub const ErrXaRbtimeout: u16 = 1613;
/// `ErrXaRbdeadlock` (1614).
pub const ErrXaRbdeadlock: u16 = 1614;
/// `ErrNeedReprepare` (1615).
pub const ErrNeedReprepare: u16 = 1615;
/// `ErrDelayedNotSupported` (1616).
pub const ErrDelayedNotSupported: u16 = 1616;
/// `WarnOptionIgnored` (1618).
pub const WarnOptionIgnored: u16 = 1618;
/// `WarnPluginDeleteBuiltin` (1619).
pub const WarnPluginDeleteBuiltin: u16 = 1619;
/// `WarnPluginBusy` (1620).
pub const WarnPluginBusy: u16 = 1620;
/// `ErrVariableIsReadonly` (1621).
pub const ErrVariableIsReadonly: u16 = 1621;
/// `ErrWarnEngineTransactionRollback` (1622).
pub const ErrWarnEngineTransactionRollback: u16 = 1622;
/// `ErrNdbReplicationSchema` (1625).
pub const ErrNdbReplicationSchema: u16 = 1625;
/// `ErrConflictFnParse` (1626).
pub const ErrConflictFnParse: u16 = 1626;
/// `ErrExceptionsWrite` (1627).
pub const ErrExceptionsWrite: u16 = 1627;
/// `ErrTooLongTableComment` (1628).
pub const ErrTooLongTableComment: u16 = 1628;
/// `ErrTooLongFieldComment` (1629).
pub const ErrTooLongFieldComment: u16 = 1629;
/// `ErrFuncInexistentNameCollision` (1630).
pub const ErrFuncInexistentNameCollision: u16 = 1630;
/// `ErrDatabaseName` (1631).
pub const ErrDatabaseName: u16 = 1631;
/// `ErrTableName` (1632).
pub const ErrTableName: u16 = 1632;
/// `ErrPartitionName` (1633).
pub const ErrPartitionName: u16 = 1633;
/// `ErrSubpartitionName` (1634).
pub const ErrSubpartitionName: u16 = 1634;
/// `ErrTemporaryName` (1635).
pub const ErrTemporaryName: u16 = 1635;
/// `ErrRenamedName` (1636).
pub const ErrRenamedName: u16 = 1636;
/// `ErrTooManyConcurrentTrxs` (1637).
pub const ErrTooManyConcurrentTrxs: u16 = 1637;
/// `WarnNonASCIISeparatorNotImplemented` (1638).
pub const WarnNonASCIISeparatorNotImplemented: u16 = 1638;
/// `ErrDebugSyncTimeout` (1639).
pub const ErrDebugSyncTimeout: u16 = 1639;
/// `ErrDebugSyncHitLimit` (1640).
pub const ErrDebugSyncHitLimit: u16 = 1640;
/// `ErrDupSignalSet` (1641).
pub const ErrDupSignalSet: u16 = 1641;
/// `ErrSignalWarn` (1642).
pub const ErrSignalWarn: u16 = 1642;
/// `ErrSignalNotFound` (1643).
pub const ErrSignalNotFound: u16 = 1643;
/// `ErrSignalException` (1644).
pub const ErrSignalException: u16 = 1644;
/// `ErrResignalWithoutActiveHandler` (1645).
pub const ErrResignalWithoutActiveHandler: u16 = 1645;
/// `ErrSignalBadConditionType` (1646).
pub const ErrSignalBadConditionType: u16 = 1646;
/// `WarnCondItemTruncated` (1647).
pub const WarnCondItemTruncated: u16 = 1647;
/// `ErrCondItemTooLong` (1648).
pub const ErrCondItemTooLong: u16 = 1648;
/// `ErrUnknownLocale` (1649).
pub const ErrUnknownLocale: u16 = 1649;
/// `ErrQueryCacheDisabled` (1651).
pub const ErrQueryCacheDisabled: u16 = 1651;
/// `ErrSameNamePartitionField` (1652).
pub const ErrSameNamePartitionField: u16 = 1652;
/// `ErrPartitionColumnList` (1653).
pub const ErrPartitionColumnList: u16 = 1653;
/// `ErrWrongTypeColumnValue` (1654).
pub const ErrWrongTypeColumnValue: u16 = 1654;
/// `ErrTooManyPartitionFuncFields` (1655).
pub const ErrTooManyPartitionFuncFields: u16 = 1655;
/// `ErrMaxvalueInValuesIn` (1656).
pub const ErrMaxvalueInValuesIn: u16 = 1656;
/// `ErrTooManyValues` (1657).
pub const ErrTooManyValues: u16 = 1657;
/// `ErrRowSinglePartitionField` (1658).
pub const ErrRowSinglePartitionField: u16 = 1658;
/// `ErrFieldTypeNotAllowedAsPartitionField` (1659).
pub const ErrFieldTypeNotAllowedAsPartitionField: u16 = 1659;
/// `ErrPartitionFieldsTooLong` (1660).
pub const ErrPartitionFieldsTooLong: u16 = 1660;
/// `ErrBinlogRowEngineAndStmtEngine` (1661).
pub const ErrBinlogRowEngineAndStmtEngine: u16 = 1661;
/// `ErrBinlogRowModeAndStmtEngine` (1662).
pub const ErrBinlogRowModeAndStmtEngine: u16 = 1662;
/// `ErrBinlogUnsafeAndStmtEngine` (1663).
pub const ErrBinlogUnsafeAndStmtEngine: u16 = 1663;
/// `ErrBinlogRowInjectionAndStmtEngine` (1664).
pub const ErrBinlogRowInjectionAndStmtEngine: u16 = 1664;
/// `ErrBinlogStmtModeAndRowEngine` (1665).
pub const ErrBinlogStmtModeAndRowEngine: u16 = 1665;
/// `ErrBinlogRowInjectionAndStmtMode` (1666).
pub const ErrBinlogRowInjectionAndStmtMode: u16 = 1666;
/// `ErrBinlogMultipleEnginesAndSelfLoggingEngine` (1667).
pub const ErrBinlogMultipleEnginesAndSelfLoggingEngine: u16 = 1667;
/// `ErrBinlogUnsafeLimit` (1668).
pub const ErrBinlogUnsafeLimit: u16 = 1668;
/// `ErrBinlogUnsafeInsertDelayed` (1669).
pub const ErrBinlogUnsafeInsertDelayed: u16 = 1669;
/// `ErrBinlogUnsafeAutoincColumns` (1671).
pub const ErrBinlogUnsafeAutoincColumns: u16 = 1671;
/// `ErrBinlogUnsafeSystemFunction` (1674).
pub const ErrBinlogUnsafeSystemFunction: u16 = 1674;
/// `ErrBinlogUnsafeNontransAfterTrans` (1675).
pub const ErrBinlogUnsafeNontransAfterTrans: u16 = 1675;
/// `ErrMessageAndStatement` (1676).
pub const ErrMessageAndStatement: u16 = 1676;
/// `ErrInsideTransactionPreventsSwitchBinlogFormat` (1679).
pub const ErrInsideTransactionPreventsSwitchBinlogFormat: u16 = 1679;
/// `ErrPathLength` (1680).
pub const ErrPathLength: u16 = 1680;
/// `ErrWarnDeprecatedSyntaxNoReplacement` (1681).
pub const ErrWarnDeprecatedSyntaxNoReplacement: u16 = 1681;
/// `ErrWrongNativeTableStructure` (1682).
pub const ErrWrongNativeTableStructure: u16 = 1682;
/// `ErrWrongPerfSchemaUsage` (1683).
pub const ErrWrongPerfSchemaUsage: u16 = 1683;
/// `ErrWarnISSkippedTable` (1684).
pub const ErrWarnISSkippedTable: u16 = 1684;
/// `ErrInsideTransactionPreventsSwitchBinlogDirect` (1685).
pub const ErrInsideTransactionPreventsSwitchBinlogDirect: u16 = 1685;
/// `ErrStoredFunctionPreventsSwitchBinlogDirect` (1686).
pub const ErrStoredFunctionPreventsSwitchBinlogDirect: u16 = 1686;
/// `ErrSpatialMustHaveGeomCol` (1687).
pub const ErrSpatialMustHaveGeomCol: u16 = 1687;
/// `ErrTooLongIndexComment` (1688).
pub const ErrTooLongIndexComment: u16 = 1688;
/// `ErrLockAborted` (1689).
pub const ErrLockAborted: u16 = 1689;
/// `ErrDataOutOfRange` (1690).
pub const ErrDataOutOfRange: u16 = 1690;
/// `ErrWrongSpvarTypeInLimit` (1691).
pub const ErrWrongSpvarTypeInLimit: u16 = 1691;
/// `ErrBinlogUnsafeMultipleEnginesAndSelfLoggingEngine` (1692).
pub const ErrBinlogUnsafeMultipleEnginesAndSelfLoggingEngine: u16 = 1692;
/// `ErrBinlogUnsafeMixedStatement` (1693).
pub const ErrBinlogUnsafeMixedStatement: u16 = 1693;
/// `ErrInsideTransactionPreventsSwitchSQLLogBin` (1694).
pub const ErrInsideTransactionPreventsSwitchSQLLogBin: u16 = 1694;
/// `ErrStoredFunctionPreventsSwitchSQLLogBin` (1695).
pub const ErrStoredFunctionPreventsSwitchSQLLogBin: u16 = 1695;
/// `ErrFailedReadFromParFile` (1696).
pub const ErrFailedReadFromParFile: u16 = 1696;
/// `ErrValuesIsNotIntType` (1697).
pub const ErrValuesIsNotIntType: u16 = 1697;
/// `ErrAccessDeniedNoPassword` (1698).
pub const ErrAccessDeniedNoPassword: u16 = 1698;
/// `ErrSetPasswordAuthPlugin` (1699).
pub const ErrSetPasswordAuthPlugin: u16 = 1699;
/// `ErrGrantPluginUserExists` (1700).
pub const ErrGrantPluginUserExists: u16 = 1700;
/// `ErrTruncateIllegalForeignKey` (1701).
pub const ErrTruncateIllegalForeignKey: u16 = 1701;
/// `ErrPluginIsPermanent` (1702).
pub const ErrPluginIsPermanent: u16 = 1702;
/// `ErrStmtCacheFull` (1705).
pub const ErrStmtCacheFull: u16 = 1705;
/// `ErrMultiUpdateKeyConflict` (1706).
pub const ErrMultiUpdateKeyConflict: u16 = 1706;
/// `ErrTableNeedsRebuild` (1707).
pub const ErrTableNeedsRebuild: u16 = 1707;
/// `WarnOptionBelowLimit` (1708).
pub const WarnOptionBelowLimit: u16 = 1708;
/// `ErrIndexColumnTooLong` (1709).
pub const ErrIndexColumnTooLong: u16 = 1709;
/// `ErrErrorInTriggerBody` (1710).
pub const ErrErrorInTriggerBody: u16 = 1710;
/// `ErrErrorInUnknownTriggerBody` (1711).
pub const ErrErrorInUnknownTriggerBody: u16 = 1711;
/// `ErrIndexCorrupt` (1712).
pub const ErrIndexCorrupt: u16 = 1712;
/// `ErrUndoRecordTooBig` (1713).
pub const ErrUndoRecordTooBig: u16 = 1713;
/// `ErrPluginNoUninstall` (1720).
pub const ErrPluginNoUninstall: u16 = 1720;
/// `ErrPluginNoInstall` (1721).
pub const ErrPluginNoInstall: u16 = 1721;
/// `ErrBinlogUnsafeInsertTwoKeys` (1724).
pub const ErrBinlogUnsafeInsertTwoKeys: u16 = 1724;
/// `ErrTableInFkCheck` (1725).
pub const ErrTableInFkCheck: u16 = 1725;
/// `ErrUnsupportedEngine` (1726).
pub const ErrUnsupportedEngine: u16 = 1726;
/// `ErrBinlogUnsafeAutoincNotFirst` (1727).
pub const ErrBinlogUnsafeAutoincNotFirst: u16 = 1727;
/// `ErrCannotLoadFromTableV2` (1728).
pub const ErrCannotLoadFromTableV2: u16 = 1728;
/// `ErrOnlyFdAndRbrEventsAllowedInBinlogStatement` (1730).
pub const ErrOnlyFdAndRbrEventsAllowedInBinlogStatement: u16 = 1730;
/// `ErrPartitionExchangeDifferentOption` (1731).
pub const ErrPartitionExchangeDifferentOption: u16 = 1731;
/// `ErrPartitionExchangePartTable` (1732).
pub const ErrPartitionExchangePartTable: u16 = 1732;
/// `ErrPartitionExchangeTempTable` (1733).
pub const ErrPartitionExchangeTempTable: u16 = 1733;
/// `ErrPartitionInsteadOfSubpartition` (1734).
pub const ErrPartitionInsteadOfSubpartition: u16 = 1734;
/// `ErrUnknownPartition` (1735).
pub const ErrUnknownPartition: u16 = 1735;
/// `ErrTablesDifferentMetadata` (1736).
pub const ErrTablesDifferentMetadata: u16 = 1736;
/// `ErrRowDoesNotMatchPartition` (1737).
pub const ErrRowDoesNotMatchPartition: u16 = 1737;
/// `ErrBinlogCacheSizeGreaterThanMax` (1738).
pub const ErrBinlogCacheSizeGreaterThanMax: u16 = 1738;
/// `ErrWarnIndexNotApplicable` (1739).
pub const ErrWarnIndexNotApplicable: u16 = 1739;
/// `ErrPartitionExchangeForeignKey` (1740).
pub const ErrPartitionExchangeForeignKey: u16 = 1740;
/// `ErrNoSuchKeyValue` (1741).
pub const ErrNoSuchKeyValue: u16 = 1741;
/// `ErrRplInfoDataTooLong` (1742).
pub const ErrRplInfoDataTooLong: u16 = 1742;
/// `ErrNetworkReadEventChecksumFailure` (1743).
pub const ErrNetworkReadEventChecksumFailure: u16 = 1743;
/// `ErrBinlogReadEventChecksumFailure` (1744).
pub const ErrBinlogReadEventChecksumFailure: u16 = 1744;
/// `ErrBinlogStmtCacheSizeGreaterThanMax` (1745).
pub const ErrBinlogStmtCacheSizeGreaterThanMax: u16 = 1745;
/// `ErrCantUpdateTableInCreateTableSelect` (1746).
pub const ErrCantUpdateTableInCreateTableSelect: u16 = 1746;
/// `ErrPartitionClauseOnNonpartitioned` (1747).
pub const ErrPartitionClauseOnNonpartitioned: u16 = 1747;
/// `ErrRowDoesNotMatchGivenPartitionSet` (1748).
pub const ErrRowDoesNotMatchGivenPartitionSet: u16 = 1748;
/// `ErrNoSuchPartitionunused` (1749).
pub const ErrNoSuchPartitionunused: u16 = 1749;
/// `ErrChangeRplInfoRepositoryFailure` (1750).
pub const ErrChangeRplInfoRepositoryFailure: u16 = 1750;
/// `ErrWarningNotCompleteRollbackWithCreatedTempTable` (1751).
pub const ErrWarningNotCompleteRollbackWithCreatedTempTable: u16 = 1751;
/// `ErrWarningNotCompleteRollbackWithDroppedTempTable` (1752).
pub const ErrWarningNotCompleteRollbackWithDroppedTempTable: u16 = 1752;
/// `ErrMtsUpdatedDBsGreaterMax` (1754).
pub const ErrMtsUpdatedDBsGreaterMax: u16 = 1754;
/// `ErrMtsCantParallel` (1755).
pub const ErrMtsCantParallel: u16 = 1755;
/// `ErrMtsInconsistentData` (1756).
pub const ErrMtsInconsistentData: u16 = 1756;
/// `ErrFulltextNotSupportedWithPartitioning` (1757).
pub const ErrFulltextNotSupportedWithPartitioning: u16 = 1757;
/// `ErrDaInvalidConditionNumber` (1758).
pub const ErrDaInvalidConditionNumber: u16 = 1758;
/// `ErrInsecurePlainText` (1759).
pub const ErrInsecurePlainText: u16 = 1759;
/// `ErrForeignDuplicateKeyWithChildInfo` (1761).
pub const ErrForeignDuplicateKeyWithChildInfo: u16 = 1761;
/// `ErrForeignDuplicateKeyWithoutChildInfo` (1762).
pub const ErrForeignDuplicateKeyWithoutChildInfo: u16 = 1762;
/// `ErrTableHasNoFt` (1764).
pub const ErrTableHasNoFt: u16 = 1764;
/// `ErrVariableNotSettableInSfOrTrigger` (1765).
pub const ErrVariableNotSettableInSfOrTrigger: u16 = 1765;
/// `ErrVariableNotSettableInTransaction` (1766).
pub const ErrVariableNotSettableInTransaction: u16 = 1766;
/// `ErrGtidNextIsNotInGtidNextList` (1767).
pub const ErrGtidNextIsNotInGtidNextList: u16 = 1767;
/// `ErrCantChangeGtidNextInTransactionWhenGtidNextListIsNull` (1768).
pub const ErrCantChangeGtidNextInTransactionWhenGtidNextListIsNull: u16 = 1768;
/// `ErrSetStatementCannotInvokeFunction` (1769).
pub const ErrSetStatementCannotInvokeFunction: u16 = 1769;
/// `ErrGtidNextCantBeAutomaticIfGtidNextListIsNonNull` (1770).
pub const ErrGtidNextCantBeAutomaticIfGtidNextListIsNonNull: u16 = 1770;
/// `ErrSkippingLoggedTransaction` (1771).
pub const ErrSkippingLoggedTransaction: u16 = 1771;
/// `ErrMalformedGtidSetSpecification` (1772).
pub const ErrMalformedGtidSetSpecification: u16 = 1772;
/// `ErrMalformedGtidSetEncoding` (1773).
pub const ErrMalformedGtidSetEncoding: u16 = 1773;
/// `ErrMalformedGtidSpecification` (1774).
pub const ErrMalformedGtidSpecification: u16 = 1774;
/// `ErrGnoExhausted` (1775).
pub const ErrGnoExhausted: u16 = 1775;
/// `ErrCantDoImplicitCommitInTrxWhenGtidNextIsSet` (1778).
pub const ErrCantDoImplicitCommitInTrxWhenGtidNextIsSet: u16 = 1778;
/// `ErrGtidMode2Or3RequiresEnforceGtidConsistencyOn` (1779).
pub const ErrGtidMode2Or3RequiresEnforceGtidConsistencyOn: u16 = 1779;
/// `ErrCantSetGtidNextToGtidWhenGtidModeIsOff` (1781).
pub const ErrCantSetGtidNextToGtidWhenGtidModeIsOff: u16 = 1781;
/// `ErrCantSetGtidNextToAnonymousWhenGtidModeIsOn` (1782).
pub const ErrCantSetGtidNextToAnonymousWhenGtidModeIsOn: u16 = 1782;
/// `ErrCantSetGtidNextListToNonNullWhenGtidModeIsOff` (1783).
pub const ErrCantSetGtidNextListToNonNullWhenGtidModeIsOff: u16 = 1783;
/// `ErrFoundGtidEventWhenGtidModeIsOff` (1784).
pub const ErrFoundGtidEventWhenGtidModeIsOff: u16 = 1784;
/// `ErrGtidUnsafeNonTransactionalTable` (1785).
pub const ErrGtidUnsafeNonTransactionalTable: u16 = 1785;
/// `ErrGtidUnsafeCreateSelect` (1786).
pub const ErrGtidUnsafeCreateSelect: u16 = 1786;
/// `ErrGtidUnsafeCreateDropTemporaryTableInTransaction` (1787).
pub const ErrGtidUnsafeCreateDropTemporaryTableInTransaction: u16 = 1787;
/// `ErrGtidModeCanOnlyChangeOneStepAtATime` (1788).
pub const ErrGtidModeCanOnlyChangeOneStepAtATime: u16 = 1788;
/// `ErrCantSetGtidNextWhenOwningGtid` (1790).
pub const ErrCantSetGtidNextWhenOwningGtid: u16 = 1790;
/// `ErrUnknownExplainFormat` (1791).
pub const ErrUnknownExplainFormat: u16 = 1791;
/// `ErrCantExecuteInReadOnlyTransaction` (1792).
pub const ErrCantExecuteInReadOnlyTransaction: u16 = 1792;
/// `ErrTooLongTablePartitionComment` (1793).
pub const ErrTooLongTablePartitionComment: u16 = 1793;
/// `ErrInnodbFtLimit` (1795).
pub const ErrInnodbFtLimit: u16 = 1795;
/// `ErrInnodbNoFtTempTable` (1796).
pub const ErrInnodbNoFtTempTable: u16 = 1796;
/// `ErrInnodbFtWrongDocidColumn` (1797).
pub const ErrInnodbFtWrongDocidColumn: u16 = 1797;
/// `ErrInnodbFtWrongDocidIndex` (1798).
pub const ErrInnodbFtWrongDocidIndex: u16 = 1798;
/// `ErrInnodbOnlineLogTooBig` (1799).
pub const ErrInnodbOnlineLogTooBig: u16 = 1799;
/// `ErrUnknownAlterAlgorithm` (1800).
pub const ErrUnknownAlterAlgorithm: u16 = 1800;
/// `ErrUnknownAlterLock` (1801).
pub const ErrUnknownAlterLock: u16 = 1801;
/// `ErrMtsResetWorkers` (1804).
pub const ErrMtsResetWorkers: u16 = 1804;
/// `ErrColCountDoesntMatchCorruptedV2` (1805).
pub const ErrColCountDoesntMatchCorruptedV2: u16 = 1805;
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

/// Every named source constant, including boundary aliases.
pub const ALL_CODES: &[(&str, u16)] = &[
    ("ErrErrorFirst", ErrErrorFirst),
    ("ErrHashchk", ErrHashchk),
    ("ErrNisamchk", ErrNisamchk),
    ("ErrNo", ErrNo),
    ("ErrYes", ErrYes),
    ("ErrCantCreateFile", ErrCantCreateFile),
    ("ErrCantCreateTable", ErrCantCreateTable),
    ("ErrCantCreateDB", ErrCantCreateDB),
    ("ErrDBCreateExists", ErrDBCreateExists),
    ("ErrDBDropExists", ErrDBDropExists),
    ("ErrDBDropDelete", ErrDBDropDelete),
    ("ErrDBDropRmdir", ErrDBDropRmdir),
    ("ErrCantDeleteFile", ErrCantDeleteFile),
    ("ErrCantFindSystemRec", ErrCantFindSystemRec),
    ("ErrCantGetStat", ErrCantGetStat),
    ("ErrCantGetWd", ErrCantGetWd),
    ("ErrCantLock", ErrCantLock),
    ("ErrCantOpenFile", ErrCantOpenFile),
    ("ErrFileNotFound", ErrFileNotFound),
    ("ErrCantReadDir", ErrCantReadDir),
    ("ErrCantSetWd", ErrCantSetWd),
    ("ErrCheckread", ErrCheckread),
    ("ErrDiskFull", ErrDiskFull),
    ("ErrDupKey", ErrDupKey),
    ("ErrErrorOnClose", ErrErrorOnClose),
    ("ErrErrorOnRead", ErrErrorOnRead),
    ("ErrErrorOnRename", ErrErrorOnRename),
    ("ErrErrorOnWrite", ErrErrorOnWrite),
    ("ErrFileUsed", ErrFileUsed),
    ("ErrFilsortAbort", ErrFilsortAbort),
    ("ErrFormNotFound", ErrFormNotFound),
    ("ErrGetErrno", ErrGetErrno),
    ("ErrIllegalHa", ErrIllegalHa),
    ("ErrKeyNotFound", ErrKeyNotFound),
    ("ErrNotFormFile", ErrNotFormFile),
    ("ErrNotKeyFile", ErrNotKeyFile),
    ("ErrOldKeyFile", ErrOldKeyFile),
    ("ErrOpenAsReadonly", ErrOpenAsReadonly),
    ("ErrOutofMemory", ErrOutofMemory),
    ("ErrOutOfSortMemory", ErrOutOfSortMemory),
    ("ErrUnexpectedEOF", ErrUnexpectedEOF),
    ("ErrConCount", ErrConCount),
    ("ErrOutOfResources", ErrOutOfResources),
    ("ErrBadHost", ErrBadHost),
    ("ErrHandshake", ErrHandshake),
    ("ErrDBaccessDenied", ErrDBaccessDenied),
    ("ErrAccessDenied", ErrAccessDenied),
    ("ErrNoDB", ErrNoDB),
    ("ErrUnknownCom", ErrUnknownCom),
    ("ErrBadNull", ErrBadNull),
    ("ErrBadDB", ErrBadDB),
    ("ErrTableExists", ErrTableExists),
    ("ErrBadTable", ErrBadTable),
    ("ErrNonUniq", ErrNonUniq),
    ("ErrServerShutdown", ErrServerShutdown),
    ("ErrBadField", ErrBadField),
    ("ErrFieldNotInGroupBy", ErrFieldNotInGroupBy),
    ("ErrWrongGroupField", ErrWrongGroupField),
    ("ErrWrongSumSelect", ErrWrongSumSelect),
    ("ErrWrongValueCount", ErrWrongValueCount),
    ("ErrTooLongIdent", ErrTooLongIdent),
    ("ErrDupFieldName", ErrDupFieldName),
    ("ErrDupKeyName", ErrDupKeyName),
    ("ErrDupEntry", ErrDupEntry),
    ("ErrWrongFieldSpec", ErrWrongFieldSpec),
    ("ErrParse", ErrParse),
    ("ErrEmptyQuery", ErrEmptyQuery),
    ("ErrNonuniqTable", ErrNonuniqTable),
    ("ErrInvalidDefault", ErrInvalidDefault),
    ("ErrMultiplePriKey", ErrMultiplePriKey),
    ("ErrTooManyKeys", ErrTooManyKeys),
    ("ErrTooManyKeyParts", ErrTooManyKeyParts),
    ("ErrTooLongKey", ErrTooLongKey),
    ("ErrKeyColumnDoesNotExits", ErrKeyColumnDoesNotExits),
    ("ErrBlobUsedAsKey", ErrBlobUsedAsKey),
    ("ErrTooBigFieldlength", ErrTooBigFieldlength),
    ("ErrWrongAutoKey", ErrWrongAutoKey),
    ("ErrReady", ErrReady),
    ("ErrNormalShutdown", ErrNormalShutdown),
    ("ErrGotSignal", ErrGotSignal),
    ("ErrShutdownComplete", ErrShutdownComplete),
    ("ErrForcingClose", ErrForcingClose),
    ("ErrIpsock", ErrIpsock),
    ("ErrNoSuchIndex", ErrNoSuchIndex),
    ("ErrWrongFieldTerminators", ErrWrongFieldTerminators),
    ("ErrBlobsAndNoTerminated", ErrBlobsAndNoTerminated),
    ("ErrTextFileNotReadable", ErrTextFileNotReadable),
    ("ErrFileExists", ErrFileExists),
    ("ErrLoadInfo", ErrLoadInfo),
    ("ErrAlterInfo", ErrAlterInfo),
    ("ErrWrongSubKey", ErrWrongSubKey),
    ("ErrCantRemoveAllFields", ErrCantRemoveAllFields),
    ("ErrCantDropFieldOrKey", ErrCantDropFieldOrKey),
    ("ErrInsertInfo", ErrInsertInfo),
    ("ErrUpdateTableUsed", ErrUpdateTableUsed),
    ("ErrNoSuchThread", ErrNoSuchThread),
    ("ErrKillDenied", ErrKillDenied),
    ("ErrNoTablesUsed", ErrNoTablesUsed),
    ("ErrTooBigSet", ErrTooBigSet),
    ("ErrNoUniqueLogFile", ErrNoUniqueLogFile),
    ("ErrTableNotLockedForWrite", ErrTableNotLockedForWrite),
    ("ErrTableNotLocked", ErrTableNotLocked),
    ("ErrBlobCantHaveDefault", ErrBlobCantHaveDefault),
    ("ErrWrongDBName", ErrWrongDBName),
    ("ErrWrongTableName", ErrWrongTableName),
    ("ErrTooBigSelect", ErrTooBigSelect),
    ("ErrUnknown", ErrUnknown),
    ("ErrUnknownProcedure", ErrUnknownProcedure),
    (
        "ErrWrongParamcountToProcedure",
        ErrWrongParamcountToProcedure,
    ),
    (
        "ErrWrongParametersToProcedure",
        ErrWrongParametersToProcedure,
    ),
    ("ErrUnknownTable", ErrUnknownTable),
    ("ErrFieldSpecifiedTwice", ErrFieldSpecifiedTwice),
    ("ErrInvalidGroupFuncUse", ErrInvalidGroupFuncUse),
    ("ErrUnsupportedExtension", ErrUnsupportedExtension),
    ("ErrTableMustHaveColumns", ErrTableMustHaveColumns),
    ("ErrRecordFileFull", ErrRecordFileFull),
    ("ErrUnknownCharacterSet", ErrUnknownCharacterSet),
    ("ErrTooManyTables", ErrTooManyTables),
    ("ErrTooManyFields", ErrTooManyFields),
    ("ErrTooBigRowsize", ErrTooBigRowsize),
    ("ErrStackOverrun", ErrStackOverrun),
    ("ErrWrongOuterJoin", ErrWrongOuterJoin),
    ("ErrNullColumnInIndex", ErrNullColumnInIndex),
    ("ErrCantFindUdf", ErrCantFindUdf),
    ("ErrCantInitializeUdf", ErrCantInitializeUdf),
    ("ErrUdfNoPaths", ErrUdfNoPaths),
    ("ErrUdfExists", ErrUdfExists),
    ("ErrCantOpenLibrary", ErrCantOpenLibrary),
    ("ErrCantFindDlEntry", ErrCantFindDlEntry),
    ("ErrFunctionNotDefined", ErrFunctionNotDefined),
    ("ErrHostIsBlocked", ErrHostIsBlocked),
    ("ErrHostNotPrivileged", ErrHostNotPrivileged),
    ("ErrPasswordAnonymousUser", ErrPasswordAnonymousUser),
    ("ErrPasswordNotAllowed", ErrPasswordNotAllowed),
    ("ErrPasswordNoMatch", ErrPasswordNoMatch),
    ("ErrUpdateInfo", ErrUpdateInfo),
    ("ErrCantCreateThread", ErrCantCreateThread),
    ("ErrWrongValueCountOnRow", ErrWrongValueCountOnRow),
    ("ErrCantReopenTable", ErrCantReopenTable),
    ("ErrInvalidUseOfNull", ErrInvalidUseOfNull),
    ("ErrRegexp", ErrRegexp),
    ("ErrMixOfGroupFuncAndFields", ErrMixOfGroupFuncAndFields),
    ("ErrNonexistingGrant", ErrNonexistingGrant),
    ("ErrTableaccessDenied", ErrTableaccessDenied),
    ("ErrColumnaccessDenied", ErrColumnaccessDenied),
    ("ErrIllegalGrantForTable", ErrIllegalGrantForTable),
    ("ErrGrantWrongHostOrUser", ErrGrantWrongHostOrUser),
    ("ErrNoSuchTable", ErrNoSuchTable),
    ("ErrNonexistingTableGrant", ErrNonexistingTableGrant),
    ("ErrNotAllowedCommand", ErrNotAllowedCommand),
    ("ErrSyntax", ErrSyntax),
    ("ErrDelayedCantChangeLock", ErrDelayedCantChangeLock),
    ("ErrTooManyDelayedThreads", ErrTooManyDelayedThreads),
    ("ErrAbortingConnection", ErrAbortingConnection),
    ("ErrNetPacketTooLarge", ErrNetPacketTooLarge),
    ("ErrNetReadErrorFromPipe", ErrNetReadErrorFromPipe),
    ("ErrNetFcntl", ErrNetFcntl),
    ("ErrNetPacketsOutOfOrder", ErrNetPacketsOutOfOrder),
    ("ErrNetUncompress", ErrNetUncompress),
    ("ErrNetRead", ErrNetRead),
    ("ErrNetReadInterrupted", ErrNetReadInterrupted),
    ("ErrNetErrorOnWrite", ErrNetErrorOnWrite),
    ("ErrNetWriteInterrupted", ErrNetWriteInterrupted),
    ("ErrTooLongString", ErrTooLongString),
    ("ErrTableCantHandleBlob", ErrTableCantHandleBlob),
    (
        "ErrTableCantHandleAutoIncrement",
        ErrTableCantHandleAutoIncrement,
    ),
    ("ErrDelayedInsertTableLocked", ErrDelayedInsertTableLocked),
    ("ErrWrongColumnName", ErrWrongColumnName),
    ("ErrWrongKeyColumn", ErrWrongKeyColumn),
    ("ErrWrongMrgTable", ErrWrongMrgTable),
    ("ErrDupUnique", ErrDupUnique),
    ("ErrBlobKeyWithoutLength", ErrBlobKeyWithoutLength),
    ("ErrPrimaryCantHaveNull", ErrPrimaryCantHaveNull),
    ("ErrTooManyRows", ErrTooManyRows),
    ("ErrRequiresPrimaryKey", ErrRequiresPrimaryKey),
    ("ErrNoRaidCompiled", ErrNoRaidCompiled),
    (
        "ErrUpdateWithoutKeyInSafeMode",
        ErrUpdateWithoutKeyInSafeMode,
    ),
    ("ErrKeyDoesNotExist", ErrKeyDoesNotExist),
    ("ErrCheckNoSuchTable", ErrCheckNoSuchTable),
    ("ErrCheckNotImplemented", ErrCheckNotImplemented),
    (
        "ErrCantDoThisDuringAnTransaction",
        ErrCantDoThisDuringAnTransaction,
    ),
    ("ErrErrorDuringCommit", ErrErrorDuringCommit),
    ("ErrErrorDuringRollback", ErrErrorDuringRollback),
    ("ErrErrorDuringFlushLogs", ErrErrorDuringFlushLogs),
    ("ErrErrorDuringCheckpoint", ErrErrorDuringCheckpoint),
    ("ErrNewAbortingConnection", ErrNewAbortingConnection),
    ("ErrDumpNotImplemented", ErrDumpNotImplemented),
    ("ErrIndexRebuild", ErrIndexRebuild),
    ("ErrFtMatchingKeyNotFound", ErrFtMatchingKeyNotFound),
    ("ErrLockOrActiveTransaction", ErrLockOrActiveTransaction),
    ("ErrUnknownSystemVariable", ErrUnknownSystemVariable),
    ("ErrCrashedOnUsage", ErrCrashedOnUsage),
    ("ErrCrashedOnRepair", ErrCrashedOnRepair),
    (
        "ErrWarningNotCompleteRollback",
        ErrWarningNotCompleteRollback,
    ),
    ("ErrTransCacheFull", ErrTransCacheFull),
    ("ErrTooManyUserConnections", ErrTooManyUserConnections),
    ("ErrSetConstantsOnly", ErrSetConstantsOnly),
    ("ErrLockWaitTimeout", ErrLockWaitTimeout),
    ("ErrLockTableFull", ErrLockTableFull),
    ("ErrReadOnlyTransaction", ErrReadOnlyTransaction),
    ("ErrDropDBWithReadLock", ErrDropDBWithReadLock),
    ("ErrCreateDBWithReadLock", ErrCreateDBWithReadLock),
    ("ErrWrongArguments", ErrWrongArguments),
    ("ErrNoPermissionToCreateUser", ErrNoPermissionToCreateUser),
    ("ErrUnionTablesInDifferentDir", ErrUnionTablesInDifferentDir),
    ("ErrLockDeadlock", ErrLockDeadlock),
    ("ErrTableCantHandleFt", ErrTableCantHandleFt),
    ("ErrCannotAddForeign", ErrCannotAddForeign),
    ("ErrNoReferencedRow", ErrNoReferencedRow),
    ("ErrRowIsReferenced", ErrRowIsReferenced),
    ("ErrErrorWhenExecutingCommand", ErrErrorWhenExecutingCommand),
    ("ErrWrongUsage", ErrWrongUsage),
    (
        "ErrWrongNumberOfColumnsInSelect",
        ErrWrongNumberOfColumnsInSelect,
    ),
    ("ErrCantUpdateWithReadlock", ErrCantUpdateWithReadlock),
    ("ErrMixingNotAllowed", ErrMixingNotAllowed),
    ("ErrDupArgument", ErrDupArgument),
    ("ErrUserLimitReached", ErrUserLimitReached),
    ("ErrSpecificAccessDenied", ErrSpecificAccessDenied),
    ("ErrLocalVariable", ErrLocalVariable),
    ("ErrGlobalVariable", ErrGlobalVariable),
    ("ErrNoDefault", ErrNoDefault),
    ("ErrWrongValueForVar", ErrWrongValueForVar),
    ("ErrWrongTypeForVar", ErrWrongTypeForVar),
    ("ErrVarCantBeRead", ErrVarCantBeRead),
    ("ErrCantUseOptionHere", ErrCantUseOptionHere),
    ("ErrNotSupportedYet", ErrNotSupportedYet),
    ("ErrIncorrectGlobalLocalVar", ErrIncorrectGlobalLocalVar),
    ("ErrWrongFkDef", ErrWrongFkDef),
    ("ErrKeyRefDoNotMatchTableRef", ErrKeyRefDoNotMatchTableRef),
    ("ErrOperandColumns", ErrOperandColumns),
    ("ErrSubqueryNo1Row", ErrSubqueryNo1Row),
    ("ErrUnknownStmtHandler", ErrUnknownStmtHandler),
    ("ErrCorruptHelpDB", ErrCorruptHelpDB),
    ("ErrCyclicReference", ErrCyclicReference),
    ("ErrAutoConvert", ErrAutoConvert),
    ("ErrIllegalReference", ErrIllegalReference),
    ("ErrDerivedMustHaveAlias", ErrDerivedMustHaveAlias),
    ("ErrSelectReduced", ErrSelectReduced),
    ("ErrTablenameNotAllowedHere", ErrTablenameNotAllowedHere),
    ("ErrNotSupportedAuthMode", ErrNotSupportedAuthMode),
    ("ErrSpatialCantHaveNull", ErrSpatialCantHaveNull),
    ("ErrCollationCharsetMismatch", ErrCollationCharsetMismatch),
    ("ErrTooBigForUncompress", ErrTooBigForUncompress),
    ("ErrZlibZMem", ErrZlibZMem),
    ("ErrZlibZBuf", ErrZlibZBuf),
    ("ErrZlibZData", ErrZlibZData),
    ("ErrCutValueGroupConcat", ErrCutValueGroupConcat),
    ("ErrWarnTooFewRecords", ErrWarnTooFewRecords),
    ("ErrWarnTooManyRecords", ErrWarnTooManyRecords),
    ("ErrWarnNullToNotnull", ErrWarnNullToNotnull),
    ("ErrWarnDataOutOfRange", ErrWarnDataOutOfRange),
    ("WarnDataTruncated", WarnDataTruncated),
    ("ErrWarnUsingOtherHandler", ErrWarnUsingOtherHandler),
    ("ErrCantAggregate2collations", ErrCantAggregate2collations),
    ("ErrDropUser", ErrDropUser),
    ("ErrRevokeGrants", ErrRevokeGrants),
    ("ErrCantAggregate3collations", ErrCantAggregate3collations),
    ("ErrCantAggregateNcollations", ErrCantAggregateNcollations),
    ("ErrVariableIsNotStruct", ErrVariableIsNotStruct),
    ("ErrUnknownCollation", ErrUnknownCollation),
    ("ErrServerIsInSecureAuthMode", ErrServerIsInSecureAuthMode),
    ("ErrWarnFieldResolved", ErrWarnFieldResolved),
    ("ErrUntilCondIgnored", ErrUntilCondIgnored),
    ("ErrWrongNameForIndex", ErrWrongNameForIndex),
    ("ErrWrongNameForCatalog", ErrWrongNameForCatalog),
    ("ErrWarnQcResize", ErrWarnQcResize),
    ("ErrBadFtColumn", ErrBadFtColumn),
    ("ErrUnknownKeyCache", ErrUnknownKeyCache),
    ("ErrWarnHostnameWontWork", ErrWarnHostnameWontWork),
    ("ErrUnknownStorageEngine", ErrUnknownStorageEngine),
    ("ErrWarnDeprecatedSyntax", ErrWarnDeprecatedSyntax),
    ("ErrNonUpdatableTable", ErrNonUpdatableTable),
    ("ErrFeatureDisabled", ErrFeatureDisabled),
    ("ErrOptionPreventsStatement", ErrOptionPreventsStatement),
    ("ErrDuplicatedValueInType", ErrDuplicatedValueInType),
    ("ErrTruncatedWrongValue", ErrTruncatedWrongValue),
    ("ErrTooMuchAutoTimestampCols", ErrTooMuchAutoTimestampCols),
    ("ErrInvalidOnUpdate", ErrInvalidOnUpdate),
    ("ErrUnsupportedPs", ErrUnsupportedPs),
    ("ErrGetErrmsg", ErrGetErrmsg),
    ("ErrGetTemporaryErrmsg", ErrGetTemporaryErrmsg),
    ("ErrUnknownTimeZone", ErrUnknownTimeZone),
    ("ErrWarnInvalidTimestamp", ErrWarnInvalidTimestamp),
    ("ErrInvalidCharacterString", ErrInvalidCharacterString),
    (
        "ErrWarnAllowedPacketOverflowed",
        ErrWarnAllowedPacketOverflowed,
    ),
    ("ErrConflictingDeclarations", ErrConflictingDeclarations),
    ("ErrSpNoRecursiveCreate", ErrSpNoRecursiveCreate),
    ("ErrSpAlreadyExists", ErrSpAlreadyExists),
    ("ErrSpDoesNotExist", ErrSpDoesNotExist),
    ("ErrSpDropFailed", ErrSpDropFailed),
    ("ErrSpStoreFailed", ErrSpStoreFailed),
    ("ErrSpLilabelMismatch", ErrSpLilabelMismatch),
    ("ErrSpLabelRedefine", ErrSpLabelRedefine),
    ("ErrSpLabelMismatch", ErrSpLabelMismatch),
    ("ErrSpUninitVar", ErrSpUninitVar),
    ("ErrSpBadselect", ErrSpBadselect),
    ("ErrSpBadreturn", ErrSpBadreturn),
    ("ErrSpBadstatement", ErrSpBadstatement),
    (
        "ErrUpdateLogDeprecatedIgnored",
        ErrUpdateLogDeprecatedIgnored,
    ),
    (
        "ErrUpdateLogDeprecatedTranslated",
        ErrUpdateLogDeprecatedTranslated,
    ),
    ("ErrQueryInterrupted", ErrQueryInterrupted),
    ("ErrSpWrongNoOfArgs", ErrSpWrongNoOfArgs),
    ("ErrSpCondMismatch", ErrSpCondMismatch),
    ("ErrSpNoreturn", ErrSpNoreturn),
    ("ErrSpNoreturnend", ErrSpNoreturnend),
    ("ErrSpBadCursorQuery", ErrSpBadCursorQuery),
    ("ErrSpBadCursorSelect", ErrSpBadCursorSelect),
    ("ErrSpCursorMismatch", ErrSpCursorMismatch),
    ("ErrSpCursorAlreadyOpen", ErrSpCursorAlreadyOpen),
    ("ErrSpCursorNotOpen", ErrSpCursorNotOpen),
    ("ErrSpUndeclaredVar", ErrSpUndeclaredVar),
    ("ErrSpWrongNoOfFetchArgs", ErrSpWrongNoOfFetchArgs),
    ("ErrSpFetchNoData", ErrSpFetchNoData),
    ("ErrSpDupParam", ErrSpDupParam),
    ("ErrSpDupVar", ErrSpDupVar),
    ("ErrSpDupCond", ErrSpDupCond),
    ("ErrSpDupCurs", ErrSpDupCurs),
    ("ErrSpCantAlter", ErrSpCantAlter),
    ("ErrSpSubselectNyi", ErrSpSubselectNyi),
    ("ErrStmtNotAllowedInSfOrTrg", ErrStmtNotAllowedInSfOrTrg),
    ("ErrSpVarcondAfterCurshndlr", ErrSpVarcondAfterCurshndlr),
    ("ErrSpCursorAfterHandler", ErrSpCursorAfterHandler),
    ("ErrSpCaseNotFound", ErrSpCaseNotFound),
    ("ErrFparserTooBigFile", ErrFparserTooBigFile),
    ("ErrFparserBadHeader", ErrFparserBadHeader),
    ("ErrFparserEOFInComment", ErrFparserEOFInComment),
    ("ErrFparserErrorInParameter", ErrFparserErrorInParameter),
    (
        "ErrFparserEOFInUnknownParameter",
        ErrFparserEOFInUnknownParameter,
    ),
    ("ErrViewNoExplain", ErrViewNoExplain),
    ("ErrFrmUnknownType", ErrFrmUnknownType),
    ("ErrWrongObject", ErrWrongObject),
    ("ErrNonupdateableColumn", ErrNonupdateableColumn),
    ("ErrViewSelectDerived", ErrViewSelectDerived),
    ("ErrViewSelectClause", ErrViewSelectClause),
    ("ErrViewSelectVariable", ErrViewSelectVariable),
    ("ErrViewSelectTmptable", ErrViewSelectTmptable),
    ("ErrViewWrongList", ErrViewWrongList),
    ("ErrWarnViewMerge", ErrWarnViewMerge),
    ("ErrWarnViewWithoutKey", ErrWarnViewWithoutKey),
    ("ErrViewInvalid", ErrViewInvalid),
    ("ErrSpNoDropSp", ErrSpNoDropSp),
    ("ErrSpGotoInHndlr", ErrSpGotoInHndlr),
    ("ErrTrgAlreadyExists", ErrTrgAlreadyExists),
    ("ErrTrgDoesNotExist", ErrTrgDoesNotExist),
    ("ErrTrgOnViewOrTempTable", ErrTrgOnViewOrTempTable),
    ("ErrTrgCantChangeRow", ErrTrgCantChangeRow),
    ("ErrTrgNoSuchRowInTrg", ErrTrgNoSuchRowInTrg),
    ("ErrNoDefaultForField", ErrNoDefaultForField),
    ("ErrDivisionByZero", ErrDivisionByZero),
    (
        "ErrTruncatedWrongValueForField",
        ErrTruncatedWrongValueForField,
    ),
    ("ErrIllegalValueForType", ErrIllegalValueForType),
    ("ErrViewNonupdCheck", ErrViewNonupdCheck),
    ("ErrViewCheckFailed", ErrViewCheckFailed),
    ("ErrProcaccessDenied", ErrProcaccessDenied),
    ("ErrRelayLogFail", ErrRelayLogFail),
    ("ErrPasswdLength", ErrPasswdLength),
    ("ErrUnknownTargetBinlog", ErrUnknownTargetBinlog),
    ("ErrIoErrLogIndexRead", ErrIoErrLogIndexRead),
    ("ErrBinlogPurgeProhibited", ErrBinlogPurgeProhibited),
    ("ErrFseekFail", ErrFseekFail),
    ("ErrBinlogPurgeFatalErr", ErrBinlogPurgeFatalErr),
    ("ErrLogInUse", ErrLogInUse),
    ("ErrLogPurgeUnknownErr", ErrLogPurgeUnknownErr),
    ("ErrRelayLogInit", ErrRelayLogInit),
    ("ErrNoBinaryLogging", ErrNoBinaryLogging),
    ("ErrReservedSyntax", ErrReservedSyntax),
    ("ErrWsasFailed", ErrWsasFailed),
    ("ErrDiffGroupsProc", ErrDiffGroupsProc),
    ("ErrNoGroupForProc", ErrNoGroupForProc),
    ("ErrOrderWithProc", ErrOrderWithProc),
    ("ErrLoggingProhibitChangingOf", ErrLoggingProhibitChangingOf),
    ("ErrNoFileMapping", ErrNoFileMapping),
    ("ErrWrongMagic", ErrWrongMagic),
    ("ErrPsManyParam", ErrPsManyParam),
    ("ErrKeyPart0", ErrKeyPart0),
    ("ErrViewChecksum", ErrViewChecksum),
    ("ErrViewMultiupdate", ErrViewMultiupdate),
    ("ErrViewNoInsertFieldList", ErrViewNoInsertFieldList),
    ("ErrViewDeleteMergeView", ErrViewDeleteMergeView),
    ("ErrCannotUser", ErrCannotUser),
    ("ErrXaerNota", ErrXaerNota),
    ("ErrXaerInval", ErrXaerInval),
    ("ErrXaerRmfail", ErrXaerRmfail),
    ("ErrXaerOutside", ErrXaerOutside),
    ("ErrXaerRmerr", ErrXaerRmerr),
    ("ErrXaRbrollback", ErrXaRbrollback),
    ("ErrNonexistingProcGrant", ErrNonexistingProcGrant),
    ("ErrProcAutoGrantFail", ErrProcAutoGrantFail),
    ("ErrProcAutoRevokeFail", ErrProcAutoRevokeFail),
    ("ErrDataTooLong", ErrDataTooLong),
    ("ErrSpBadSQLstate", ErrSpBadSQLstate),
    ("ErrStartup", ErrStartup),
    (
        "ErrLoadFromFixedSizeRowsToVar",
        ErrLoadFromFixedSizeRowsToVar,
    ),
    ("ErrCantCreateUserWithGrant", ErrCantCreateUserWithGrant),
    ("ErrWrongValueForType", ErrWrongValueForType),
    ("ErrTableDefChanged", ErrTableDefChanged),
    ("ErrSpDupHandler", ErrSpDupHandler),
    ("ErrSpNotVarArg", ErrSpNotVarArg),
    ("ErrSpNoRetset", ErrSpNoRetset),
    ("ErrCantCreateGeometryObject", ErrCantCreateGeometryObject),
    ("ErrFailedRoutineBreakBinlog", ErrFailedRoutineBreakBinlog),
    ("ErrBinlogUnsafeRoutine", ErrBinlogUnsafeRoutine),
    (
        "ErrBinlogCreateRoutineNeedSuper",
        ErrBinlogCreateRoutineNeedSuper,
    ),
    ("ErrExecStmtWithOpenCursor", ErrExecStmtWithOpenCursor),
    ("ErrStmtHasNoOpenCursor", ErrStmtHasNoOpenCursor),
    ("ErrCommitNotAllowedInSfOrTrg", ErrCommitNotAllowedInSfOrTrg),
    ("ErrNoDefaultForViewField", ErrNoDefaultForViewField),
    ("ErrSpNoRecursion", ErrSpNoRecursion),
    ("ErrTooBigScale", ErrTooBigScale),
    ("ErrTooBigPrecision", ErrTooBigPrecision),
    ("ErrMBiggerThanD", ErrMBiggerThanD),
    ("ErrWrongLockOfSystemTable", ErrWrongLockOfSystemTable),
    (
        "ErrConnectToForeignDataSource",
        ErrConnectToForeignDataSource,
    ),
    ("ErrQueryOnForeignDataSource", ErrQueryOnForeignDataSource),
    (
        "ErrForeignDataSourceDoesntExist",
        ErrForeignDataSourceDoesntExist,
    ),
    (
        "ErrForeignDataStringInvalidCantCreate",
        ErrForeignDataStringInvalidCantCreate,
    ),
    ("ErrForeignDataStringInvalid", ErrForeignDataStringInvalid),
    ("ErrCantCreateFederatedTable", ErrCantCreateFederatedTable),
    ("ErrTrgInWrongSchema", ErrTrgInWrongSchema),
    ("ErrStackOverrunNeedMore", ErrStackOverrunNeedMore),
    ("ErrTooLongBody", ErrTooLongBody),
    (
        "ErrWarnCantDropDefaultKeycache",
        ErrWarnCantDropDefaultKeycache,
    ),
    ("ErrTooBigDisplaywidth", ErrTooBigDisplaywidth),
    ("ErrXaerDupid", ErrXaerDupid),
    ("ErrDatetimeFunctionOverflow", ErrDatetimeFunctionOverflow),
    (
        "ErrCantUpdateUsedTableInSfOrTrg",
        ErrCantUpdateUsedTableInSfOrTrg,
    ),
    ("ErrViewPreventUpdate", ErrViewPreventUpdate),
    ("ErrPsNoRecursion", ErrPsNoRecursion),
    ("ErrSpCantSetAutocommit", ErrSpCantSetAutocommit),
    ("ErrMalformedDefiner", ErrMalformedDefiner),
    ("ErrViewFrmNoUser", ErrViewFrmNoUser),
    ("ErrViewOtherUser", ErrViewOtherUser),
    ("ErrNoSuchUser", ErrNoSuchUser),
    ("ErrForbidSchemaChange", ErrForbidSchemaChange),
    ("ErrRowIsReferenced2", ErrRowIsReferenced2),
    ("ErrNoReferencedRow2", ErrNoReferencedRow2),
    ("ErrSpBadVarShadow", ErrSpBadVarShadow),
    ("ErrTrgNoDefiner", ErrTrgNoDefiner),
    ("ErrOldFileFormat", ErrOldFileFormat),
    ("ErrSpRecursionLimit", ErrSpRecursionLimit),
    ("ErrSpProcTableCorrupt", ErrSpProcTableCorrupt),
    ("ErrSpWrongName", ErrSpWrongName),
    ("ErrTableNeedsUpgrade", ErrTableNeedsUpgrade),
    ("ErrSpNoAggregate", ErrSpNoAggregate),
    (
        "ErrMaxPreparedStmtCountReached",
        ErrMaxPreparedStmtCountReached,
    ),
    ("ErrViewRecursive", ErrViewRecursive),
    ("ErrNonGroupingFieldUsed", ErrNonGroupingFieldUsed),
    ("ErrTableCantHandleSpkeys", ErrTableCantHandleSpkeys),
    ("ErrNoTriggersOnSystemSchema", ErrNoTriggersOnSystemSchema),
    ("ErrRemovedSpaces", ErrRemovedSpaces),
    ("ErrAutoincReadFailed", ErrAutoincReadFailed),
    ("ErrUsername", ErrUsername),
    ("ErrHostname", ErrHostname),
    ("ErrWrongStringLength", ErrWrongStringLength),
    ("ErrNonInsertableTable", ErrNonInsertableTable),
    ("ErrAdminWrongMrgTable", ErrAdminWrongMrgTable),
    (
        "ErrTooHighLevelOfNestingForSelect",
        ErrTooHighLevelOfNestingForSelect,
    ),
    ("ErrNameBecomesEmpty", ErrNameBecomesEmpty),
    ("ErrAmbiguousFieldTerm", ErrAmbiguousFieldTerm),
    ("ErrForeignServerExists", ErrForeignServerExists),
    ("ErrForeignServerDoesntExist", ErrForeignServerDoesntExist),
    ("ErrIllegalHaCreateOption", ErrIllegalHaCreateOption),
    ("ErrPartitionRequiresValues", ErrPartitionRequiresValues),
    ("ErrPartitionWrongValues", ErrPartitionWrongValues),
    ("ErrPartitionMaxvalue", ErrPartitionMaxvalue),
    ("ErrPartitionSubpartition", ErrPartitionSubpartition),
    ("ErrPartitionSubpartMix", ErrPartitionSubpartMix),
    ("ErrPartitionWrongNoPart", ErrPartitionWrongNoPart),
    ("ErrPartitionWrongNoSubpart", ErrPartitionWrongNoSubpart),
    ("ErrWrongExprInPartitionFunc", ErrWrongExprInPartitionFunc),
    ("ErrNoConstExprInRangeOrList", ErrNoConstExprInRangeOrList),
    ("ErrFieldNotFoundPart", ErrFieldNotFoundPart),
    ("ErrListOfFieldsOnlyInHash", ErrListOfFieldsOnlyInHash),
    ("ErrInconsistentPartitionInfo", ErrInconsistentPartitionInfo),
    ("ErrPartitionFuncNotAllowed", ErrPartitionFuncNotAllowed),
    ("ErrPartitionsMustBeDefined", ErrPartitionsMustBeDefined),
    ("ErrRangeNotIncreasing", ErrRangeNotIncreasing),
    (
        "ErrInconsistentTypeOfFunctions",
        ErrInconsistentTypeOfFunctions,
    ),
    (
        "ErrMultipleDefConstInListPart",
        ErrMultipleDefConstInListPart,
    ),
    ("ErrPartitionEntry", ErrPartitionEntry),
    ("ErrMixHandler", ErrMixHandler),
    ("ErrPartitionNotDefined", ErrPartitionNotDefined),
    ("ErrTooManyPartitions", ErrTooManyPartitions),
    ("ErrSubpartition", ErrSubpartition),
    ("ErrCantCreateHandlerFile", ErrCantCreateHandlerFile),
    ("ErrBlobFieldInPartFunc", ErrBlobFieldInPartFunc),
    (
        "ErrUniqueKeyNeedAllFieldsInPf",
        ErrUniqueKeyNeedAllFieldsInPf,
    ),
    ("ErrNoParts", ErrNoParts),
    (
        "ErrPartitionMgmtOnNonpartitioned",
        ErrPartitionMgmtOnNonpartitioned,
    ),
    ("ErrForeignKeyOnPartitioned", ErrForeignKeyOnPartitioned),
    ("ErrDropPartitionNonExistent", ErrDropPartitionNonExistent),
    ("ErrDropLastPartition", ErrDropLastPartition),
    (
        "ErrCoalesceOnlyOnHashPartition",
        ErrCoalesceOnlyOnHashPartition,
    ),
    ("ErrReorgHashOnlyOnSameNo", ErrReorgHashOnlyOnSameNo),
    ("ErrReorgNoParam", ErrReorgNoParam),
    ("ErrOnlyOnRangeListPartition", ErrOnlyOnRangeListPartition),
    ("ErrAddPartitionSubpart", ErrAddPartitionSubpart),
    (
        "ErrAddPartitionNoNewPartition",
        ErrAddPartitionNoNewPartition,
    ),
    (
        "ErrCoalescePartitionNoPartition",
        ErrCoalescePartitionNoPartition,
    ),
    ("ErrReorgPartitionNotExist", ErrReorgPartitionNotExist),
    ("ErrSameNamePartition", ErrSameNamePartition),
    ("ErrNoBinlog", ErrNoBinlog),
    (
        "ErrConsecutiveReorgPartitions",
        ErrConsecutiveReorgPartitions,
    ),
    ("ErrReorgOutsideRange", ErrReorgOutsideRange),
    ("ErrPartitionFunctionFailure", ErrPartitionFunctionFailure),
    ("ErrPartState", ErrPartState),
    ("ErrLimitedPartRange", ErrLimitedPartRange),
    ("ErrPluginIsNotLoaded", ErrPluginIsNotLoaded),
    ("ErrWrongValue", ErrWrongValue),
    ("ErrNoPartitionForGivenValue", ErrNoPartitionForGivenValue),
    ("ErrFilegroupOptionOnlyOnce", ErrFilegroupOptionOnlyOnce),
    ("ErrCreateFilegroupFailed", ErrCreateFilegroupFailed),
    ("ErrDropFilegroupFailed", ErrDropFilegroupFailed),
    ("ErrTablespaceAutoExtend", ErrTablespaceAutoExtend),
    ("ErrWrongSizeNumber", ErrWrongSizeNumber),
    ("ErrSizeOverflow", ErrSizeOverflow),
    ("ErrAlterFilegroupFailed", ErrAlterFilegroupFailed),
    ("ErrBinlogRowLoggingFailed", ErrBinlogRowLoggingFailed),
    ("ErrEventAlreadyExists", ErrEventAlreadyExists),
    ("ErrEventStoreFailed", ErrEventStoreFailed),
    ("ErrEventDoesNotExist", ErrEventDoesNotExist),
    ("ErrEventCantAlter", ErrEventCantAlter),
    ("ErrEventDropFailed", ErrEventDropFailed),
    (
        "ErrEventIntervalNotPositiveOrTooBig",
        ErrEventIntervalNotPositiveOrTooBig,
    ),
    ("ErrEventEndsBeforeStarts", ErrEventEndsBeforeStarts),
    ("ErrEventExecTimeInThePast", ErrEventExecTimeInThePast),
    ("ErrEventOpenTableFailed", ErrEventOpenTableFailed),
    ("ErrEventNeitherMExprNorMAt", ErrEventNeitherMExprNorMAt),
    (
        "ErrObsoleteColCountDoesntMatchCorrupted",
        ErrObsoleteColCountDoesntMatchCorrupted,
    ),
    (
        "ErrObsoleteCannotLoadFromTable",
        ErrObsoleteCannotLoadFromTable,
    ),
    ("ErrEventCannotDelete", ErrEventCannotDelete),
    ("ErrEventCompile", ErrEventCompile),
    ("ErrEventSameName", ErrEventSameName),
    ("ErrEventDataTooLong", ErrEventDataTooLong),
    (
        "ErrDropIndexNeededInForeignKey",
        ErrDropIndexNeededInForeignKey,
    ),
    (
        "ErrWarnDeprecatedSyntaxWithVer",
        ErrWarnDeprecatedSyntaxWithVer,
    ),
    ("ErrCantWriteLockLogTable", ErrCantWriteLockLogTable),
    ("ErrCantLockLogTable", ErrCantLockLogTable),
    (
        "ErrForeignDuplicateKeyOldUnused",
        ErrForeignDuplicateKeyOldUnused,
    ),
    (
        "ErrColCountDoesntMatchPleaseUpdate",
        ErrColCountDoesntMatchPleaseUpdate,
    ),
    (
        "ErrTempTablePreventsSwitchOutOfRbr",
        ErrTempTablePreventsSwitchOutOfRbr,
    ),
    (
        "ErrStoredFunctionPreventsSwitchBinlogFormat",
        ErrStoredFunctionPreventsSwitchBinlogFormat,
    ),
    ("ErrNdbCantSwitchBinlogFormat", ErrNdbCantSwitchBinlogFormat),
    ("ErrPartitionNoTemporary", ErrPartitionNoTemporary),
    ("ErrPartitionConstDomain", ErrPartitionConstDomain),
    (
        "ErrPartitionFunctionIsNotAllowed",
        ErrPartitionFunctionIsNotAllowed,
    ),
    ("ErrDdlLog", ErrDdlLog),
    ("ErrNullInValuesLessThan", ErrNullInValuesLessThan),
    ("ErrWrongPartitionName", ErrWrongPartitionName),
    (
        "ErrCantChangeTxCharacteristics",
        ErrCantChangeTxCharacteristics,
    ),
    ("ErrDupEntryAutoincrementCase", ErrDupEntryAutoincrementCase),
    ("ErrEventModifyQueue", ErrEventModifyQueue),
    ("ErrEventSetVar", ErrEventSetVar),
    ("ErrPartitionMerge", ErrPartitionMerge),
    ("ErrCantActivateLog", ErrCantActivateLog),
    ("ErrRbrNotAvailable", ErrRbrNotAvailable),
    ("ErrBase64Decode", ErrBase64Decode),
    ("ErrEventRecursionForbidden", ErrEventRecursionForbidden),
    ("ErrEventsDB", ErrEventsDB),
    ("ErrOnlyIntegersAllowed", ErrOnlyIntegersAllowed),
    ("ErrUnsuportedLogEngine", ErrUnsuportedLogEngine),
    ("ErrBadLogStatement", ErrBadLogStatement),
    ("ErrCantRenameLogTable", ErrCantRenameLogTable),
    (
        "ErrWrongParamcountToNativeFct",
        ErrWrongParamcountToNativeFct,
    ),
    (
        "ErrWrongParametersToNativeFct",
        ErrWrongParametersToNativeFct,
    ),
    (
        "ErrWrongParametersToStoredFct",
        ErrWrongParametersToStoredFct,
    ),
    ("ErrNativeFctNameCollision", ErrNativeFctNameCollision),
    ("ErrDupEntryWithKeyName", ErrDupEntryWithKeyName),
    ("ErrBinlogPurgeEmFile", ErrBinlogPurgeEmFile),
    (
        "ErrEventCannotCreateInThePast",
        ErrEventCannotCreateInThePast,
    ),
    ("ErrEventCannotAlterInThePast", ErrEventCannotAlterInThePast),
    (
        "ErrNoPartitionForGivenValueSilent",
        ErrNoPartitionForGivenValueSilent,
    ),
    ("ErrBinlogUnsafeStatement", ErrBinlogUnsafeStatement),
    ("ErrBinlogLoggingImpossible", ErrBinlogLoggingImpossible),
    ("ErrViewNoCreationCtx", ErrViewNoCreationCtx),
    ("ErrViewInvalidCreationCtx", ErrViewInvalidCreationCtx),
    ("ErrSrInvalidCreationCtx", ErrSrInvalidCreationCtx),
    ("ErrTrgCorruptedFile", ErrTrgCorruptedFile),
    ("ErrTrgNoCreationCtx", ErrTrgNoCreationCtx),
    ("ErrTrgInvalidCreationCtx", ErrTrgInvalidCreationCtx),
    ("ErrEventInvalidCreationCtx", ErrEventInvalidCreationCtx),
    ("ErrTrgCantOpenTable", ErrTrgCantOpenTable),
    ("ErrCantCreateSroutine", ErrCantCreateSroutine),
    (
        "ErrNoFormatDescriptionEventBeforeBinlogStatement",
        ErrNoFormatDescriptionEventBeforeBinlogStatement,
    ),
    ("ErrLoadDataInvalidColumn", ErrLoadDataInvalidColumn),
    ("ErrLogPurgeNoFile", ErrLogPurgeNoFile),
    ("ErrXaRbtimeout", ErrXaRbtimeout),
    ("ErrXaRbdeadlock", ErrXaRbdeadlock),
    ("ErrNeedReprepare", ErrNeedReprepare),
    ("ErrDelayedNotSupported", ErrDelayedNotSupported),
    ("WarnOptionIgnored", WarnOptionIgnored),
    ("WarnPluginDeleteBuiltin", WarnPluginDeleteBuiltin),
    ("WarnPluginBusy", WarnPluginBusy),
    ("ErrVariableIsReadonly", ErrVariableIsReadonly),
    (
        "ErrWarnEngineTransactionRollback",
        ErrWarnEngineTransactionRollback,
    ),
    ("ErrNdbReplicationSchema", ErrNdbReplicationSchema),
    ("ErrConflictFnParse", ErrConflictFnParse),
    ("ErrExceptionsWrite", ErrExceptionsWrite),
    ("ErrTooLongTableComment", ErrTooLongTableComment),
    ("ErrTooLongFieldComment", ErrTooLongFieldComment),
    (
        "ErrFuncInexistentNameCollision",
        ErrFuncInexistentNameCollision,
    ),
    ("ErrDatabaseName", ErrDatabaseName),
    ("ErrTableName", ErrTableName),
    ("ErrPartitionName", ErrPartitionName),
    ("ErrSubpartitionName", ErrSubpartitionName),
    ("ErrTemporaryName", ErrTemporaryName),
    ("ErrRenamedName", ErrRenamedName),
    ("ErrTooManyConcurrentTrxs", ErrTooManyConcurrentTrxs),
    (
        "WarnNonASCIISeparatorNotImplemented",
        WarnNonASCIISeparatorNotImplemented,
    ),
    ("ErrDebugSyncTimeout", ErrDebugSyncTimeout),
    ("ErrDebugSyncHitLimit", ErrDebugSyncHitLimit),
    ("ErrDupSignalSet", ErrDupSignalSet),
    ("ErrSignalWarn", ErrSignalWarn),
    ("ErrSignalNotFound", ErrSignalNotFound),
    ("ErrSignalException", ErrSignalException),
    (
        "ErrResignalWithoutActiveHandler",
        ErrResignalWithoutActiveHandler,
    ),
    ("ErrSignalBadConditionType", ErrSignalBadConditionType),
    ("WarnCondItemTruncated", WarnCondItemTruncated),
    ("ErrCondItemTooLong", ErrCondItemTooLong),
    ("ErrUnknownLocale", ErrUnknownLocale),
    ("ErrQueryCacheDisabled", ErrQueryCacheDisabled),
    ("ErrSameNamePartitionField", ErrSameNamePartitionField),
    ("ErrPartitionColumnList", ErrPartitionColumnList),
    ("ErrWrongTypeColumnValue", ErrWrongTypeColumnValue),
    (
        "ErrTooManyPartitionFuncFields",
        ErrTooManyPartitionFuncFields,
    ),
    ("ErrMaxvalueInValuesIn", ErrMaxvalueInValuesIn),
    ("ErrTooManyValues", ErrTooManyValues),
    ("ErrRowSinglePartitionField", ErrRowSinglePartitionField),
    (
        "ErrFieldTypeNotAllowedAsPartitionField",
        ErrFieldTypeNotAllowedAsPartitionField,
    ),
    ("ErrPartitionFieldsTooLong", ErrPartitionFieldsTooLong),
    (
        "ErrBinlogRowEngineAndStmtEngine",
        ErrBinlogRowEngineAndStmtEngine,
    ),
    (
        "ErrBinlogRowModeAndStmtEngine",
        ErrBinlogRowModeAndStmtEngine,
    ),
    ("ErrBinlogUnsafeAndStmtEngine", ErrBinlogUnsafeAndStmtEngine),
    (
        "ErrBinlogRowInjectionAndStmtEngine",
        ErrBinlogRowInjectionAndStmtEngine,
    ),
    (
        "ErrBinlogStmtModeAndRowEngine",
        ErrBinlogStmtModeAndRowEngine,
    ),
    (
        "ErrBinlogRowInjectionAndStmtMode",
        ErrBinlogRowInjectionAndStmtMode,
    ),
    (
        "ErrBinlogMultipleEnginesAndSelfLoggingEngine",
        ErrBinlogMultipleEnginesAndSelfLoggingEngine,
    ),
    ("ErrBinlogUnsafeLimit", ErrBinlogUnsafeLimit),
    ("ErrBinlogUnsafeInsertDelayed", ErrBinlogUnsafeInsertDelayed),
    (
        "ErrBinlogUnsafeAutoincColumns",
        ErrBinlogUnsafeAutoincColumns,
    ),
    (
        "ErrBinlogUnsafeSystemFunction",
        ErrBinlogUnsafeSystemFunction,
    ),
    (
        "ErrBinlogUnsafeNontransAfterTrans",
        ErrBinlogUnsafeNontransAfterTrans,
    ),
    ("ErrMessageAndStatement", ErrMessageAndStatement),
    (
        "ErrInsideTransactionPreventsSwitchBinlogFormat",
        ErrInsideTransactionPreventsSwitchBinlogFormat,
    ),
    ("ErrPathLength", ErrPathLength),
    (
        "ErrWarnDeprecatedSyntaxNoReplacement",
        ErrWarnDeprecatedSyntaxNoReplacement,
    ),
    ("ErrWrongNativeTableStructure", ErrWrongNativeTableStructure),
    ("ErrWrongPerfSchemaUsage", ErrWrongPerfSchemaUsage),
    ("ErrWarnISSkippedTable", ErrWarnISSkippedTable),
    (
        "ErrInsideTransactionPreventsSwitchBinlogDirect",
        ErrInsideTransactionPreventsSwitchBinlogDirect,
    ),
    (
        "ErrStoredFunctionPreventsSwitchBinlogDirect",
        ErrStoredFunctionPreventsSwitchBinlogDirect,
    ),
    ("ErrSpatialMustHaveGeomCol", ErrSpatialMustHaveGeomCol),
    ("ErrTooLongIndexComment", ErrTooLongIndexComment),
    ("ErrLockAborted", ErrLockAborted),
    ("ErrDataOutOfRange", ErrDataOutOfRange),
    ("ErrWrongSpvarTypeInLimit", ErrWrongSpvarTypeInLimit),
    (
        "ErrBinlogUnsafeMultipleEnginesAndSelfLoggingEngine",
        ErrBinlogUnsafeMultipleEnginesAndSelfLoggingEngine,
    ),
    (
        "ErrBinlogUnsafeMixedStatement",
        ErrBinlogUnsafeMixedStatement,
    ),
    (
        "ErrInsideTransactionPreventsSwitchSQLLogBin",
        ErrInsideTransactionPreventsSwitchSQLLogBin,
    ),
    (
        "ErrStoredFunctionPreventsSwitchSQLLogBin",
        ErrStoredFunctionPreventsSwitchSQLLogBin,
    ),
    ("ErrFailedReadFromParFile", ErrFailedReadFromParFile),
    ("ErrValuesIsNotIntType", ErrValuesIsNotIntType),
    ("ErrAccessDeniedNoPassword", ErrAccessDeniedNoPassword),
    ("ErrSetPasswordAuthPlugin", ErrSetPasswordAuthPlugin),
    ("ErrGrantPluginUserExists", ErrGrantPluginUserExists),
    ("ErrTruncateIllegalForeignKey", ErrTruncateIllegalForeignKey),
    ("ErrPluginIsPermanent", ErrPluginIsPermanent),
    ("ErrStmtCacheFull", ErrStmtCacheFull),
    ("ErrMultiUpdateKeyConflict", ErrMultiUpdateKeyConflict),
    ("ErrTableNeedsRebuild", ErrTableNeedsRebuild),
    ("WarnOptionBelowLimit", WarnOptionBelowLimit),
    ("ErrIndexColumnTooLong", ErrIndexColumnTooLong),
    ("ErrErrorInTriggerBody", ErrErrorInTriggerBody),
    ("ErrErrorInUnknownTriggerBody", ErrErrorInUnknownTriggerBody),
    ("ErrIndexCorrupt", ErrIndexCorrupt),
    ("ErrUndoRecordTooBig", ErrUndoRecordTooBig),
    ("ErrPluginNoUninstall", ErrPluginNoUninstall),
    ("ErrPluginNoInstall", ErrPluginNoInstall),
    ("ErrBinlogUnsafeInsertTwoKeys", ErrBinlogUnsafeInsertTwoKeys),
    ("ErrTableInFkCheck", ErrTableInFkCheck),
    ("ErrUnsupportedEngine", ErrUnsupportedEngine),
    (
        "ErrBinlogUnsafeAutoincNotFirst",
        ErrBinlogUnsafeAutoincNotFirst,
    ),
    ("ErrCannotLoadFromTableV2", ErrCannotLoadFromTableV2),
    (
        "ErrOnlyFdAndRbrEventsAllowedInBinlogStatement",
        ErrOnlyFdAndRbrEventsAllowedInBinlogStatement,
    ),
    (
        "ErrPartitionExchangeDifferentOption",
        ErrPartitionExchangeDifferentOption,
    ),
    (
        "ErrPartitionExchangePartTable",
        ErrPartitionExchangePartTable,
    ),
    (
        "ErrPartitionExchangeTempTable",
        ErrPartitionExchangeTempTable,
    ),
    (
        "ErrPartitionInsteadOfSubpartition",
        ErrPartitionInsteadOfSubpartition,
    ),
    ("ErrUnknownPartition", ErrUnknownPartition),
    ("ErrTablesDifferentMetadata", ErrTablesDifferentMetadata),
    ("ErrRowDoesNotMatchPartition", ErrRowDoesNotMatchPartition),
    (
        "ErrBinlogCacheSizeGreaterThanMax",
        ErrBinlogCacheSizeGreaterThanMax,
    ),
    ("ErrWarnIndexNotApplicable", ErrWarnIndexNotApplicable),
    (
        "ErrPartitionExchangeForeignKey",
        ErrPartitionExchangeForeignKey,
    ),
    ("ErrNoSuchKeyValue", ErrNoSuchKeyValue),
    ("ErrRplInfoDataTooLong", ErrRplInfoDataTooLong),
    (
        "ErrNetworkReadEventChecksumFailure",
        ErrNetworkReadEventChecksumFailure,
    ),
    (
        "ErrBinlogReadEventChecksumFailure",
        ErrBinlogReadEventChecksumFailure,
    ),
    (
        "ErrBinlogStmtCacheSizeGreaterThanMax",
        ErrBinlogStmtCacheSizeGreaterThanMax,
    ),
    (
        "ErrCantUpdateTableInCreateTableSelect",
        ErrCantUpdateTableInCreateTableSelect,
    ),
    (
        "ErrPartitionClauseOnNonpartitioned",
        ErrPartitionClauseOnNonpartitioned,
    ),
    (
        "ErrRowDoesNotMatchGivenPartitionSet",
        ErrRowDoesNotMatchGivenPartitionSet,
    ),
    ("ErrNoSuchPartitionunused", ErrNoSuchPartitionunused),
    (
        "ErrChangeRplInfoRepositoryFailure",
        ErrChangeRplInfoRepositoryFailure,
    ),
    (
        "ErrWarningNotCompleteRollbackWithCreatedTempTable",
        ErrWarningNotCompleteRollbackWithCreatedTempTable,
    ),
    (
        "ErrWarningNotCompleteRollbackWithDroppedTempTable",
        ErrWarningNotCompleteRollbackWithDroppedTempTable,
    ),
    ("ErrMtsUpdatedDBsGreaterMax", ErrMtsUpdatedDBsGreaterMax),
    ("ErrMtsCantParallel", ErrMtsCantParallel),
    ("ErrMtsInconsistentData", ErrMtsInconsistentData),
    (
        "ErrFulltextNotSupportedWithPartitioning",
        ErrFulltextNotSupportedWithPartitioning,
    ),
    ("ErrDaInvalidConditionNumber", ErrDaInvalidConditionNumber),
    ("ErrInsecurePlainText", ErrInsecurePlainText),
    (
        "ErrForeignDuplicateKeyWithChildInfo",
        ErrForeignDuplicateKeyWithChildInfo,
    ),
    (
        "ErrForeignDuplicateKeyWithoutChildInfo",
        ErrForeignDuplicateKeyWithoutChildInfo,
    ),
    ("ErrTableHasNoFt", ErrTableHasNoFt),
    (
        "ErrVariableNotSettableInSfOrTrigger",
        ErrVariableNotSettableInSfOrTrigger,
    ),
    (
        "ErrVariableNotSettableInTransaction",
        ErrVariableNotSettableInTransaction,
    ),
    (
        "ErrGtidNextIsNotInGtidNextList",
        ErrGtidNextIsNotInGtidNextList,
    ),
    (
        "ErrCantChangeGtidNextInTransactionWhenGtidNextListIsNull",
        ErrCantChangeGtidNextInTransactionWhenGtidNextListIsNull,
    ),
    (
        "ErrSetStatementCannotInvokeFunction",
        ErrSetStatementCannotInvokeFunction,
    ),
    (
        "ErrGtidNextCantBeAutomaticIfGtidNextListIsNonNull",
        ErrGtidNextCantBeAutomaticIfGtidNextListIsNonNull,
    ),
    ("ErrSkippingLoggedTransaction", ErrSkippingLoggedTransaction),
    (
        "ErrMalformedGtidSetSpecification",
        ErrMalformedGtidSetSpecification,
    ),
    ("ErrMalformedGtidSetEncoding", ErrMalformedGtidSetEncoding),
    (
        "ErrMalformedGtidSpecification",
        ErrMalformedGtidSpecification,
    ),
    ("ErrGnoExhausted", ErrGnoExhausted),
    (
        "ErrCantDoImplicitCommitInTrxWhenGtidNextIsSet",
        ErrCantDoImplicitCommitInTrxWhenGtidNextIsSet,
    ),
    (
        "ErrGtidMode2Or3RequiresEnforceGtidConsistencyOn",
        ErrGtidMode2Or3RequiresEnforceGtidConsistencyOn,
    ),
    (
        "ErrCantSetGtidNextToGtidWhenGtidModeIsOff",
        ErrCantSetGtidNextToGtidWhenGtidModeIsOff,
    ),
    (
        "ErrCantSetGtidNextToAnonymousWhenGtidModeIsOn",
        ErrCantSetGtidNextToAnonymousWhenGtidModeIsOn,
    ),
    (
        "ErrCantSetGtidNextListToNonNullWhenGtidModeIsOff",
        ErrCantSetGtidNextListToNonNullWhenGtidModeIsOff,
    ),
    (
        "ErrFoundGtidEventWhenGtidModeIsOff",
        ErrFoundGtidEventWhenGtidModeIsOff,
    ),
    (
        "ErrGtidUnsafeNonTransactionalTable",
        ErrGtidUnsafeNonTransactionalTable,
    ),
    ("ErrGtidUnsafeCreateSelect", ErrGtidUnsafeCreateSelect),
    (
        "ErrGtidUnsafeCreateDropTemporaryTableInTransaction",
        ErrGtidUnsafeCreateDropTemporaryTableInTransaction,
    ),
    (
        "ErrGtidModeCanOnlyChangeOneStepAtATime",
        ErrGtidModeCanOnlyChangeOneStepAtATime,
    ),
    (
        "ErrCantSetGtidNextWhenOwningGtid",
        ErrCantSetGtidNextWhenOwningGtid,
    ),
    ("ErrUnknownExplainFormat", ErrUnknownExplainFormat),
    (
        "ErrCantExecuteInReadOnlyTransaction",
        ErrCantExecuteInReadOnlyTransaction,
    ),
    (
        "ErrTooLongTablePartitionComment",
        ErrTooLongTablePartitionComment,
    ),
    ("ErrInnodbFtLimit", ErrInnodbFtLimit),
    ("ErrInnodbNoFtTempTable", ErrInnodbNoFtTempTable),
    ("ErrInnodbFtWrongDocidColumn", ErrInnodbFtWrongDocidColumn),
    ("ErrInnodbFtWrongDocidIndex", ErrInnodbFtWrongDocidIndex),
    ("ErrInnodbOnlineLogTooBig", ErrInnodbOnlineLogTooBig),
    ("ErrUnknownAlterAlgorithm", ErrUnknownAlterAlgorithm),
    ("ErrUnknownAlterLock", ErrUnknownAlterLock),
    ("ErrMtsResetWorkers", ErrMtsResetWorkers),
    (
        "ErrColCountDoesntMatchCorruptedV2",
        ErrColCountDoesntMatchCorruptedV2,
    ),
    ("ErrDiscardFkChecksRunning", ErrDiscardFkChecksRunning),
    ("ErrTableSchemaMismatch", ErrTableSchemaMismatch),
    ("ErrTableInSystemTablespace", ErrTableInSystemTablespace),
    ("ErrIoRead", ErrIoRead),
    ("ErrIoWrite", ErrIoWrite),
    ("ErrTablespaceMissing", ErrTablespaceMissing),
    ("ErrTablespaceExists", ErrTablespaceExists),
    ("ErrTablespaceDiscarded", ErrTablespaceDiscarded),
    ("ErrInternal", ErrInternal),
    ("ErrInnodbImport", ErrInnodbImport),
    ("ErrInnodbIndexCorrupt", ErrInnodbIndexCorrupt),
    ("ErrInvalidYearColumnLength", ErrInvalidYearColumnLength),
    ("ErrNotValidPassword", ErrNotValidPassword),
    ("ErrMustChangePassword", ErrMustChangePassword),
    ("ErrFkNoIndexChild", ErrFkNoIndexChild),
    ("ErrForeignKeyNoIndexInParent", ErrForeignKeyNoIndexInParent),
    ("ErrFkFailAddSystem", ErrFkFailAddSystem),
    (
        "ErrForeignKeyCannotOpenParent",
        ErrForeignKeyCannotOpenParent,
    ),
    ("ErrFkIncorrectOption", ErrFkIncorrectOption),
    ("ErrFkDupName", ErrFkDupName),
    ("ErrPasswordFormat", ErrPasswordFormat),
    ("ErrFkColumnCannotDrop", ErrFkColumnCannotDrop),
    ("ErrFkColumnCannotDropChild", ErrFkColumnCannotDropChild),
    ("ErrForeignKeyColumnNotNull", ErrForeignKeyColumnNotNull),
    ("ErrDupIndex", ErrDupIndex),
    (
        "ErrForeignKeyColumnCannotChange",
        ErrForeignKeyColumnCannotChange,
    ),
    (
        "ErrForeignKeyColumnCannotChangeChild",
        ErrForeignKeyColumnCannotChangeChild,
    ),
    ("ErrFkCannotDeleteParent", ErrFkCannotDeleteParent),
    ("ErrMalformedPacket", ErrMalformedPacket),
    ("ErrReadOnlyMode", ErrReadOnlyMode),
    ("ErrVariableNotSettableInSp", ErrVariableNotSettableInSp),
    (
        "ErrCantSetGtidPurgedWhenGtidModeIsOff",
        ErrCantSetGtidPurgedWhenGtidModeIsOff,
    ),
    (
        "ErrCantSetGtidPurgedWhenGtidExecutedIsNotEmpty",
        ErrCantSetGtidPurgedWhenGtidExecutedIsNotEmpty,
    ),
    (
        "ErrCantSetGtidPurgedWhenOwnedGtidsIsNotEmpty",
        ErrCantSetGtidPurgedWhenOwnedGtidsIsNotEmpty,
    ),
    ("ErrGtidPurgedWasChanged", ErrGtidPurgedWasChanged),
    ("ErrGtidExecutedWasChanged", ErrGtidExecutedWasChanged),
    (
        "ErrBinlogStmtModeAndNoReplTables",
        ErrBinlogStmtModeAndNoReplTables,
    ),
    (
        "ErrAlterOperationNotSupported",
        ErrAlterOperationNotSupported,
    ),
    (
        "ErrAlterOperationNotSupportedReason",
        ErrAlterOperationNotSupportedReason,
    ),
    (
        "ErrAlterOperationNotSupportedReasonCopy",
        ErrAlterOperationNotSupportedReasonCopy,
    ),
    (
        "ErrAlterOperationNotSupportedReasonPartition",
        ErrAlterOperationNotSupportedReasonPartition,
    ),
    (
        "ErrAlterOperationNotSupportedReasonFkRename",
        ErrAlterOperationNotSupportedReasonFkRename,
    ),
    (
        "ErrAlterOperationNotSupportedReasonColumnType",
        ErrAlterOperationNotSupportedReasonColumnType,
    ),
    (
        "ErrAlterOperationNotSupportedReasonFkCheck",
        ErrAlterOperationNotSupportedReasonFkCheck,
    ),
    (
        "ErrAlterOperationNotSupportedReasonIgnore",
        ErrAlterOperationNotSupportedReasonIgnore,
    ),
    (
        "ErrAlterOperationNotSupportedReasonNopk",
        ErrAlterOperationNotSupportedReasonNopk,
    ),
    (
        "ErrAlterOperationNotSupportedReasonAutoinc",
        ErrAlterOperationNotSupportedReasonAutoinc,
    ),
    (
        "ErrAlterOperationNotSupportedReasonHiddenFts",
        ErrAlterOperationNotSupportedReasonHiddenFts,
    ),
    (
        "ErrAlterOperationNotSupportedReasonChangeFts",
        ErrAlterOperationNotSupportedReasonChangeFts,
    ),
    (
        "ErrAlterOperationNotSupportedReasonFts",
        ErrAlterOperationNotSupportedReasonFts,
    ),
    ("ErrDupUnknownInIndex", ErrDupUnknownInIndex),
    ("ErrIdentCausesTooLongPath", ErrIdentCausesTooLongPath),
    (
        "ErrAlterOperationNotSupportedReasonNotNull",
        ErrAlterOperationNotSupportedReasonNotNull,
    ),
    ("ErrMustChangePasswordLogin", ErrMustChangePasswordLogin),
    ("ErrRowInWrongPartition", ErrRowInWrongPartition),
    ("ErrErrorLast", ErrErrorLast),
    (
        "ErrForeignKeyCascadeDepthExceeded",
        ErrForeignKeyCascadeDepthExceeded,
    ),
    ("ErrInvalidFieldSize", ErrInvalidFieldSize),
    (
        "ErrPasswordExpireAnonymousUser",
        ErrPasswordExpireAnonymousUser,
    ),
    (
        "ErrInvalidArgumentForLogarithm",
        ErrInvalidArgumentForLogarithm,
    ),
    ("ErrMaxExecTimeExceeded", ErrMaxExecTimeExceeded),
    ("ErrAggregateOrderNonAggQuery", ErrAggregateOrderNonAggQuery),
    ("ErrUserLockWrongName", ErrUserLockWrongName),
    ("ErrUserLockDeadlock", ErrUserLockDeadlock),
    ("ErrIncorrectType", ErrIncorrectType),
    ("ErrFieldInOrderNotSelect", ErrFieldInOrderNotSelect),
    ("ErrAggregateInOrderNotSelect", ErrAggregateInOrderNotSelect),
    ("ErrInvalidJSONData", ErrInvalidJSONData),
    (
        "ErrGeneratedColumnFunctionIsNotAllowed",
        ErrGeneratedColumnFunctionIsNotAllowed,
    ),
    (
        "ErrUnsupportedAlterInplaceOnVirtualColumn",
        ErrUnsupportedAlterInplaceOnVirtualColumn,
    ),
    (
        "ErrWrongFKOptionForGeneratedColumn",
        ErrWrongFKOptionForGeneratedColumn,
    ),
    ("ErrBadGeneratedColumn", ErrBadGeneratedColumn),
    (
        "ErrUnsupportedOnGeneratedColumn",
        ErrUnsupportedOnGeneratedColumn,
    ),
    ("ErrGeneratedColumnNonPrior", ErrGeneratedColumnNonPrior),
    (
        "ErrDependentByGeneratedColumn",
        ErrDependentByGeneratedColumn,
    ),
    ("ErrGeneratedColumnRefAutoInc", ErrGeneratedColumnRefAutoInc),
    ("ErrAccountHasBeenLocked", ErrAccountHasBeenLocked),
    ("ErrWarnConflictingHint", ErrWarnConflictingHint),
    ("ErrUnresolvedHintName", ErrUnresolvedHintName),
    ("ErrInvalidJSONText", ErrInvalidJSONText),
    ("ErrInvalidJSONTextInParam", ErrInvalidJSONTextInParam),
    ("ErrInvalidJSONPath", ErrInvalidJSONPath),
    ("ErrInvalidJSONCharset", ErrInvalidJSONCharset),
    ("ErrInvalidTypeForJSON", ErrInvalidTypeForJSON),
    (
        "ErrInvalidJSONPathMultipleSelection",
        ErrInvalidJSONPathMultipleSelection,
    ),
    (
        "ErrInvalidJSONContainsPathType",
        ErrInvalidJSONContainsPathType,
    ),
    ("ErrJSONUsedAsKey", ErrJSONUsedAsKey),
    ("ErrJSONVacuousPath", ErrJSONVacuousPath),
    ("ErrJSONBadOneOrAllArg", ErrJSONBadOneOrAllArg),
    ("ErrJSONDocumentTooDeep", ErrJSONDocumentTooDeep),
    ("ErrJSONDocumentNULLKey", ErrJSONDocumentNULLKey),
    ("ErrSecureTransportRequired", ErrSecureTransportRequired),
    ("ErrBadUser", ErrBadUser),
    ("ErrUserAlreadyExists", ErrUserAlreadyExists),
    ("ErrInvalidJSONPathArrayCell", ErrInvalidJSONPathArrayCell),
    ("ErrInvalidEncryptionOption", ErrInvalidEncryptionOption),
    ("ErrTooLongValueForType", ErrTooLongValueForType),
    ("ErrPKIndexCantBeInvisible", ErrPKIndexCantBeInvisible),
    ("ErrGrantRole", ErrGrantRole),
    ("ErrRoleNotGranted", ErrRoleNotGranted),
    (
        "ErrLockAcquireFailAndNoWaitSet",
        ErrLockAcquireFailAndNoWaitSet,
    ),
    ("ErrCTERecursiveRequiresUnion", ErrCTERecursiveRequiresUnion),
    (
        "ErrCTERecursiveRequiresNonRecursiveFirst",
        ErrCTERecursiveRequiresNonRecursiveFirst,
    ),
    (
        "ErrCTERecursiveForbidsAggregation",
        ErrCTERecursiveForbidsAggregation,
    ),
    (
        "ErrCTERecursiveForbiddenJoinOrder",
        ErrCTERecursiveForbiddenJoinOrder,
    ),
    (
        "ErrInvalidRequiresSingleReference",
        ErrInvalidRequiresSingleReference,
    ),
    ("ErrWindowNoSuchWindow", ErrWindowNoSuchWindow),
    (
        "ErrWindowCircularityInWindowGraph",
        ErrWindowCircularityInWindowGraph,
    ),
    ("ErrWindowNoChildPartitioning", ErrWindowNoChildPartitioning),
    ("ErrWindowNoInherentFrame", ErrWindowNoInherentFrame),
    ("ErrWindowNoRedefineOrderBy", ErrWindowNoRedefineOrderBy),
    ("ErrWindowFrameStartIllegal", ErrWindowFrameStartIllegal),
    ("ErrWindowFrameEndIllegal", ErrWindowFrameEndIllegal),
    ("ErrWindowFrameIllegal", ErrWindowFrameIllegal),
    ("ErrWindowRangeFrameOrderType", ErrWindowRangeFrameOrderType),
    (
        "ErrWindowRangeFrameTemporalType",
        ErrWindowRangeFrameTemporalType,
    ),
    (
        "ErrWindowRangeFrameNumericType",
        ErrWindowRangeFrameNumericType,
    ),
    (
        "ErrWindowRangeBoundNotConstant",
        ErrWindowRangeBoundNotConstant,
    ),
    ("ErrWindowDuplicateName", ErrWindowDuplicateName),
    ("ErrWindowIllegalOrderBy", ErrWindowIllegalOrderBy),
    (
        "ErrWindowInvalidWindowFuncUse",
        ErrWindowInvalidWindowFuncUse,
    ),
    (
        "ErrWindowInvalidWindowFuncAliasUse",
        ErrWindowInvalidWindowFuncAliasUse,
    ),
    (
        "ErrWindowNestedWindowFuncUseInWindowSpec",
        ErrWindowNestedWindowFuncUseInWindowSpec,
    ),
    ("ErrWindowRowsIntervalUse", ErrWindowRowsIntervalUse),
    ("ErrWindowNoGroupOrderUnused", ErrWindowNoGroupOrderUnused),
    ("ErrWindowExplainJSON", ErrWindowExplainJSON),
    (
        "ErrWindowFunctionIgnoresFrame",
        ErrWindowFunctionIgnoresFrame,
    ),
    ("ErrInvalidNumberOfArgs", ErrInvalidNumberOfArgs),
    ("ErrFieldInGroupingNotGroupBy", ErrFieldInGroupingNotGroupBy),
    ("ErrIllegalPrivilegeLevel", ErrIllegalPrivilegeLevel),
    ("ErrCTEMaxRecursionDepth", ErrCTEMaxRecursionDepth),
    ("ErrNotHintUpdatable", ErrNotHintUpdatable),
    ("ErrExistsInHistoryPassword", ErrExistsInHistoryPassword),
    (
        "ErrInvalidDefaultUTF8MB4Collation",
        ErrInvalidDefaultUTF8MB4Collation,
    ),
    (
        "ErrForeignKeyCannotDropParent",
        ErrForeignKeyCannotDropParent,
    ),
    (
        "ErrForeignKeyCannotUseVirtualColumn",
        ErrForeignKeyCannotUseVirtualColumn,
    ),
    (
        "ErrForeignKeyNoColumnInParent",
        ErrForeignKeyNoColumnInParent,
    ),
    (
        "ErrDataTruncatedFunctionalIndex",
        ErrDataTruncatedFunctionalIndex,
    ),
    (
        "ErrDataOutOfRangeFunctionalIndex",
        ErrDataOutOfRangeFunctionalIndex,
    ),
    (
        "ErrFunctionalIndexOnJSONOrGeometryFunction",
        ErrFunctionalIndexOnJSONOrGeometryFunction,
    ),
    (
        "ErrFunctionalIndexRefAutoIncrement",
        ErrFunctionalIndexRefAutoIncrement,
    ),
    (
        "ErrCannotDropColumnFunctionalIndex",
        ErrCannotDropColumnFunctionalIndex,
    ),
    ("ErrFunctionalIndexPrimaryKey", ErrFunctionalIndexPrimaryKey),
    ("ErrFunctionalIndexOnBlob", ErrFunctionalIndexOnBlob),
    (
        "ErrFunctionalIndexFunctionIsNotAllowed",
        ErrFunctionalIndexFunctionIsNotAllowed,
    ),
    ("ErrFulltextFunctionalIndex", ErrFulltextFunctionalIndex),
    ("ErrSpatialFunctionalIndex", ErrSpatialFunctionalIndex),
    (
        "ErrWrongKeyColumnFunctionalIndex",
        ErrWrongKeyColumnFunctionalIndex,
    ),
    ("ErrFunctionalIndexOnField", ErrFunctionalIndexOnField),
    (
        "ErrGeneratedColumnRowValueIsNotAllowed",
        ErrGeneratedColumnRowValueIsNotAllowed,
    ),
    (
        "ErrDefValGeneratedNamedFunctionIsNotAllowed",
        ErrDefValGeneratedNamedFunctionIsNotAllowed,
    ),
    ("ErrFKIncompatibleColumns", ErrFKIncompatibleColumns),
    (
        "ErrFunctionalIndexRowValueIsNotAllowed",
        ErrFunctionalIndexRowValueIsNotAllowed,
    ),
    ("ErrInvalidLateralJoin", ErrInvalidLateralJoin),
    (
        "ErrNonBooleanExprForCheckConstraint",
        ErrNonBooleanExprForCheckConstraint,
    ),
    (
        "ErrColumnCheckConstraintReferencesOtherColumn",
        ErrColumnCheckConstraintReferencesOtherColumn,
    ),
    (
        "ErrCheckConstraintNamedFunctionIsNotAllowed",
        ErrCheckConstraintNamedFunctionIsNotAllowed,
    ),
    (
        "ErrCheckConstraintFunctionIsNotAllowed",
        ErrCheckConstraintFunctionIsNotAllowed,
    ),
    ("ErrCheckConstraintVariables", ErrCheckConstraintVariables),
    (
        "ErrCheckConstraintRefersAutoIncrementColumn",
        ErrCheckConstraintRefersAutoIncrementColumn,
    ),
    ("ErrCheckConstraintViolated", ErrCheckConstraintViolated),
    (
        "ErrTableCheckConstraintReferUnknown",
        ErrTableCheckConstraintReferUnknown,
    ),
    ("ErrCheckConstraintDupName", ErrCheckConstraintDupName),
    (
        "ErrCheckConstraintClauseUsingFKReferActionColumn",
        ErrCheckConstraintClauseUsingFKReferActionColumn,
    ),
    (
        "ErrDependentByFunctionalIndex",
        ErrDependentByFunctionalIndex,
    ),
    ("ErrInvalidJSONType", ErrInvalidJSONType),
    ("ErrCannotConvertString", ErrCannotConvertString),
    (
        "ErrDependentByPartitionFunctional",
        ErrDependentByPartitionFunctional,
    ),
    (
        "ErrInvalidJSONValueForFuncIndex",
        ErrInvalidJSONValueForFuncIndex,
    ),
    (
        "ErrJSONValueOutOfRangeForFuncIndex",
        ErrJSONValueOutOfRangeForFuncIndex,
    ),
    (
        "ErrFunctionalIndexDataIsTooLong",
        ErrFunctionalIndexDataIsTooLong,
    ),
    (
        "ErrFunctionalIndexNotApplicable",
        ErrFunctionalIndexNotApplicable,
    ),
    (
        "ErrDynamicPrivilegeNotRegistered",
        ErrDynamicPrivilegeNotRegistered,
    ),
    ("ErrConstraintNotFound", ErrConstraintNotFound),
    (
        "ErUserAccessDeniedForUserAccountBlockedByPasswordLock",
        ErUserAccessDeniedForUserAccountBlockedByPasswordLock,
    ),
    (
        "ErrDependentByCheckConstraint",
        ErrDependentByCheckConstraint,
    ),
    (
        "ErrEngineAttributeNotSupported",
        ErrEngineAttributeNotSupported,
    ),
    ("ErrJSONInBooleanContext", ErrJSONInBooleanContext),
    ("ErrTableWithoutPrimaryKey", ErrTableWithoutPrimaryKey),
    (
        "ErrSecondPasswordCannotBeEmpty",
        ErrSecondPasswordCannotBeEmpty,
    ),
    (
        "ErrPasswordCannotBeRetainedOnPluginChange",
        ErrPasswordCannotBeRetainedOnPluginChange,
    ),
    (
        "ErrCurrentPasswordCannotBeRetained",
        ErrCurrentPasswordCannotBeRetained,
    ),
    (
        "ErrOnlyOneDefaultPartionAllowed",
        ErrOnlyOneDefaultPartionAllowed,
    ),
    (
        "ErrWrongPartitionTypeExpectedSystemTime",
        ErrWrongPartitionTypeExpectedSystemTime,
    ),
    (
        "ErrSystemVersioningWrongPartitions",
        ErrSystemVersioningWrongPartitions,
    ),
    ("ErrSequenceRunOut", ErrSequenceRunOut),
    ("ErrSequenceInvalidData", ErrSequenceInvalidData),
    ("ErrSequenceAccessFail", ErrSequenceAccessFail),
    ("ErrNotSequence", ErrNotSequence),
    ("ErrUnknownSequence", ErrUnknownSequence),
    ("ErrWrongInsertIntoSequence", ErrWrongInsertIntoSequence),
    (
        "ErrSequenceInvalidTableStructure",
        ErrSequenceInvalidTableStructure,
    ),
    ("ErrMemExceedThreshold", ErrMemExceedThreshold),
    ("ErrForUpdateCantRetry", ErrForUpdateCantRetry),
    ("ErrAdminCheckTable", ErrAdminCheckTable),
    ("ErrTxnTooLarge", ErrTxnTooLarge),
    ("ErrWriteConflictInTiDB", ErrWriteConflictInTiDB),
    ("ErrOptOnTemporaryTable", ErrOptOnTemporaryTable),
    ("ErrDropTableOnTemporaryTable", ErrDropTableOnTemporaryTable),
    ("ErrUnsupportedReloadPlugin", ErrUnsupportedReloadPlugin),
    (
        "ErrUnsupportedReloadPluginVar",
        ErrUnsupportedReloadPluginVar,
    ),
    ("ErrTableLocked", ErrTableLocked),
    ("ErrNotExist", ErrNotExist),
    ("ErrTxnRetryable", ErrTxnRetryable),
    ("ErrCannotSetNilValue", ErrCannotSetNilValue),
    ("ErrInvalidTxn", ErrInvalidTxn),
    ("ErrEntryTooLarge", ErrEntryTooLarge),
    ("ErrNotImplemented", ErrNotImplemented),
    ("ErrInfoSchemaExpired", ErrInfoSchemaExpired),
    ("ErrInfoSchemaChanged", ErrInfoSchemaChanged),
    ("ErrBadNumber", ErrBadNumber),
    ("ErrCastAsSignedOverflow", ErrCastAsSignedOverflow),
    ("ErrCastNegIntAsUnsigned", ErrCastNegIntAsUnsigned),
    ("ErrInvalidYearFormat", ErrInvalidYearFormat),
    ("ErrInvalidYear", ErrInvalidYear),
    ("ErrIncorrectDatetimeValue", ErrIncorrectDatetimeValue),
    ("ErrInvalidTimeFormat", ErrInvalidTimeFormat),
    ("ErrInvalidWeekModeFormat", ErrInvalidWeekModeFormat),
    ("ErrFieldGetDefaultFailed", ErrFieldGetDefaultFailed),
    ("ErrIndexOutBound", ErrIndexOutBound),
    ("ErrUnsupportedOp", ErrUnsupportedOp),
    ("ErrRowNotFound", ErrRowNotFound),
    ("ErrTableStateCantNone", ErrTableStateCantNone),
    ("ErrColumnStateNonPublic", ErrColumnStateNonPublic),
    ("ErrIndexStateCantNone", ErrIndexStateCantNone),
    ("ErrInvalidRecordKey", ErrInvalidRecordKey),
    ("ErrColumnStateCantNone", ErrColumnStateCantNone),
    ("ErrUnsupportedValueForVar", ErrUnsupportedValueForVar),
    ("ErrUnsupportedIsolationLevel", ErrUnsupportedIsolationLevel),
    ("ErrLoadPrivilege", ErrLoadPrivilege),
    ("ErrInvalidPrivilegeType", ErrInvalidPrivilegeType),
    ("ErrUnknownFieldType", ErrUnknownFieldType),
    ("ErrInvalidSequence", ErrInvalidSequence),
    ("ErrCantGetValidID", ErrCantGetValidID),
    ("ErrCantSetToNull", ErrCantSetToNull),
    ("ErrSnapshotTooOld", ErrSnapshotTooOld),
    ("ErrInvalidTableID", ErrInvalidTableID),
    ("ErrInvalidType", ErrInvalidType),
    ("ErrUnknownAllocatorType", ErrUnknownAllocatorType),
    ("ErrAutoRandReadFailed", ErrAutoRandReadFailed),
    ("ErrInvalidIncrementAndOffset", ErrInvalidIncrementAndOffset),
    (
        "ErrWarnOptimizerHintUnsupportedHint",
        ErrWarnOptimizerHintUnsupportedHint,
    ),
    (
        "ErrWarnOptimizerHintInvalidToken",
        ErrWarnOptimizerHintInvalidToken,
    ),
    ("ErrWarnMemoryQuotaOverflow", ErrWarnMemoryQuotaOverflow),
    (
        "ErrWarnOptimizerHintParseError",
        ErrWarnOptimizerHintParseError,
    ),
    (
        "ErrWarnOptimizerHintInvalidInteger",
        ErrWarnOptimizerHintInvalidInteger,
    ),
    ("ErrWarnOptimizerHintWrongPos", ErrWarnOptimizerHintWrongPos),
    (
        "ErrUnsupportedSecondArgumentType",
        ErrUnsupportedSecondArgumentType,
    ),
    ("ErrColumnNotMatched", ErrColumnNotMatched),
    ("ErrInvalidPluginID", ErrInvalidPluginID),
    ("ErrInvalidPluginManifest", ErrInvalidPluginManifest),
    ("ErrInvalidPluginName", ErrInvalidPluginName),
    ("ErrInvalidPluginVersion", ErrInvalidPluginVersion),
    ("ErrDuplicatePlugin", ErrDuplicatePlugin),
    ("ErrInvalidPluginSysVarName", ErrInvalidPluginSysVarName),
    ("ErrRequireVersionCheckFail", ErrRequireVersionCheckFail),
    ("ErrUnsupportedType", ErrUnsupportedType),
    ("ErrAnalyzeMissIndex", ErrAnalyzeMissIndex),
    (
        "ErrCartesianProductUnsupported",
        ErrCartesianProductUnsupported,
    ),
    ("ErrPreparedStmtNotFound", ErrPreparedStmtNotFound),
    ("ErrWrongParamCount", ErrWrongParamCount),
    ("ErrSchemaChanged", ErrSchemaChanged),
    ("ErrUnknownPlan", ErrUnknownPlan),
    ("ErrPrepareMulti", ErrPrepareMulti),
    ("ErrPrepareDDL", ErrPrepareDDL),
    ("ErrResultIsEmpty", ErrResultIsEmpty),
    ("ErrBuildExecutor", ErrBuildExecutor),
    ("ErrBatchInsertFail", ErrBatchInsertFail),
    ("ErrGetStartTS", ErrGetStartTS),
    ("ErrPrivilegeCheckFail", ErrPrivilegeCheckFail),
    ("ErrInvalidWildCard", ErrInvalidWildCard),
    (
        "ErrMixOfGroupFuncAndFieldsIncompatible",
        ErrMixOfGroupFuncAndFieldsIncompatible,
    ),
    ("ErrBRIEBackupFailed", ErrBRIEBackupFailed),
    ("ErrBRIERestoreFailed", ErrBRIERestoreFailed),
    ("ErrBRIEImportFailed", ErrBRIEImportFailed),
    ("ErrBRIEExportFailed", ErrBRIEExportFailed),
    ("ErrInvalidTableSample", ErrInvalidTableSample),
    ("ErrJSONObjectKeyTooLong", ErrJSONObjectKeyTooLong),
    ("ErrMultiStatementDisabled", ErrMultiStatementDisabled),
    ("ErrPartitionStatsMissing", ErrPartitionStatsMissing),
    ("ErrNotSupportedWithSem", ErrNotSupportedWithSem),
    (
        "ErrDataInconsistentMismatchCount",
        ErrDataInconsistentMismatchCount,
    ),
    (
        "ErrDataInconsistentMismatchIndex",
        ErrDataInconsistentMismatchIndex,
    ),
    ("ErrAsOf", ErrAsOf),
    ("ErrVariableNoLongerSupported", ErrVariableNoLongerSupported),
    ("ErrAnalyzeMissColumn", ErrAnalyzeMissColumn),
    ("ErrInconsistentRowValue", ErrInconsistentRowValue),
    ("ErrInconsistentHandle", ErrInconsistentHandle),
    ("ErrInconsistentIndexedValue", ErrInconsistentIndexedValue),
    ("ErrAssertionFailed", ErrAssertionFailed),
    ("ErrInstanceScope", ErrInstanceScope),
    (
        "ErrNonTransactionalJobFailure",
        ErrNonTransactionalJobFailure,
    ),
    ("ErrSettingNoopVariable", ErrSettingNoopVariable),
    ("ErrGettingNoopVariable", ErrGettingNoopVariable),
    ("ErrCannotMigrateSession", ErrCannotMigrateSession),
    (
        "ErrLazyUniquenessCheckFailure",
        ErrLazyUniquenessCheckFailure,
    ),
    (
        "ErrUnsupportedColumnInTTLConfig",
        ErrUnsupportedColumnInTTLConfig,
    ),
    ("ErrTTLColumnCannotDrop", ErrTTLColumnCannotDrop),
    (
        "ErrSetTTLOptionForNonTTLTable",
        ErrSetTTLOptionForNonTTLTable,
    ),
    (
        "ErrTempTableNotAllowedWithTTL",
        ErrTempTableNotAllowedWithTTL,
    ),
    (
        "ErrUnsupportedTTLReferencedByFK",
        ErrUnsupportedTTLReferencedByFK,
    ),
    (
        "ErrUnsupportedPrimaryKeyTypeWithTTL",
        ErrUnsupportedPrimaryKeyTypeWithTTL,
    ),
    ("ErrLoadDataFromServerDisk", ErrLoadDataFromServerDisk),
    ("ErrLoadParquetFromLocal", ErrLoadParquetFromLocal),
    ("ErrLoadDataEmptyPath", ErrLoadDataEmptyPath),
    ("ErrLoadDataUnsupportedFormat", ErrLoadDataUnsupportedFormat),
    ("ErrLoadDataInvalidURI", ErrLoadDataInvalidURI),
    ("ErrLoadDataCantAccess", ErrLoadDataCantAccess),
    ("ErrLoadDataCantRead", ErrLoadDataCantRead),
    ("ErrLoadDataWrongFormatConfig", ErrLoadDataWrongFormatConfig),
    ("ErrUnknownOption", ErrUnknownOption),
    ("ErrInvalidOptionVal", ErrInvalidOptionVal),
    ("ErrDuplicateOption", ErrDuplicateOption),
    ("ErrLoadDataUnsupportedOption", ErrLoadDataUnsupportedOption),
    (
        "ErrLoadDataDuplicateKeyConflict",
        ErrLoadDataDuplicateKeyConflict,
    ),
    ("ErrLoadDataJobNotFound", ErrLoadDataJobNotFound),
    ("ErrLoadDataInvalidOperation", ErrLoadDataInvalidOperation),
    (
        "ErrLoadDataLocalUnsupportedOption",
        ErrLoadDataLocalUnsupportedOption,
    ),
    ("ErrLoadDataPreCheckFailed", ErrLoadDataPreCheckFailed),
    ("ErrBRJobNotFound", ErrBRJobNotFound),
    ("ErrMemoryExceedForQuery", ErrMemoryExceedForQuery),
    ("ErrMemoryExceedForInstance", ErrMemoryExceedForInstance),
    ("ErrDeleteNotFoundColumn", ErrDeleteNotFoundColumn),
    ("ErrKeyTooLarge", ErrKeyTooLarge),
    ("ErrTimeStampInDSTTransition", ErrTimeStampInDSTTransition),
    ("ErrQueryExecStopped", ErrQueryExecStopped),
    ("ErrUnsupportedDDLOperation", ErrUnsupportedDDLOperation),
    ("ErrNotOwner", ErrNotOwner),
    ("ErrCantDecodeRecord", ErrCantDecodeRecord),
    ("ErrInvalidDDLWorker", ErrInvalidDDLWorker),
    ("ErrInvalidDDLJob", ErrInvalidDDLJob),
    ("ErrInvalidDDLJobFlag", ErrInvalidDDLJobFlag),
    ("ErrWaitReorgTimeout", ErrWaitReorgTimeout),
    ("ErrInvalidStoreVersion", ErrInvalidStoreVersion),
    ("ErrUnknownTypeLength", ErrUnknownTypeLength),
    ("ErrUnknownFractionLength", ErrUnknownFractionLength),
    ("ErrInvalidDDLState", ErrInvalidDDLState),
    ("ErrReorgPanic", ErrReorgPanic),
    ("ErrInvalidSplitRegionRanges", ErrInvalidSplitRegionRanges),
    ("ErrInvalidDDLJobVersion", ErrInvalidDDLJobVersion),
    ("ErrCancelledDDLJob", ErrCancelledDDLJob),
    ("ErrRepairTable", ErrRepairTable),
    ("ErrInvalidAutoRandom", ErrInvalidAutoRandom),
    ("ErrInvalidHashKeyFlag", ErrInvalidHashKeyFlag),
    ("ErrInvalidListIndex", ErrInvalidListIndex),
    ("ErrInvalidListMetaData", ErrInvalidListMetaData),
    ("ErrWriteOnSnapshot", ErrWriteOnSnapshot),
    ("ErrInvalidKey", ErrInvalidKey),
    ("ErrInvalidIndexKey", ErrInvalidIndexKey),
    ("ErrDataInconsistent", ErrDataInconsistent),
    ("ErrDDLJobNotFound", ErrDDLJobNotFound),
    ("ErrCancelFinishedDDLJob", ErrCancelFinishedDDLJob),
    ("ErrCannotCancelDDLJob", ErrCannotCancelDDLJob),
    (
        "ErrSequenceUnsupportedTableOption",
        ErrSequenceUnsupportedTableOption,
    ),
    (
        "ErrColumnTypeUnsupportedNextValue",
        ErrColumnTypeUnsupportedNextValue,
    ),
    ("ErrLockExpire", ErrLockExpire),
    (
        "ErrAddColumnWithSequenceAsDefault",
        ErrAddColumnWithSequenceAsDefault,
    ),
    (
        "ErrUnsupportedConstraintCheck",
        ErrUnsupportedConstraintCheck,
    ),
    (
        "ErrTableOptionUnionUnsupported",
        ErrTableOptionUnionUnsupported,
    ),
    (
        "ErrTableOptionInsertMethodUnsupported",
        ErrTableOptionInsertMethodUnsupported,
    ),
    ("ErrDDLReorgElementNotExist", ErrDDLReorgElementNotExist),
    ("ErrPlacementPolicyCheck", ErrPlacementPolicyCheck),
    ("ErrInvalidAttributesSpec", ErrInvalidAttributesSpec),
    ("ErrPlacementPolicyExists", ErrPlacementPolicyExists),
    ("ErrPlacementPolicyNotExists", ErrPlacementPolicyNotExists),
    (
        "ErrPlacementPolicyWithDirectOption",
        ErrPlacementPolicyWithDirectOption,
    ),
    ("ErrPlacementPolicyInUse", ErrPlacementPolicyInUse),
    ("ErrOptOnCacheTable", ErrOptOnCacheTable),
    ("ErrHTTPServiceError", ErrHTTPServiceError),
    (
        "ErrPartitionColumnStatsMissing",
        ErrPartitionColumnStatsMissing,
    ),
    ("ErrColumnInChange", ErrColumnInChange),
    ("ErrDDLSetting", ErrDDLSetting),
    ("ErrIngestFailed", ErrIngestFailed),
    ("ErrIngestCheckEnvFailed", ErrIngestCheckEnvFailed),
    ("ErrProtectedTableMode", ErrProtectedTableMode),
    ("ErrInvalidTableModeSet", ErrInvalidTableModeSet),
    ("ErrCannotPauseDDLJob", ErrCannotPauseDDLJob),
    ("ErrCannotResumeDDLJob", ErrCannotResumeDDLJob),
    ("ErrPausedDDLJob", ErrPausedDDLJob),
    ("ErrBDRRestrictedDDL", ErrBDRRestrictedDDL),
    (
        "ErrGlobalIndexNotExplicitlySet",
        ErrGlobalIndexNotExplicitlySet,
    ),
    (
        "ErrWarnGlobalIndexNeedManuallyAnalyze",
        ErrWarnGlobalIndexNeedManuallyAnalyze,
    ),
    ("ErrInvalidAffinityOption", ErrInvalidAffinityOption),
    ("ErrForbiddenDDL", ErrForbiddenDDL),
    ("ErrMaskingPolicyExists", ErrMaskingPolicyExists),
    ("ErrMaskingPolicyNotExists", ErrMaskingPolicyNotExists),
    (
        "ErrMaskingPolicyExprInvalidColumn",
        ErrMaskingPolicyExprInvalidColumn,
    ),
    ("ErrDDLAutoPausedByKVDiskFull", ErrDDLAutoPausedByKVDiskFull),
    ("ErrResourceGroupExists", ErrResourceGroupExists),
    ("ErrResourceGroupNotExists", ErrResourceGroupNotExists),
    (
        "ErrResourceGroupSupportDisabled",
        ErrResourceGroupSupportDisabled,
    ),
    (
        "ErrResourceGroupConfigUnavailable",
        ErrResourceGroupConfigUnavailable,
    ),
    ("ErrResourceGroupThrottled", ErrResourceGroupThrottled),
    (
        "ErrResourceGroupQueryRunawayInterrupted",
        ErrResourceGroupQueryRunawayInterrupted,
    ),
    (
        "ErrResourceGroupQueryRunawayQuarantine",
        ErrResourceGroupQueryRunawayQuarantine,
    ),
    (
        "ErrResourceGroupInvalidBackgroundTaskName",
        ErrResourceGroupInvalidBackgroundTaskName,
    ),
    (
        "ErrResourceGroupInvalidForRole",
        ErrResourceGroupInvalidForRole,
    ),
    (
        "ErrEngineAttributeInvalidFormat",
        ErrEngineAttributeInvalidFormat,
    ),
    ("ErrStorageClassInvalidSpec", ErrStorageClassInvalidSpec),
    (
        "ErrModifyColumnReferencedByPartialCondition",
        ErrModifyColumnReferencedByPartialCondition,
    ),
    (
        "ErrCheckPartialIndexWithoutFastCheck",
        ErrCheckPartialIndexWithoutFastCheck,
    ),
    ("ErrMaxKeysReadExceeded", ErrMaxKeysReadExceeded),
    ("ErrPDServerTimeout", ErrPDServerTimeout),
    ("ErrTiKVServerTimeout", ErrTiKVServerTimeout),
    ("ErrTiKVServerBusy", ErrTiKVServerBusy),
    ("ErrResolveLockTimeout", ErrResolveLockTimeout),
    ("ErrRegionUnavailable", ErrRegionUnavailable),
    ("ErrTxnAbortedByGC", ErrTxnAbortedByGC),
    ("ErrWriteConflict", ErrWriteConflict),
    ("ErrTiKVStoreLimit", ErrTiKVStoreLimit),
    ("ErrPrometheusAddrIsNotSet", ErrPrometheusAddrIsNotSet),
    ("ErrTiKVStaleCommand", ErrTiKVStaleCommand),
    ("ErrTiKVMaxTimestampNotSynced", ErrTiKVMaxTimestampNotSynced),
    ("ErrTiFlashServerTimeout", ErrTiFlashServerTimeout),
    ("ErrTiFlashServerBusy", ErrTiFlashServerBusy),
    ("ErrTiFlashBackfillIndex", ErrTiFlashBackfillIndex),
    ("ErrUserPrefixMismatch", ErrUserPrefixMismatch),
];
