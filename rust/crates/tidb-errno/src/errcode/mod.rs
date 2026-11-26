// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");

//! Direct constants from `pkg/errno/errcode.go`.

// The constants and `ALL_CODES` are generated from Go's errcode.go by
// rust/scripts/generate-errno.py.
mod codes_generated;

pub use codes_generated::*;

#[cfg(test)]
mod source_tests {
    use super::*;

    #[test]
    fn test_error_code() {
        // Direct port of pkg/table/table_test.go::TestErrorCode.  The Go
        // table package constructs terror errors from these errno bindings;
        // Rust owns the same source constants directly, so this matrix fixes
        // every table-local binding at its wire-visible value.
        let cases = [
            ("ErrColumnCantNull", ErrBadNull, 1048),
            ("ErrUnknownColumn", ErrBadField, 1054),
            ("errDuplicateColumn", ErrFieldSpecifiedTwice, 1110),
            ("errGetDefaultFailed", ErrFieldGetDefaultFailed, 8038),
            ("ErrNoDefaultValue", ErrNoDefaultForField, 1364),
            ("ErrIndexOutBound", ErrIndexOutBound, 8039),
            ("ErrUnsupportedOp", ErrUnsupportedOp, 8040),
            ("ErrRowNotFound", ErrRowNotFound, 8041),
            ("ErrTableStateCantNone", ErrTableStateCantNone, 8042),
            ("ErrColumnStateCantNone", ErrColumnStateCantNone, 8046),
            ("ErrColumnStateNonPublic", ErrColumnStateNonPublic, 8043),
            ("ErrIndexStateCantNone", ErrIndexStateCantNone, 8044),
            ("ErrInvalidRecordKey", ErrInvalidRecordKey, 8045),
            (
                "ErrTruncatedWrongValueForField",
                ErrTruncatedWrongValueForField,
                1366,
            ),
            ("ErrUnknownPartition", ErrUnknownPartition, 1735),
            (
                "ErrNoPartitionForGivenValue",
                ErrNoPartitionForGivenValue,
                1526,
            ),
            (
                "ErrLockOrActiveTransaction",
                ErrLockOrActiveTransaction,
                1192,
            ),
        ];

        for (table_error, actual, expected) in cases {
            assert_eq!(actual, expected, "{table_error}");
        }
    }
}
