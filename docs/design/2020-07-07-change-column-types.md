# Proposal: Changing column types

- Author(s):     [zimuxia](https://github.com/zimulala) (Xia Li)
- Last updated:  2020-07-07
- Discussion at: https://tidbcommunity.slack.com/archives/CMAKWBNJU

## Abstract

This proposal proposes a new feature that supports column type modification more comprehensively.

## Background

This feature mainly uses the syntax of `alter table ... change/modify column` to support the modification of column type. The specific syntax is as follows:

```sql
ALTER TABLE tbl_name
    [alter_specification]

alter_specification:
CHANGE [COLUMN] old_col_name new_col_name column_definition
        [FIRST | AFTER col_name]
| MODIFY [COLUMN] col_name column_definition
        [FIRST | AFTER col_name]
```

At present, column type modification only supports the lengthening of the same type, that is, there is no true change to the data type in the storage layer. The limitation of the new support is as follows:
* Lossy changes are not supported, such as changing from BIGINT to INTEGER, or from VARCHAR(255) to VARCHAR(10)
* Modification of the precision of DECIMAL type is not supported
* Does not support changing the UNSIGNED attribute
* Only support changing CHARACTER SET attribute from utf8 to utf8mb4

## Proposal

The column type modifications supported by this proposal will involve rewriting column data and refactoring related indexes. This operation still needs to meet the basic requirements of online DDL operation, and to meet the compatibility between previous and subsequent versions of TiDB.

To support this feature, TiDB needs to add additional related columns to the modified column. The type of the newly added column is the type you want to modify. In addition, it is necessary to add each additional related index to the index that contains this column, and update the type of the corresponding column in new indexes.

## Rationale

This proposal suggests adding additional related columns and indexes which involve the column to be modified, that is, making a copy of the columns and indexes that contain `the changed/modified column`. And if user data is written during the copying process, the corresponding data needs to be updated in real time. Assume that the type of `colA` is changed from `originalType` to `newType`, `idxA` (index with `colA`, here it is assumed that there is only one corresponding general index, the index has no relation to column generation).

### Pre-preparation

Add `ChangingCol` and `ChangingIdx` fields to the column and related index that need to be modified, which are used to associate the modified column and index information.

When performing an insert or update operation (executed in the `AddRecord` or `UpdateRecord` function) on the column whose type is to be modified, the column and index to be backed up are written or updated according to the `newType` type.

### Process

1. Update the metadata information, including creating a corresponding column for `changingcolA` and `changingIdxA` for idxA, and add them to the end of the column array or index array in `TableInfo`. The state of `changingColA` and `changingIdxA` can be changed similar to add index operation, especially in `StateWriteReorganization` state, such as initializing reorganization information.
2. In the reorganization processing stage, row data and index data need to be processed in batches, which is similar to the `add index` operation during initial implementation. Get the batch processing range, and then use the `newType` to construct the `changingColA` and `changingIdxA` information for the insert operation (this operation requires the corresponding row and index column to be locked). If data-truncated are encountered and errors at this stage, it is necessary to report the errors and roll back to exit.
3. The next phase of change requires 3 status changes:
    * Lock this table and cannot write to it.
    * Drop `colA` and `idxA`, change the status of `changingColA` and `changingIdxA` to public, and change their names, offsets, etc.
    * Unlock this table.

### Note

This operation itself is an `alter table tableName modify/change column …` statement, which includes changing the column type, changing the column attribute from null to not null, changing the column offset, etc. Considering the atomicity, some operations need to be changed together.

In addition, due to the complexity of this feature, considering that the friendly review needs to be divided into multiple PRs, a switch can be added to deal with the problem of multiple PRs.

### Rolling back

This requires changing the state of `colA` and `idxA` to `StatePublic`, and then dropping `changingColA` and `changingIdxA`. Of course, if you modify the flag and other attributes, you need to roll back.

## Compatibility

### Compatibility issues with MySQL

* Considering the complexity of supporting clustered-index and currently TiDB does not fully support clustered-index, so temporarily this feature does not support type modification for columns with the primary key.
* In addition, we temporarily do not support column type change on three features of partition table, generated column, and expression index in the first stage of implementation.

### Compatibility issues with TiDB

The DDL statement itself already has a corresponding type (ActionModifyColumn). If you need to improve this operation, you will encounter compatibility issues when performing rolling upgrades. This problem is handled by adding a global variable `tidb_enable_change_column_type`. When this value is true, the `modify/change column` statement is allowed to perform more types of column type changes (features supported by this proposal).

* For new clusters, set this variable to true during bootstrap.
* For clusters requiring rolling upgrade, the default value is false, and users need to explicitly set this variable to true.

### Compatible issues with other components

This feature needs to be synchronized with components such as Tools, TiFlash, and BR.

## Implementation

In addition to considering the update of the corresponding column type and data, and the update of the data in the index column involved, the implementation of column type modification also needs to consider the following characteristics.

### Generate columns

When the modified type relates to generated columns (either stored or virtual), the implementation has the following characteristics:

* The generated column type related to this column is unchanged.
* When performing an insert operation, the column value of the modified type will be affected by the generated column expression with this column. 
* The value of the relevant generated column changes with the value of the column of the modified type.

### Expression index

When columns of the modified type have a related expression index, the implementation has the following characteristics:

* The Type, DBType and Max_length of the related expression index may be modified.
* When performing an insert operation, the value of the column of the modified type will be affected by the expression in the expression index of this column.

## Open issues (if applicable)

https://github.com/pingcap/tidb/issues/17526
