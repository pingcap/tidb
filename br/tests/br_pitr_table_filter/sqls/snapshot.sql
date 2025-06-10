-- ActionDropSchema
create database test_snapshot_db_to_be_deleted;
create table test_snapshot_db_to_be_deleted.t1 (id int);

-- ActionCreateTable
create database test_snapshot_db_create;

-- ActionCreateTables
-- ActionDropTable
create table test_snapshot_db_create.t_to_be_deleted (id int);

-- ActionAddColumn
create table test_snapshot_db_create.t_add_column (id int);

-- ActionDropColumn
create table test_snapshot_db_create.t_drop_column (id int, a int);

-- ActionAddIndex
create table test_snapshot_db_create.t_add_index (id int);
create table test_snapshot_db_create.t_add_unique_key (id int);

-- ActionDropIndex
create table test_snapshot_db_create.t_drop_index (id int, key i1(id));
create table test_snapshot_db_create.t_drop_unique_key (id int, unique key i1(id));

-- ActionAddForeignKey
-- ActionDropForeignKey
-- ActionTruncateTable
create table test_snapshot_db_create.t_to_be_truncated (id int);

-- ActionModifyColumn
create table test_snapshot_db_create.t_modify_column (id int);

-- ActionRebaseAutoID
-- ActionRenameTable
create table test_snapshot_db_create.t_rename_a (id int);

-- ActionRenameTables
create table test_snapshot_db_create.t_renames_a (id int);
create table test_snapshot_db_create.t_renames_b (id int);
create table test_snapshot_db_create.t_renames_aa (id int);
create table test_snapshot_db_create.t_renames_bb (id int);

-- ActionSetDefaultValue
-- ActionShardRowID
-- ActionModifyTableComment
create table test_snapshot_db_create.t_modify_comment (id int) comment='before modify column';

-- ActionRenameIndex
create table test_snapshot_db_create.t_rename_index (id int, index i1 (id));

-- ActionAddTablePartition

-- ActionDropTablePartition

-- ActionCreateView
-- ActionModifyTableCharsetAndCollate

-- ActionTruncateTablePartition

-- ActionDropView
-- ActionRecoverTable
-- ActionModifySchemaCharsetAndCollate

-- ActionLockTable
-- ActionUnlockTable
-- ActionRepairTable
-- ActionSetTiFlashReplica
-- ActionUpdateTiFlashReplicaStatus
-- ActionAddPrimaryKey
create table test_snapshot_db_create.t_add_primary_key (id int);

-- ActionDropPrimaryKey
create table test_snapshot_db_create.t_drop_primary_key (id int, primary key (id) NONCLUSTERED);

-- ActionCreateSequence
-- ActionAlterSequence
-- ActionDropSequence
-- ActionModifyTableAutoIDCache
-- ActionRebaseAutoRandomBase
-- ActionAlterIndexVisibility
-- ActionExchangeTablePartition
-- ActionAddCheckConstraint
-- ActionDropCheckConstraint
-- ActionAlterCheckConstraint
-- ActionAlterTableAttributes
-- ActionAlterTablePartitionPlacement
-- ActionAlterTablePartitionAttributes
-- ActionCreatePlacementPolicy
-- ActionAlterPlacementPolicy
-- ActionDropPlacementPolicy
-- ActionModifySchemaDefaultPlacement
-- ActionAlterTablePlacement
-- ActionAlterCacheTable
-- ActionAlterNoCacheTable
-- ActionAlterTableStatsOptions
-- ActionMultiSchemaChange
-- ActionFlashbackCluster
-- ActionRecoverSchema
-- ActionReorganizePartition
-- ActionAlterTTLInfo
-- ActionAlterTTLRemove
-- ActionCreateResourceGroup
-- ActionAlterResourceGroup
-- ActionDropResourceGroup
-- ActionAlterTablePartitioning
-- ActionRemovePartitioning
-- ActionAddVectorIndex
-- ActionAlterTableMode
-- ActionRefreshMeta


