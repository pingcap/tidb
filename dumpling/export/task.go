// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import "fmt"

// Task is a file dump task for dumpling, it could either be dumping database/table/view/policy metadata, table data
type Task interface {
	// Brief is the brief for a dumping task
	Brief() string
}

// TaskDatabaseMeta is a dumping database metadata task
type TaskDatabaseMeta struct {
	Task
	DatabaseName      string
	CreateDatabaseSQL string
}

// TaskTableMeta is a dumping table metadata task
type TaskTableMeta struct {
	Task
	DatabaseName   string
	TableName      string
	CreateTableSQL string
}

// TaskViewMeta is a dumping view metadata task
type TaskViewMeta struct {
	Task
	DatabaseName   string
	ViewName       string
	CreateTableSQL string
	CreateViewSQL  string
}

// TaskSequenceMeta is a dumping sequence metadata task
type TaskSequenceMeta struct {
	Task
	DatabaseName      string
	SequenceName      string
	CreateSequenceSQL string
}

// TaskPolicyMeta is a dumping view metadata task
type TaskPolicyMeta struct {
	Task
	PolicyName      string
	CreatePolicySQL string
}

// TaskTableData is a dumping table data task
type TaskTableData struct {
	Task
	Meta        TableMeta
	Data        TableDataIR
	ChunkIndex  int
	TotalChunks int
}

// NewTaskDatabaseMeta returns a new dumping database metadata task
func NewTaskDatabaseMeta(dbName, createSQL string) *TaskDatabaseMeta {
	return &TaskDatabaseMeta{
		DatabaseName:      dbName,
		CreateDatabaseSQL: createSQL,
	}
}

// NewTaskTableMeta returns a new dumping table metadata task
func NewTaskTableMeta(dbName, tblName, createSQL string) *TaskTableMeta {
	return &TaskTableMeta{
		DatabaseName:   dbName,
		TableName:      tblName,
		CreateTableSQL: createSQL,
	}
}

// NewTaskViewMeta returns a new dumping view metadata task
func NewTaskViewMeta(dbName, tblName, createTableSQL, createViewSQL string) *TaskViewMeta {
	return &TaskViewMeta{
		DatabaseName:   dbName,
		ViewName:       tblName,
		CreateTableSQL: createTableSQL,
		CreateViewSQL:  createViewSQL,
	}
}

// NewTaskSequenceMeta returns a new dumping sequence metadata task
func NewTaskSequenceMeta(dbName, tblName, createSequenceSQL string) *TaskSequenceMeta {
	return &TaskSequenceMeta{
		DatabaseName:      dbName,
		SequenceName:      tblName,
		CreateSequenceSQL: createSequenceSQL,
	}
}

// NewTaskPolicyMeta returns a new dumping placement policy metadata task
func NewTaskPolicyMeta(policyName, createPolicySQL string) *TaskPolicyMeta {
	return &TaskPolicyMeta{
		PolicyName:      policyName,
		CreatePolicySQL: createPolicySQL,
	}
}

// NewTaskTableData returns a new dumping table data task
func NewTaskTableData(meta TableMeta, data TableDataIR, currentChunk, totalChunks int) *TaskTableData {
	return &TaskTableData{
		Meta:        meta,
		Data:        data,
		ChunkIndex:  currentChunk,
		TotalChunks: totalChunks,
	}
}

// Brief implements task.Brief
func (t *TaskDatabaseMeta) Brief() string {
	return fmt.Sprintf("meta of dababase '%s'", t.DatabaseName)
}

// Brief implements task.Brief
func (t *TaskTableMeta) Brief() string {
	return fmt.Sprintf("meta of table '%s'.'%s'", t.DatabaseName, t.TableName)
}

// Brief implements task.Brief
func (t *TaskViewMeta) Brief() string {
	return fmt.Sprintf("meta of view '%s'.'%s'", t.DatabaseName, t.ViewName)
}

// Brief implements task.Brief
func (t *TaskSequenceMeta) Brief() string {
	return fmt.Sprintf("meta of sequence '%s'.'%s'", t.DatabaseName, t.SequenceName)
}

// Brief implements task.Brief
func (t *TaskPolicyMeta) Brief() string {
	return fmt.Sprintf("meta of placement policy '%s'", t.PolicyName)
}

// Brief implements task.Brief
func (t *TaskTableData) Brief() string {
	db, tbl := t.Meta.DatabaseName(), t.Meta.TableName()
	idx, total := t.ChunkIndex, t.TotalChunks
	return fmt.Sprintf("data of table '%s'.'%s'(%d/%d)", db, tbl, idx, total)
}
