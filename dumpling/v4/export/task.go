package export

import "fmt"

type Task interface {
	Brief() string
}

type TaskDatabaseMeta struct {
	Task
	DatabaseName      string
	CreateDatabaseSQL string
}

type TaskTableMeta struct {
	Task
	DatabaseName   string
	TableName      string
	CreateTableSQL string
}

type TaskViewMeta struct {
	Task
	DatabaseName   string
	ViewName       string
	CreateTableSQL string
	CreateViewSQL  string
}

type TaskTableData struct {
	Task
	Meta        TableMeta
	Data        TableDataIR
	ChunkIndex  int
	TotalChunks int
}

func NewTaskDatabaseMeta(dbName, createSQL string) *TaskDatabaseMeta {
	return &TaskDatabaseMeta{
		DatabaseName:      dbName,
		CreateDatabaseSQL: createSQL,
	}
}

func NewTaskTableMeta(dbName, tblName, createSQL string) *TaskTableMeta {
	return &TaskTableMeta{
		DatabaseName:   dbName,
		TableName:      tblName,
		CreateTableSQL: createSQL,
	}
}

func NewTaskViewMeta(dbName, tblName, createTableSQL, createViewSQL string) *TaskViewMeta {
	return &TaskViewMeta{
		DatabaseName:   dbName,
		ViewName:       tblName,
		CreateTableSQL: createTableSQL,
		CreateViewSQL:  createViewSQL,
	}
}

func NewTaskTableData(meta TableMeta, data TableDataIR, currentChunk, totalChunks int) *TaskTableData {
	return &TaskTableData{
		Meta:        meta,
		Data:        data,
		ChunkIndex:  currentChunk,
		TotalChunks: totalChunks,
	}
}

func (t *TaskDatabaseMeta) Brief() string {
	return fmt.Sprintf("meta of dababase '%s'", t.DatabaseName)
}

func (t *TaskTableMeta) Brief() string {
	return fmt.Sprintf("meta of table '%s'.'%s'", t.DatabaseName, t.TableName)
}

func (t *TaskViewMeta) Brief() string {
	return fmt.Sprintf("meta of view '%s'.'%s'", t.DatabaseName, t.ViewName)
}

func (t *TaskTableData) Brief() string {
	db, tbl := t.Meta.DatabaseName(), t.Meta.TableName()
	idx, total := t.ChunkIndex, t.TotalChunks
	return fmt.Sprintf("data of table '%s'.'%s'(%d/%d)", db, tbl, idx, total)
}
