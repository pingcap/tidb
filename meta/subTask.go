package meta

import (
	"encoding/json"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
)

// ActionType of SubTask
type SubTaskType byte

type SubTaskStatus byte

const (
	// addIndexSubTaskType
	AddIndexSubTaskType SubTaskType = 1

	//When the subTask status is not executed or empty
	RunnerEmptyStr = ""

	//SubTask worker type
	SubTaskTypeStr = "subTask"
)

// the status of execution SubTask
const (
	Running     SubTaskStatus = 0
	Unclaimed   SubTaskStatus = 1
	Failed      SubTaskStatus = 2
	Success     SubTaskStatus = 3
	Reorganized SubTaskStatus = 4
	UNKNOWN     SubTaskStatus = 10
)

// extra attribute for the job that has been split
type ParentJob struct {
	SubTaskNum int64 `json:"sub_task_num"`
}

//  the definition of the  SubTask
type SubTask struct {
	JobID    int64           `json:"jobId"`
	TaskID   int64           `json:"taskID"`
	TaskType SubTaskType     `json:"taskType"`
	RawArgs  json.RawMessage `json:"raw_args"`
	Args     []interface{}   `json:"-"`
	StartTS  uint64          `json:"start_ts"`
}

//  the definition of the addIndex subTask
type AddIndexSubTask struct {
	TableInfo *model.TableInfo `json:"tbl_info"`
	IndexInfo *model.IndexInfo `json:"index_info"`
	SchemaID  int64            `json:"schema_id"`
}

func (task *SubTask) Decode(b []byte) error {
	err := json.Unmarshal(b, task)
	return errors.Trace(err)
}

func (task *SubTask) encode(updateRawArgs bool) ([]byte, error) {
	var err error
	if updateRawArgs {
		task.RawArgs, err = json.Marshal(task.Args)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	var b []byte
	b, err = json.Marshal(task)
	return b, errors.Trace(err)
}

// decode specific json args for a SubTask
func (task *SubTask) DecodeArgs(args ...interface{}) error {
	var rawArgs []json.RawMessage
	if err := json.Unmarshal(task.RawArgs, &rawArgs); err != nil {
		return errors.Trace(err)
	}

	sz := len(rawArgs)
	if sz > len(args) {
		sz = len(args)
	}

	for i := 0; i < sz; i++ {
		if err := json.Unmarshal(rawArgs[i], args[i]); err != nil {
			return errors.Trace(err)
		}
	}
	task.Args = args[:sz]
	return nil
}
