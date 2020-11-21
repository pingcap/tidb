package meta

import (
	"encoding/json"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
)

type SubTaskType byte

type SubTaskStatus byte

const (
	AddIndexSubTaskType SubTaskType = 1

	SubTaskTypeStr = "subTask"
)

const (
	Running     SubTaskStatus = 0
	Unclaimed   SubTaskStatus = 1
	Failed      SubTaskStatus = 2
	Success     SubTaskStatus = 3
	Reorganized SubTaskStatus = 4
	UNKOWN      SubTaskStatus = 10
)

type ParentJob struct {
	SubTaskNum int64 `json:"sub_task_num"`
}

type SubTask struct {
	JobID    int64           `json:"jobId"`
	TaskID   int64           `json:"taskID"`
	TaskType SubTaskType     `json:"taskType"`
	RawArgs  json.RawMessage `json:"raw_args"`
	Args     []interface{}   `json:"-"`
	StartTS  uint64          `json:"start_ts"`
}

type AddIndexSubTask struct {
	TableInfo *model.TableInfo `json:"tbl_info"`
	IndexInfo *model.IndexInfo `json:"index_info"`
	SchemaID  int64            `json:"schema_id"`
}

func (task *SubTask) Decode(b []byte) error {
	err := json.Unmarshal(b, task)
	return errors.Trace(err)
}

func (task *SubTask) Encode(updateRawArgs bool) ([]byte, error) {
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
