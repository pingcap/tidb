package example

import (
	"errors"
	"github.com/pingcap/tidb/distribute_framework/dispatcher"
	"github.com/pingcap/tidb/distribute_framework/proto"
	"github.com/pingcap/tidb/util/logutil"
)

type NumberExampleHandle struct {
}

func (n NumberExampleHandle) Progress(d dispatcher.Dispatch, gTask *proto.Task, fromPending bool) (finished bool, subTasks []*proto.Subtask, err error) {
	if fromPending {
		gTask.Step = proto.StepInit
	}
	switch gTask.Step {
	case proto.StepInit:
		gTask.Step = proto.StepOne
		gTask.Concurrency = 4
		for i := 0; i < 10; i++ {
			subTasksM := proto.SimpleNumberSTaskMeta{Numbers: make([]int, 0, 10)}
			for j := 0; j < 10; j++ {
				subTasksM.Numbers = append(subTasksM.Numbers, j)
			}
			subTasks = append(subTasks, &proto.Subtask{Meta: &subTasksM})
		}
		logutil.BgLogger().Info("progress step init")
	case proto.StepOne:
		gTask.Step = proto.StepTwo
		gTask.Concurrency = 6
		for i := 0; i < 10; i++ {
			subTasksM := proto.SimpleNumberSTaskMeta{Numbers: make([]int, 0, 10)}
			for j := 0; j < 10; j++ {
				subTasksM.Numbers = append(subTasksM.Numbers, j)
			}
			subTasks = append(subTasks, &proto.Subtask{Meta: &subTasksM})
		}
		logutil.BgLogger().Info("progress step one")
	case proto.StepTwo:
		logutil.BgLogger().Info("progress step two")
		return true, nil, nil
	default:
		return false, nil, errors.New("unknown step")
	}
	return false, subTasks, nil
}

func (n NumberExampleHandle) HandleError(d dispatcher.Dispatch, gTask *proto.Task, receive string) (meta proto.SubTaskMeta, err error) {
	// Don't handle not.
	return &proto.SimpleNumberSTaskMeta{}, nil
}

func init() {
	dispatcher.RegisterGTaskFlowHandle(proto.TaskTypeExample, NumberExampleHandle{})
}
