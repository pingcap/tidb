package example

import (
	"errors"

	"github.com/pingcap/tidb/distribute_framework/dispatcher"
	"github.com/pingcap/tidb/distribute_framework/proto"
)

type NumberExampleHandle struct {
}

func (n NumberExampleHandle) Progress(d *dispatcher.Dispatcher, gTask *proto.Task) (finished bool, subTasks []*proto.Subtask, err error) {
	switch gTask.Step {
	case proto.StepInit:
		gTask.Step = proto.StepOne
		gTask.Concurrency = 4
		for i := 0; i < 10; i++ {
			subTasksM := proto.SimpleNumberSTaskMeta{Numbers: make([]int, 0, 10)}
			for j := 0; j < 10; j++ {
				subTasksM.Numbers = append(subTasksM.Numbers, i*10+j)
			}
			subTasks = append(subTasks, &proto.Subtask{Meta: &subTasksM})
		}
	case proto.StepOne:
		gTask.Step = proto.StepTwo
		gTask.Concurrency = 6
		for i := 0; i < 10; i++ {
			subTasksM := proto.SimpleNumberSTaskMeta{Numbers: make([]int, 0, 10)}
			for j := 0; j < 10; j++ {
				subTasksM.Numbers = append(subTasksM.Numbers, i*10+j)
			}
			subTasks = append(subTasks, &proto.Subtask{Meta: &subTasksM})
		}
	case proto.StepTwo:
		return true, nil, nil
	default:
		return false, nil, errors.New("unknown step")
	}
	return false, subTasks, nil
}

func (n NumberExampleHandle) HandleError(d *dispatcher.Dispatcher, gTask *proto.Task, receive string) error {
	// Don't handle not.
	return nil
}

func init() {
	dispatcher.RegisterTaskDispatcherHandle(proto.TaskTypeExample, NumberExampleHandle{})
}
