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
		gTask.Step = stepOne
		subTasks = append(subTasks, &proto.Subtask{})
	case stepOne:
		gTask.Step = stepTwo
		subTasks = append(subTasks, &proto.Subtask{})
	case stepTwo:
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
