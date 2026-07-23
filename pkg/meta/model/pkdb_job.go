package model

// Only used in PingkaiDB.
const (
	ActionCreateTrigger   ActionType = 200
	ActionDropTrigger     ActionType = 201
	ActionCreateProcedure ActionType = 202
	ActionDropProcedure   ActionType = 203
	ActionAlterProcedure  ActionType = 204
)

func init() {
	ActionMap[ActionCreateTrigger] = "create trigger"
	ActionMap[ActionDropTrigger] = "drop trigger"
	ActionMap[ActionCreateProcedure] = "create procedure"
	ActionMap[ActionDropProcedure] = "drop procedure"
	ActionMap[ActionAlterProcedure] = "alter procedure"
}
