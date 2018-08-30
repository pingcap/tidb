package plan

// Trace represents a trace plan.
type Trace struct {
	baseSchemaProducer

	StmtPlan Plan
}
