package ast

// txtNode is the struct implements partial node interface
// can be embeded by other nodes.
type txtNode struct {
	txt string
}

// SetText implements Node interface.
func (bn *txtNode) SetText(text string) {
	bn.txt = text
}

// Text implements Node interface.
func (bn *txtNode) Text() string {
	return bn.txt
}

// VariableAssignment is a variable assignment struct.
type VariableAssignment struct {
	txtNode
	Name     string
	Value    Expression
	IsGlobal bool
	IsSystem bool
}

// Accept implements Node interface.
func (va *VariableAssignment) Accept(v Visitor) (Node, bool) {
	node, ok := v.VisitEnter(va)
	if !ok {
		return node, false
	}
	node, ok = va.Value.Accept(v)
	if !ok {
		return va, false
	}
	va.Value = node.(Expression)
	return v.VisitLeave(va)
}

// SetStmt is the statement to set variables.
type SetStmt struct {
	txtNode
	// Variables is the list of variable assignment.
	Variables []*VariableAssignment
}

// Accept implements Node interface.
func (set *SetStmt) Accept(v Visitor) (Node, bool) {
	node, ok := v.VisitEnter(set)
	if !ok {
		return node, false
	}
	for i, val := range set.Variables {
		node, ok = val.Accept(v)
		if !ok {
			return set, false
		}
		set.Variables[i] = node.(*VariableAssignment)
	}
	return v.VisitLeave(set)
}