package main

// ExtNode is an external, non-local node
type ExtNode struct {
	ID      string
	Address string
}

func (extNode ExtNode) String() (str string) {
	str += "ID:" + extNode.ID + "\n"
	str += "Address:" + extNode.Address + "\n"
	return
}

func extNodeCopy(src *ExtNode, dst *ExtNode) {
	dst.ID = src.ID
	dst.Address = src.Address
}

func (extNode *ExtNode) isNil() bool {
	return ((extNode == nil) || (extNode.ID == "nil"))
}
