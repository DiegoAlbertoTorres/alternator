package main

// ExtNode is an external, non-local node
type ExtNode struct {
	ID      Key
	Address string
}

func (extNode ExtNode) String() (str string) {
	str += "ID: " + keyToString(extNode.ID) + "\n"
	str += "Address: " + extNode.Address + "\n"
	return
}

// Unresolved: why does this not need to be an actual copy?
func extNodeCopy(src *ExtNode, dst *ExtNode) {
	// fmt.Println("copying this " + keyToString(src.ID))
	// fmt.Printf("copied %d\n", copy(dst.ID, src.ID))
	dst.ID = src.ID
	dst.Address = src.Address
}
