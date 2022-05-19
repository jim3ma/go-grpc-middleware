package balancer

import "strings"

type nodeRecord struct {
	hashValue    uint64
	nodeKey      string
	member       Member
	virtualNodes []virtualNode
}

type virtualNode struct {
	hashValue uint64
	members   nodeRecord
}

func (vn virtualNode) less(rhs virtualNode) bool {
	if vn.hashValue == rhs.hashValue {
		if vn.members.hashValue == rhs.members.hashValue {
			return strings.Compare(vn.members.nodeKey, rhs.members.nodeKey) < 0
		}

		return vn.members.hashValue < rhs.members.hashValue
	}
	return vn.hashValue < rhs.hashValue
}

type virtualNodeList []virtualNode

func (vnl virtualNodeList) Len() int {
	return len(vnl)
}

func (vnl virtualNodeList) Less(i, j int) bool {
	return vnl[i].less(vnl[j])
}

func (vnl virtualNodeList) Swap(i, j int) {
	temp := vnl[i]
	vnl[i] = vnl[j]
	vnl[j] = temp
}
