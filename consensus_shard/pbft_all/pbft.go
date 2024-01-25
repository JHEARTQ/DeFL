package pbft_all

import "DeFL/params"

type PbftNode struct {
}

func NewPbftNode(shardID, nodeID uint64, pcc *params.ChainConfig) *PbftNode {
	p := new(PbftNode)
	return p
}
