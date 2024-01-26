package pbft_all

import (
	"DeFL/chain"
	"DeFL/consensus_shard/pbft_all/pbft_log"
	"DeFL/message"
	"DeFL/networks"
	"DeFL/params"
	"DeFL/shard"
	"bufio"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"io"
	"log"
	"net"
	"strconv"
	"sync"
)

type PbftNode struct {
	// the local config about pbft
	RunningNode *shard.Node // the node information
	ShardID     uint64      // denote the ID of the shard (or pbft), only one pbft consensus in a shard
	NodeID      uint64      // denote the ID of the node in the pbft (shard)

	// the data structure for blockchain
	CurChain *chain.BlockChain // all node in the shard maintain the same blockchain
	db       ethdb.Database    // to save the mpt

	// the global config about pbft
	pbftChainConfig *params.ChainConfig          // the chain config in this pbft
	ip_nodeTable    map[uint64]map[uint64]string // denote the ip of the specific node
	node_nums       uint64                       // the number of nodes in this pfbt, denoted by N
	malicious_nums  uint64                       // f, 3f + 1 = N
	view            uint64                       // denote the view of this pbft, the main node can be inferred from this variant

	// the control message and message checking utils in pbft
	sequenceID        uint64                          // the message sequence id of the pbft
	stop              bool                            // send stop signal
	pStop             chan uint64                     // channle for stopping consensus
	requestPool       map[string]*message.Request     // RequestHash to Request
	cntPrepareConfirm map[string]map[*shard.Node]bool // count the prepare confirm message, [messageHash][Node]bool
	cntCommitConfirm  map[string]map[*shard.Node]bool // count the commit confirm message, [messageHash][Node]bool
	isCommitBordcast  map[string]bool                 // denote whether the commit is broadcast
	isReply           map[string]bool                 // denote whether the message is reply
	height2Digest     map[uint64]string               // sequence (block height) -> request, fast read

	// locks about pbft
	sequenceLock sync.Mutex // the lock of sequence
	lock         sync.Mutex // lock the stage
	askForLock   sync.Mutex // lock for asking for a serise of requests
	stopLock     sync.Mutex // lock the stop varient

	// seqID of other Shards, to synchronize
	seqIDMap   map[uint64]uint64
	seqMapLock sync.Mutex

	// logger
	pl *pbft_log.PbftLog
	// tcp control
	tcpln       net.Listener
	tcpPoolLock sync.Mutex

	// to handle the message in the pbft
	ihm ExtraOpInConsensus

	// to handle the message outside of pbft
	ohm OpInterShards
}

func NewPbftNode(shardID, nodeID uint64, pcc *params.ChainConfig) *PbftNode {
	p := new(PbftNode)
	p.ip_nodeTable = params.IPmap_nodeTable
	p.node_nums = pcc.Nodes_perShard
	p.ShardID = shardID
	p.NodeID = nodeID
	p.pbftChainConfig = pcc
	fp := "./record/ldb/s" + strconv.FormatUint(shardID, 10) + "/n" + strconv.FormatUint(nodeID, 10)
	var err error
	p.db, err = rawdb.NewLevelDBDatabase(fp, 0, 1, "accountState", false)
	if err != nil {
		log.Panic(err)
	}
	p.CurChain, err = chain.NewBlockChain(pcc, p.db)
	if err != nil {
		log.Panic("cannot new a blockchain")
	}

	p.RunningNode = &shard.Node{
		NodeID:  nodeID,
		ShardID: shardID,
		IPaddr:  p.ip_nodeTable[shardID][nodeID],
	}

	p.stop = false
	p.sequenceID = p.CurChain.CurrentBlock.Header.Number + 1
	p.pStop = make(chan uint64)
	p.requestPool = make(map[string]*message.Request)
	p.cntPrepareConfirm = make(map[string]map[*shard.Node]bool)
	p.cntCommitConfirm = make(map[string]map[*shard.Node]bool)
	p.isCommitBordcast = make(map[string]bool)
	p.isReply = make(map[string]bool)
	p.height2Digest = make(map[uint64]string)
	p.malicious_nums = (p.node_nums - 1) / 3
	p.view = 0

	p.seqIDMap = make(map[uint64]uint64)

	p.pl = pbft_log.NewPbftLog(shardID, nodeID)

	// choose how to handle the messages in pbft or beyond pbft
	p.ihm = &RawRelayPbftExtraHandleMod{
		pbftNode: p,
	}
	p.ohm = &RawRelayOutsideModule{
		pbftNode: p,
	}
	return p
}

func (p *PbftNode) TcpListen() {
	ln, err := net.Listen("tcp", p.RunningNode.IPaddr)
	if err != nil {
		log.Panic(err)
	}
	p.tcpln = ln
	for {
		conn, err := p.tcpln.Accept()
		if err != nil {
			return
		}
		go p.handleClientRequest(conn)
	}
}

func (p *PbftNode) handleClientRequest(conn net.Conn) {
	defer conn.Close()
	clientReader := bufio.NewReader(conn) // create a new buffered reader for reading data received from the network connection.
	for {
		clientRequest, err := clientReader.ReadBytes('\n') //delimiter: 定界符; the method reads data from the buffer until it encounters the delimiter
		if p.getStopSignal() {
			return
		}
		switch err {
		case nil:
			p.tcpPoolLock.Lock()
			p.handleMessage(clientRequest)
			p.tcpPoolLock.Unlock()
		case io.EOF:
			log.Println("client closed the connection by terminating the process")
			return
		default:
			log.Printf("error: %v\n", err)
			return
		}
	}
}

func (p *PbftNode) getStopSignal() bool {
	p.stopLock.Lock()
	defer p.stopLock.Unlock()
	return p.stop
}

// handle the raw message, send it to corresponded interfaces
func (p *PbftNode) handleMessage(msg []byte) {
	msgType, content := message.SplitMessage(msg) // msgType is a phase of pbft protocol.
	switch msgType {
	// pbft inside message type
	case message.CPrePrepare:
		p.handlePrePrepare(content)
	case message.CPrepare:
		p.handlePrepare(content)
	case message.CCommit:
		p.handleCommit(content)
	case message.CRequestOldrequest:
		p.handleRequestOldSeq(content)
	case message.CSendOldrequest:
		p.handleSendOldSeq(content)
	case message.CStop:
		p.WaitToStop()

	// handle the message from outside
	default:
		p.ohm.HandleMessageOutsidePBFT(msgType, content)
	}
}

// when received stop
func (p *PbftNode) WaitToStop() {
	p.pl.Plog.Println("handling stop message")
	p.stopLock.Lock()
	p.stop = true
	p.stopLock.Unlock()
	if p.NodeID == p.view {
		p.pStop <- 1
	}
	networks.CloseAllConnInPool()
	p.tcpln.Close()
	p.closePbft()
	p.pl.Plog.Println("handled stop message")
}

// close the pbft
func (p *PbftNode) closePbft() {
	p.CurChain.CloseBlockChain()
}
