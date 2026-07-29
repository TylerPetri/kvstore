package raft

import (
	"sync"
	"time"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

type Node struct {
	mu sync.Mutex

	// identity
	id     NodeID
	config Config

	// persistent state
	currentTerm uint64
	votedFor    NodeID // empty string means null
	log         any    // we will define a raft.Log interface that matches what we already have

	// volatile state
	commitIndex uint64
	lastApplied uint64
	state       State

	// leader-only
	nextIndex  map[NodeID]uint64
	matchIndex map[NodeID]uint64

	// transport & timing
	transport Transport
	tickCh    chan struct{} // driven by a ticker in tests / main
	stopCh    chan struct{}

	// for applying committed entries later
	applyCh chan Entry
}

func NewNode(cfg Config, transport Transport, log any) *Node {
	n := &Node{
		id:         cfg.ID,
		config:     cfg,
		log:        log,
		state:      Follower,
		transport:  transport,
		nextIndex:  make(map[NodeID]uint64),
		matchIndex: make(map[NodeID]uint64),
		tickCh:     make(chan struct{}, 1),
		stopCh:     make(chan struct{}),
		applyCh:    make(chan Entry, 64),
	}
	return n
}

func (n *Node) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	// TODO
	return RequestVoteReply{Term: n.currentTerm, VoteGranted: false}
}

func (n *Node) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	// TODO
	return AppendEntriesReply{Term: n.currentTerm, Success: false}
}

// Start launches the background tick loop.
func (n *Node) Start() {
	go n.run()
}

func (n *Node) Stop() {
	close(n.stopCh)
}

func (n *Node) run() {
	ticker := time.NewTicker(50 * time.Millisecond) // base tick; we will randomize election
	defer ticker.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.mu.Lock()
			// election / heartbeat logic will go here
			n.mu.Unlock()
		}
	}
}
