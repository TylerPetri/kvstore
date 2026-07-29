package raft

import (
	"math/rand"
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

	id     NodeID
	config Config

	currentTerm uint64
	votedFor    NodeID
	log         Log

	commitIndex uint64
	lastApplied uint64
	state       State

	nextIndex  map[NodeID]uint64
	matchIndex map[NodeID]uint64

	transport Transport

	// election / heartbeat timing
	electionElapsed           int
	heartbeatElapsed          int
	randomizedElectionTimeout int

	stopCh  chan struct{}
	applyCh chan Entry // unused until replication
}

func NewNode(cfg Config, transport Transport, log Log) *Node {
	n := &Node{
		id:         cfg.ID,
		config:     cfg,
		log:        log,
		state:      Follower,
		transport:  transport,
		nextIndex:  make(map[NodeID]uint64),
		matchIndex: make(map[NodeID]uint64),
		stopCh:     make(chan struct{}),
		applyCh:    make(chan Entry, 64),
	}
	n.resetElectionTimeout()
	return n
}

func (n *Node) resetElectionTimeout() {
	n.randomizedElectionTimeout = n.config.ElectionTick + rand.Intn(n.config.ElectionTick)
	n.electionElapsed = 0
}

// Start launches the background tick loop.
func (n *Node) Start() {
	go n.run()
}

func (n *Node) Stop() {
	close(n.stopCh)
}

func (n *Node) run() {
	ticker := time.NewTicker(20 * time.Millisecond) // base tick unit
	defer ticker.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.tick()
		}
	}
}

func (n *Node) tick() {
	n.mu.Lock()
	defer n.mu.Unlock()

	switch n.state {
	case Follower, Candidate:
		n.electionElapsed++
		if n.electionElapsed >= n.randomizedElectionTimeout {
			n.startElection()
		}
	case Leader:
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.config.HeartbeatTick {
			n.broadcastHeartbeat()
			n.heartbeatElapsed = 0
		}
	}
}

// ---------- Election ----------

func (n *Node) startElection() {
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.electionElapsed = 0
	n.resetElectionTimeout()

	term := n.currentTerm
	lastIndex := n.log.LastIndex()
	lastTerm := n.log.LastTerm()

	votes := 1 // vote for self
	var mu sync.Mutex

	for _, peer := range n.config.Peers {
		if peer.ID == n.id {
			continue
		}
		go func(to NodeID) {
			args := RequestVoteArgs{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			}
			reply, err := n.transport.SendRequestVote(nil, to, args)
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			// ignore replies from old terms
			if reply.Term > n.currentTerm {
				n.becomeFollower(reply.Term)
				return
			}
			if n.state != Candidate || n.currentTerm != term {
				return
			}
			if reply.VoteGranted {
				mu.Lock()
				votes++
				if votes >= n.config.Majority() {
					n.becomeLeader()
				}
				mu.Unlock()
			}
		}(peer.ID)
	}
}

func (n *Node) becomeLeader() {
	n.state = Leader
	n.heartbeatElapsed = 0

	last := n.log.LastIndex()
	for _, peer := range n.config.Peers {
		n.nextIndex[peer.ID] = last + 1
		n.matchIndex[peer.ID] = 0
	}

	// send initial empty heartbeat immediately
	n.broadcastHeartbeat()
}

func (n *Node) becomeFollower(term uint64) {
	n.state = Follower
	n.currentTerm = term
	n.votedFor = ""
	n.resetElectionTimeout()
}

// ---------- RPCs ----------

func (n *Node) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := RequestVoteReply{Term: n.currentTerm, VoteGranted: false}

	if args.Term < n.currentTerm {
		return reply
	}
	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
	}

	// already voted for someone else in this term
	if n.votedFor != "" && n.votedFor != args.CandidateID {
		return reply
	}

	// log must be at least as up-to-date as ours
	lastIndex := n.log.LastIndex()
	lastTerm := n.log.LastTerm()
	upToDate := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)

	if upToDate {
		n.votedFor = args.CandidateID
		n.resetElectionTimeout() // grant of vote resets timer
		reply.VoteGranted = true
		reply.Term = n.currentTerm
	}
	return reply
}

func (n *Node) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := AppendEntriesReply{Term: n.currentTerm, Success: false}

	if args.Term < n.currentTerm {
		return reply
	}
	if args.Term > n.currentTerm || n.state == Candidate {
		n.becomeFollower(args.Term)
	}

	// This is a heartbeat (or will become real replication later).
	// For now we just accept it and reset the election timer.
	n.resetElectionTimeout()
	reply.Success = true
	reply.Term = n.currentTerm

	// LeaderCommit handling will come with real replication
	return reply
}

func (n *Node) broadcastHeartbeat() {
	term := n.currentTerm
	commit := n.commitIndex
	lastIndex := n.log.LastIndex()
	lastTerm := n.log.LastTerm()

	for _, peer := range n.config.Peers {
		if peer.ID == n.id {
			continue
		}
		go func(to NodeID) {
			args := AppendEntriesArgs{
				Term:         term,
				LeaderID:     n.id,
				PrevLogIndex: lastIndex,
				PrevLogTerm:  lastTerm,
				Entries:      nil, // heartbeat
				LeaderCommit: commit,
			}
			reply, err := n.transport.SendAppendEntries(nil, to, args)
			if err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if reply.Term > n.currentTerm {
				n.becomeFollower(reply.Term)
			}
		}(peer.ID)
	}
}

// ---------- Test helpers ----------

func (n *Node) State() State {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state
}

func (n *Node) Term() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm
}

func (n *Node) ID() NodeID {
	return n.id
}
