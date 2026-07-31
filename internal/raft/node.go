package raft

import (
	"context"
	"fmt"
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
	storage     Storage

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

	pendingMu sync.Mutex
	pending   map[uint64]chan struct{}

	leaderLost chan struct{}

	currentLeader NodeID
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
		pending:    make(map[uint64]chan struct{}),
		leaderLost: make(chan struct{}),
	}
	n.resetElectionTimeout()
	return n
}

func NewNodeWithStorage(cfg Config, transport Transport, storage Storage) (*Node, error) {
	term, votedFor, err := storage.State()
	if err != nil {
		return nil, err
	}
	entries, err := storage.Log()
	if err != nil {
		return nil, err
	}

	log := NewMemoryLog()
	if len(log.entries) > 1 {
		_, _ = log.Append(entries[1:]...)
	}

	n := NewNode(cfg, transport, log)
	n.currentTerm = term
	n.votedFor = votedFor
	n.storage = storage // add field: storage Storage
	return n, nil
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
			ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
			defer cancel()
			reply, err := n.transport.SendRequestVote(ctx, to, args)
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

	if n.storage != nil {
		_ = n.storage.SaveState(n.currentTerm, n.votedFor)
	}

	last := n.log.LastIndex()
	for _, peer := range n.config.Peers {
		n.nextIndex[peer.ID] = last + 1
		n.matchIndex[peer.ID] = 0
	}

	// send initial empty heartbeat immediately
	n.broadcastHeartbeat()
}

func (n *Node) becomeFollower(term uint64) {
	wasLeader := n.state == Leader
	n.state = Follower
	n.currentTerm = term
	n.votedFor = ""
	n.currentLeader = ""
	n.resetElectionTimeout()

	if n.storage != nil {
		_ = n.storage.SaveState(n.currentTerm, n.votedFor)
	}

	if wasLeader {
		n.notifyLeaderLost()
	}
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
	if args.Term > n.currentTerm || n.state != Follower {
		n.becomeFollower(args.Term)
	}

	// Reset election timer – we heard from a legitimate leader
	n.currentLeader = args.LeaderID
	n.resetElectionTimeout()

	// Consistency check
	if args.PrevLogIndex > 0 {
		e, ok := n.log.At(args.PrevLogIndex)
		if !ok || e.Term != args.PrevLogTerm {
			return reply // reject → leader will back off nextIndex
		}
	}

	// Append new entries (truncate any conflict first)
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + uint64(i) + 1
		existing, ok := n.log.At(idx)
		if ok && existing.Term != entry.Term {
			_ = n.log.Truncate(idx) // remove conflicting suffix
			ok = false
		}
		if !ok {
			_, _ = n.log.Append(entry)
		}
	}

	// Advance commitIndex
	if args.LeaderCommit > n.commitIndex {
		last := n.log.LastIndex()
		if args.LeaderCommit < last {
			n.commitIndex = args.LeaderCommit
		} else {
			n.commitIndex = last
		}
		n.applyCommitted()
	}

	if n.storage != nil {
		// TODO: less naive; this works for now
		entries := make([]Entry, 0, n.log.LastIndex()+1)
		entries = append(entries, Entry{}) // index 0
		for i := uint64(1); i <= n.log.LastIndex(); i++ {
			if e, ok := n.log.At(1); ok {
				entries = append(entries, e)
			}
		}
		_ = n.storage.SaveLog(entries)
	}

	reply.Success = true
	reply.Term = n.currentTerm
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
			ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
			defer cancel()
			reply, err := n.transport.SendAppendEntries(ctx, to, args)
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

// ----------- Propose and Replication Logic ----------
// Propose is called by an upper layer (later engine)
// Only the leader can accept
// For now: optimistic accept of proposal, later will wait for commit
func (n *Node) Propose(ctx context.Context, cmd any) (index uint64, err error) {
	n.mu.Lock()

	if n.state != Leader {
		n.mu.Unlock()
		return 0, ErrNotLeader
	}

	entry := Entry{
		Term: n.currentTerm,
		Cmd:  cmd,
	}
	index, err = n.log.Append(entry)
	if err != nil {
		n.mu.Unlock()
		return 0, err
	}

	// Register a waiter before we unlock
	waitCh := make(chan struct{})
	n.pendingMu.Lock()
	n.pending[index] = waitCh
	lostCh := n.leaderLost
	n.pendingMu.Unlock()

	// Kick replication
	n.broadcastAppendEntries()
	n.mu.Unlock()

	// Wait for apply (or cancellation / leadership loss)
	select {
	case <-ctx.Done():
		n.cleanupPending(index)
		return 0, ctx.Err()
	case <-waitCh:
		return index, nil
	case <-lostCh:
		n.cleanupPending(index)
		return 0, ErrNotLeader
	}
}

func (n *Node) cleanupPending(index uint64) {
	n.pendingMu.Lock()
	defer n.pendingMu.Unlock()
	if ch, ok := n.pending[index]; ok {
		delete(n.pending, index)
		// do not close - the apply side my still try to close it
		_ = ch
	}
}

func (n *Node) notifyLeaderLost() {
	n.pendingMu.Lock()
	defer n.pendingMu.Unlock()

	close(n.leaderLost)
	n.leaderLost = make(chan struct{})

	for idx, ch := range n.pending {
		close(ch)
		delete(n.pending, idx)
	}
}

var ErrNotLeader = fmt.Errorf("not leader")

// broadcastAppendEntries sends entries or heartbeats to peers
func (n *Node) broadcastAppendEntries() {
	for _, peer := range n.config.Peers {
		if peer.ID == n.id {
			continue
		}
		n.sendAppendEntries(peer.ID)
	}
}

func (n *Node) sendAppendEntries(to NodeID) {
	next := n.nextIndex[to]
	prevIndex := next - 1
	prevTerm := uint64(0)
	if prevIndex > 0 {
		if e, ok := n.log.At(prevIndex); ok {
			prevTerm = e.Term
		}
	}

	// Collect entries starting at nextIndex
	var entries []Entry
	last := n.log.LastIndex()
	for i := next; i <= last; i++ {
		if e, ok := n.log.At(i); ok {
			entries = append(entries, e)
		}
	}

	args := AppendEntriesArgs{
		Term:         n.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
		defer cancel()
		reply, err := n.transport.SendAppendEntries(ctx, to, args)
		if err != nil {
			return
		}
		n.mu.Lock()
		defer n.mu.Unlock()

		if reply.Term > n.currentTerm {
			n.becomeFollower(reply.Term)
			return
		}
		if n.state != Leader || n.currentTerm != args.Term {
			return
		}

		if reply.Success {
			// Update match & next
			n.matchIndex[to] = args.PrevLogIndex + uint64(len(args.Entries))
			n.nextIndex[to] = n.matchIndex[to] + 1
			n.advanceCommitIndex()
		} else {
			// Simple backoff (we can make this smarter later)
			if n.nextIndex[to] > 1 {
				n.nextIndex[to]--
			}
		}
	}()
}

// advanceCommitIndex looks for the highest index that is replicated
// on a majority and has the current term (Raft safety rule).
func (n *Node) advanceCommitIndex() {
	last := n.log.LastIndex()
	for i := n.commitIndex + 1; i <= last; i++ {
		e, ok := n.log.At(i)
		if !ok || e.Term != n.currentTerm {
			continue // only commit current-term entries directly
		}

		count := 1 // self
		for _, peer := range n.config.Peers {
			if peer.ID == n.id {
				continue
			}
			if n.matchIndex[peer.ID] >= i {
				count++
			}
		}
		if count >= n.config.Majority() {
			n.commitIndex = i
		}
	}

	// Apply newly committed entries
	n.applyCommitted()
}

func (n *Node) applyCommitted() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		e, ok := n.log.At(n.lastApplied)
		if !ok {
			continue
		}

		n.pendingMu.Lock()
		if ch, exists := n.pending[n.lastApplied]; exists {
			close(ch)
			delete(n.pending, n.lastApplied)
		}
		n.pendingMu.Unlock()

		select {
		case n.applyCh <- e:
		default:
			// drop if full – tests can make the channel larger if needed
		}
	}
}

func (n *Node) ApplyCh() <-chan Entry {
	return n.applyCh
}

func (n *Node) CommitIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}

func (n *Node) LastApplied() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastApplied
}

func (n *Node) LeaderID() NodeID {
	n.mu.Lock()
	defer n.mu.Unlock()

	switch n.state {
	case Leader:
		return n.id
	case Follower, Candidate:
		return n.currentLeader
	default:
		return ""
	}
}
