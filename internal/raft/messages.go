package raft

// ----------- Request Vote -----------
type RequestVoteArgs struct {
	Term          uint64
	CandidateID   NodeID
	LastVoteIndex uint64
	LastVoteTerm  uint64
}

type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

// ------------ Append Entries -----------
type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     NodeID
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term    uint64
	Success bool
}

type Entry struct {
	Index uint64
	Term  uint64
	Cmd   string
}
