package raft

import "context"

type Transport interface {
	SendRequestVote(ctx context.Context, to NodeID, args RequestVoteArgs) (RequestVoteReply, error)
	SendAppendEntries(ctx context.Context, to NodeID, arg AppendEntriesArgs) (AppendEntriesReply, error)
}
