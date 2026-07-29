package raft

import (
	"context"
	"fmt"
	"sync"
)

type InProcessTransport struct {
	mu    sync.RWMutex
	nodes map[NodeID]*Node
}

func NewInProcessTransport() *InProcessTransport {
	return &InProcessTransport{
		nodes: make(map[NodeID]*Node),
	}
}

func (t *InProcessTransport) Register(n *Node) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nodes[n.id] = n
}

func (t *InProcessTransport) SendRequestVote(ctx context.Context, to NodeID, args RequestVoteArgs) (RequestVoteReply, error) {
	t.mu.RLock()
	node, ok := t.nodes[to]
	t.mu.RUnlock()
	if !ok {
		return RequestVoteReply{}, fmt.Errorf("unknown node %s", to)
	}
	return node.HandleRequestVote(args), nil
}

func (t *InProcessTransport) SendAppendEntries(ctx context.Context, to NodeID, args AppendEntriesArgs) (AppendEntriesReply, error) {
	t.mu.RLock()
	node, ok := t.nodes[to]
	t.mu.RUnlock()
	if !ok {
		return AppendEntriesReply{}, fmt.Errorf("unknown node %s", to)
	}
	return node.HandleAppendEntries(args), nil
}
