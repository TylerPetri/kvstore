package raft

import (
	"testing"
	"time"
)

type testCluster struct {
	nodes     map[NodeID]*Node
	transport *InProcessTransport
}

func newTestCluster(t *testing.T, ids ...NodeID) *testCluster {
	t.Helper()
	if len(ids) == 0 {
		ids = []NodeID{"n1", "n2", "n3"}
	}

	peers := make([]Peer, 0, len(ids))
	for _, id := range ids {
		peers = append(peers, Peer{ID: id})
	}

	tr := NewInProcessTransport()
	nodes := make(map[NodeID]*Node, len(ids))

	for _, id := range ids {
		cfg := Config{
			ID:            id,
			Peers:         peers,
			ElectionTick:  8,
			HeartbeatTick: 3,
		}
		n := NewNode(cfg, tr, NewMemoryLog())
		tr.Register(n)
		nodes[id] = n
	}

	for _, n := range nodes {
		n.Start()
	}

	return &testCluster{nodes: nodes, transport: tr}
}

func (c *testCluster) stopAll() {
	for _, n := range c.nodes {
		n.Stop()
	}
}

func (c *testCluster) leader(t *testing.T, timeout time.Duration) *Node {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaders []*Node
		for _, n := range c.nodes {
			if n.State() == Leader {
				leaders = append(leaders, n)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		if len(leaders) > 1 {
			t.Fatalf("multiple leaders: %v", leaders)
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatal("no leader elected")
	return nil
}

func (c *testCluster) stopNode(id NodeID) {
	if n, ok := c.nodes[id]; ok {
		n.Stop()
		delete(c.nodes, id) // so future leader() calls ignore it
	}
}
