package raft

import (
	"testing"
	"time"
)

func newTestCluster(t *testing.T) ([]*Node, *InProcessTransport) {
	t.Helper()

	ids := []NodeID{"n1", "n2", "n3"}
	peers := []Peer{
		{ID: "n1"},
		{ID: "n2"},
		{ID: "n3"},
	}

	tr := NewInProcessTransport()
	nodes := make([]*Node, 0, 3)

	for _, id := range ids {
		cfg := Config{
			ID:            id,
			Peers:         peers,
			ElectionTick:  10, // relatively fast for tests
			HeartbeatTick: 3,
		}
		log := NewMemoryLog()
		n := NewNode(cfg, tr, log)
		tr.Register(n)
		nodes = append(nodes, n)
	}

	for _, n := range nodes {
		n.Start()
	}
	return nodes, tr
}

func TestElection(t *testing.T) {
	nodes, _ := newTestCluster(t)
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
	}()

	// Wait until a leader appears
	deadline := time.Now().Add(3 * time.Second)
	var leader *Node
	for time.Now().Before(deadline) {
		leaders := 0
		for _, n := range nodes {
			if n.State() == Leader {
				leaders++
				leader = n
			}
		}
		if leaders == 1 {
			break
		}
		if leaders > 1 {
			t.Fatalf("more than one leader")
		}
		time.Sleep(20 * time.Millisecond)
	}

	if leader == nil {
		t.Fatal("no leader elected")
	}

	t.Logf("leader=%s term=%d", leader.ID(), leader.Term())

	// Leader should stay stable for a while
	time.Sleep(500 * time.Millisecond)

	stillLeader := 0
	for _, n := range nodes {
		if n.State() == Leader {
			stillLeader++
			if n.ID() != leader.ID() {
				t.Fatalf("leader changed unexpectedly")
			}
		}
	}
	if stillLeader != 1 {
		t.Fatalf("expected 1 leader, got %d", stillLeader)
	}
}
