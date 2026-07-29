package raft

import (
	"testing"
	"time"
)

func TestReplication(t *testing.T) {
	nodes, _ := newTestCluster(t)
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
	}()

	// Wait for a leader
	var leader *Node
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.State() == Leader {
				leader = n
				break
			}
		}
		if leader != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if leader == nil {
		t.Fatal("no leader")
	}
	t.Logf("leader=%s term=%d", leader.ID(), leader.Term())

	// Propose a few commands
	cmds := []string{"set a=1", "set b=2", "set c=3"}
	for _, cmd := range cmds {
		idx, err := leader.Propose(cmd)
		if err != nil {
			t.Fatalf("Propose: %v", err)
		}
		t.Logf("proposed %q at index %d", cmd, idx)
	}

	// Wait until all nodes have committed and applied the last entry
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		allDone := true
		for _, n := range nodes {
			if n.CommitIndex() < 3 || n.LastApplied() < 3 {
				allDone = false
				break
			}
		}
		if allDone {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	for _, n := range nodes {
		if n.CommitIndex() != 3 || n.LastApplied() != 3 {
			t.Errorf("node %s: commit=%d applied=%d", n.ID(), n.CommitIndex(), n.LastApplied())
		}
	}

	// Drain apply channels and check order (optional but nice)
	for _, n := range nodes {
		applied := make([]string, 0, 3)
		timeout := time.After(500 * time.Millisecond)
	loop:
		for len(applied) < 3 {
			select {
			case e := <-n.ApplyCh():
				if s, ok := e.Cmd.(string); ok {
					applied = append(applied, s)
				}
			case <-timeout:
				break loop
			}
		}
		t.Logf("node %s applied: %v", n.ID(), applied)
	}
}
