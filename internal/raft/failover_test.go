package raft

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLeaderFailure_InFlightProposals(t *testing.T) {
	c := newTestCluster(t)
	defer c.stopAll()

	// 1. Wait for initial leader
	leader := c.leader(t, 3*time.Second)
	oldID := leader.ID()
	t.Logf("initial leader = %s (term %d)", oldID, leader.Term())

	// 2. Start several in-flight proposals on the leader
	const nProposals = 12
	var (
		wg          sync.WaitGroup
		successes   atomic.Int32
		notLeader   atomic.Int32
		otherErrors atomic.Int32
	)

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()

	wg.Add(nProposals)
	for i := 0; i < nProposals; i++ {
		go func(i int) {
			defer wg.Done()
			// Stagger them a little so some are mid-replication when we kill
			time.Sleep(time.Duration(i) * 8 * time.Millisecond)

			_, err := leader.Propose(ctx, map[string]any{
				"op":  "set",
				"key": "k",
				"i":   i,
			})
			switch {
			case err == nil:
				successes.Add(1)
			case err == ErrNotLeader:
				notLeader.Add(1)
			default:
				otherErrors.Add(1)
				t.Logf("proposal %d unexpected error: %v", i, err)
			}
		}(i)
	}

	// 3. Give the proposals a moment to start, then kill the leader
	time.Sleep(40 * time.Millisecond)
	t.Logf("killing leader %s", oldID)
	c.stopNode(oldID)

	// 4. Wait for all in-flight work to finish
	wg.Wait()

	t.Logf("results: success=%d  notLeader=%d  other=%d",
		successes.Load(), notLeader.Load(), otherErrors.Load())

	// At least some proposals should have failed with ErrNotLeader
	// (the exact number depends on timing, but zero would be suspicious)
	if notLeader.Load() == 0 && successes.Load() == nProposals {
		t.Log("warning: every proposal succeeded – leader may have applied everything before death")
	}
	if otherErrors.Load() > 0 {
		t.Errorf("unexpected errors: %d", otherErrors.Load())
	}

	// 5. A new leader must appear among the remaining nodes
	newLeader := c.leader(t, 3*time.Second)
	if newLeader.ID() == oldID {
		t.Fatal("old leader came back; it should have been removed")
	}
	t.Logf("new leader = %s (term %d)", newLeader.ID(), newLeader.Term())

	// 6. New leader must be able to accept a write
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	idx, err := newLeader.Propose(ctx2, "after-failover")
	if err != nil {
		t.Fatalf("propose on new leader: %v", err)
	}
	t.Logf("new leader proposed at index %d", idx)

	// 7. Optional: check that commit/apply advanced on the new leader
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if newLeader.CommitIndex() >= idx && newLeader.LastApplied() >= idx {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if newLeader.LastApplied() < idx {
		t.Errorf("new leader did not apply the post-failover entry (applied=%d, want >=%d)",
			newLeader.LastApplied(), idx)
	}
}
