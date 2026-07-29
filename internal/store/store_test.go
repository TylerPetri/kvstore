package store

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestBasicSetGet(t *testing.T) {
	e := NewEngine(4)
	ctx, cancel := context.WithCancel(context.Background())
	e.Start(ctx)

	// defer executes last in, first out
	// must one-line
	defer func() {
		cancel()
		e.Stop()
	}()

	_, err := e.Submit(ctx, Command{Op: "set", Key: "foo", Value: "bar"})
	if err != nil {
		t.Fatal(err)
	}

	resp, err := e.Submit(ctx, Command{Op: "get", Key: "foo"})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.OK || resp.Value != "bar" {
		t.Fatalf("got %+v", resp)
	}
}

func TestConcurrent(t *testing.T) {
	e := NewEngine(8)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	e.Start(ctx)

	defer func() {
		cancel()
		e.Stop()
	}()

	const n = 1000
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			key := "k" + string(rune(i%100))
			_, _ = e.Submit(ctx, Command{Op: "set", Key: key, Value: "v"})
		}(i)
	}
	wg.Wait()
}

func TestMemoryLog(t *testing.T) {
	l := NewMemoryLog()

	if l.LastIndex() != 0 {
		t.Fatalf("expected empty log")
	}

	idx, err := l.Append(Entry{Cmd: Command{Op: "set", Key: "a", Value: "1"}})
	if err != nil || idx != 1 {
		t.Fatalf("append 1: idx=%d err=%v", idx, err)
	}
	idx, err = l.Append(Entry{Cmd: Command{Op: "set", Key: "b", Value: "2"}})
	if err != nil || idx != 2 {
		t.Fatalf("append 2: idx=%d err=%v", idx, err)
	}

	e, ok := l.At(1)
	if !ok || e.Cmd.Key != "a" {
		t.Fatalf("At(1) =%+v", err)
	}

	if err := l.Truncate(2); err != nil {
		t.Fatal(err)
	}
	if l.LastIndex() != 1 {
		t.Fatalf("after truncate last index = %d", l.LastIndex())
	}
}
