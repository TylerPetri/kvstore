package store

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/TylerPetri/kvstore/internal/raft"
)

type Request struct {
	Cmd      Command
	Response chan Response
}

type Response struct {
	Value string
	OK    bool
	Err   error
}

type Engine struct {
	store   *Store
	log     Log
	raft    *raft.Node // nil means single-node mode
	reqCh   chan Request
	workers int
	wg      sync.WaitGroup

	sets    atomic.Uint64
	gets    atomic.Uint64
	deletes atomic.Uint64
}

func NewEngine(workers int) *Engine {
	e := &Engine{
		store:   New(),
		reqCh:   make(chan Request, 1024),
		workers: workers,
	}
	return e
}

func NewEngineWithLog(workers int, log Log) *Engine {
	e := NewEngine(workers)
	e.log = log
	return e
}

func NewEngineWithRaft(workers int, n *raft.Node) *Engine {
	e := NewEngine(workers)
	e.raft = n
	return e
}

func (e *Engine) Start(ctx context.Context) {
	for i := 0; i < e.workers; i++ {
		e.wg.Add(1)
		go e.worker(ctx)
	}
}

func (e *Engine) worker(ctx context.Context) {
	defer e.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case req, ok := <-e.reqCh:
			if !ok {
				return
			}

			var resp Response

			switch {
			case e.raft != nil && (req.Cmd.Op == "set" || req.Cmd.Op == "delete"):
				_, err := e.raft.Propose(req.Cmd)
				if err != nil {
					resp.Err = err
				} else {
					resp.OK = true
					if req.Cmd.Op == "set" {
						e.sets.Add(1)
					} else {
						e.deletes.Add(1)
					}
				}

			case e.log != nil:
				// legacy memory-log path
				_, err := e.log.Append(Entry{Cmd: req.Cmd})
				if err != nil {
					resp.Err = err
				} else {
					val, ok := e.store.Apply(req.Cmd)
					resp.Value = val
					resp.OK = ok
				}

			default:
				// pure in-memory or read path
				val, ok := e.store.Apply(req.Cmd)
				resp.Value = val
				resp.OK = ok
				if req.Cmd.Op == "get" {
					e.gets.Add(1)
				}
			}

			req.Response <- resp
		}
	}
}

func (e *Engine) Submit(ctx context.Context, cmd Command) (Response, error) {
	respCh := make(chan Response, 1)
	req := Request{Cmd: cmd, Response: respCh}

	select {
	case <-ctx.Done():
		return Response{}, ctx.Err()
	case e.reqCh <- req:
	}

	select {
	case <-ctx.Done():
		return Response{}, ctx.Err()
	case resp := <-respCh:
		if resp.Err != nil {
			return resp, resp.Err
		}
		return resp, nil
	}
}

func (e *Engine) Stop() {
	e.wg.Wait()
}

func (e *Engine) Len() int {
	return e.store.Len()
}

type Stats struct {
	Keys    int    `json:"keys"`
	Sets    uint64 `json:"sets"`
	Gets    uint64 `json:"gets"`
	Deletes uint64 `json:"deletes"`
}

func (e *Engine) Stats() Stats {
	return Stats{
		Keys:    e.Len(),
		Sets:    e.sets.Load(),
		Gets:    e.gets.Load(),
		Deletes: e.deletes.Load(),
	}
}

func (e *Engine) StartApplyLoop(ctx context.Context) {
	if e.raft == nil {
		return
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case entry := <-e.raft.ApplyCh():
				if cmd, ok := entry.Cmd.(Command); ok {
					e.store.Apply(cmd)
				}
			}
		}
	}()
}
