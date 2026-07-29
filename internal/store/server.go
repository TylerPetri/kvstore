package store

import (
	"context"
	"sync"
	"sync/atomic"
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
			val, ok := e.store.Apply(req.Cmd)

			switch req.Cmd.Op {
			case "set":
				e.sets.Add(1)
			case "get":
				e.gets.Add(1)
			case "delete":
				e.deletes.Add(1)
			}

			req.Response <- Response{Value: val, OK: ok}
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
