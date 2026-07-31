package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type HTTPTransport struct {
	client *http.Client
	addrs  map[NodeID]string
}

func NewHTTPTransport(addrs map[NodeID]string) *HTTPTransport {
	return &HTTPTransport{
		client: &http.Client{Timeout: 2 * time.Second},
		addrs:  addrs,
	}
}

func (t *HTTPTransport) SendRequestVote(ctx context.Context, to NodeID, args RequestVoteArgs) (RequestVoteReply, error) {
	var reply RequestVoteReply
	err := t.post(ctx, to, "/raft/requestvote", args, &reply)
	return reply, err
}

func (t *HTTPTransport) SendAppendEntries(ctx context.Context, to NodeID, args AppendEntriesArgs) (AppendEntriesReply, error) {
	var reply AppendEntriesReply
	err := t.post(ctx, to, "/raft/appendentries", args, &reply)
	return reply, err
}

func (t *HTTPTransport) post(ctx context.Context, to NodeID, path string, in, out any) error {
	addr, ok := t.addrs[to]
	if !ok {
		return fmt.Errorf("unknown node %s", to)
	}
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, addr+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := t.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
