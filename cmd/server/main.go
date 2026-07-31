package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/TylerPetri/kvstore/internal/raft"
	"github.com/TylerPetri/kvstore/internal/store"
)

// Single source of truth for the demo cluster.
var nodeAddrs = map[raft.NodeID]string{
	"n1": "http://localhost:8081",
	"n2": "http://localhost:8082",
	"n3": "http://localhost:8083",
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ----- Build peer list from nodeAddrs -----
	nodes := make(map[raft.NodeID]*raft.Node)
	engines := make(map[raft.NodeID]*store.Engine)
	var peers []raft.Peer
	for id, addr := range nodeAddrs {
		peers = append(peers, raft.Peer{ID: id, Addr: addr})
	}
	tr := raft.NewHTTPTransport(nodeAddrs)

	// ----- Create & start every Raft node + Engine -----
	for id := range nodeAddrs {
		cfg := raft.Config{
			ID:            id,
			Peers:         peers,
			ElectionTick:  10,
			HeartbeatTick: 3,
		}

		// Persistent storage per node
		dir := filepath.Join("data", string(id))
		storage, err := raft.NewFileStorage(dir)
		if err != nil {
			log.Fatalf("storage for %s: %v", id, err)
		}

		n, err := raft.NewNodeWithStorage(cfg, tr, storage)
		if err != nil {
			log.Fatalf("node %s: %v", id, err)
		}
		n.Start()
		nodes[id] = n

		eng := store.NewEngineWithRaft(4, n)
		eng.Start(ctx)
		eng.StartApplyLoop(ctx)
		engines[id] = eng
	}

	// ----- HTTP servers (KV API + Raft RPCs) -----
	servers := make([]*http.Server, 0, len(nodeAddrs))
	for id, fullAddr := range nodeAddrs {
		id := id // capture
		addr := portOnly(fullAddr)

		mux := http.NewServeMux()
		attachHandlers(mux, engines[id], nodes[id])

		srv := &http.Server{
			Addr:         addr,
			Handler:      mux,
			ReadTimeout:  5 * time.Second,
			WriteTimeout: 10 * time.Second,
			IdleTimeout:  60 * time.Second,
		}
		servers = append(servers, srv)

		go func(nid raft.NodeID, a string) {
			log.Printf("node %s listening on %s", nid, a)
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Printf("node %s: %v", nid, err)
			}
		}(id, addr)
	}

	// ----- Graceful shutdown -----
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("shutting down...")

	cancel()
	for _, n := range nodes {
		n.Stop()
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	for _, srv := range servers {
		_ = srv.Shutdown(shutdownCtx)
	}

	log.Println("bye")
}

// portOnly turns "http://localhost:8081" into ":8081"
func portOnly(full string) string {
	for i := len(full) - 1; i >= 0; i-- {
		if full[i] == ':' {
			return full[i:]
		}
	}
	return full
}

func attachHandlers(mux *http.ServeMux, eng *store.Engine, node *raft.Node) {
	// -------------------- KV API --------------------

	// POST /set  (with transparent proxy to leader)
	mux.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// ---- 1. Read body exactly once ----
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
			return
		}
		r.Body.Close()

		log.Printf("[%s] /set received %d bytes: %s", node.ID(), len(bodyBytes), string(bodyBytes))

		var body struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}
		if err := json.Unmarshal(bodyBytes, &body); err != nil {
			http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}
		if body.Key == "" {
			http.Error(w, "missing key", http.StatusBadRequest)
			return
		}

		cmd := store.Command{Op: "set", Key: body.Key, Value: body.Value}

		// ---- 2. Try local engine ----
		_, err = eng.Submit(r.Context(), cmd)
		if err == nil {
			log.Printf("[%s] local submit succeeded", node.ID())
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
			return
		}

		if err != raft.ErrNotLeader {
			log.Printf("[%s] local submit error: %v", node.ID(), err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// ---- 3. Real HTTP proxy to leader ----
		leaderID := node.LeaderID()
		log.Printf("[%s] not leader, current leader = %q", node.ID(), leaderID)

		if leaderID == "" || leaderID == node.ID() {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "not leader, leader unknown or self",
			})
			return
		}

		leaderAddr, ok := nodeAddrs[leaderID]
		if !ok {
			http.Error(w, "unknown leader address", http.StatusInternalServerError)
			return
		}

		proxyURL := leaderAddr + "/set"
		log.Printf("[%s] proxying to %s", node.ID(), proxyURL)

		proxyReq, err := http.NewRequestWithContext(
			r.Context(),
			http.MethodPost,
			proxyURL,
			bytes.NewReader(bodyBytes), // fresh reader every time
		)
		if err != nil {
			http.Error(w, "create proxy req: "+err.Error(), http.StatusInternalServerError)
			return
		}
		proxyReq.Header.Set("Content-Type", "application/json")
		proxyReq.ContentLength = int64(len(bodyBytes))

		client := &http.Client{Timeout: 3 * time.Second}
		proxyResp, err := client.Do(proxyReq)
		if err != nil {
			log.Printf("[%s] proxy transport error: %v", node.ID(), err)
			http.Error(w, "proxy failed: "+err.Error(), http.StatusBadGateway)
			return
		}
		defer proxyResp.Body.Close()

		respBody, _ := io.ReadAll(proxyResp.Body)
		log.Printf("[%s] proxy response %d: %s", node.ID(), proxyResp.StatusCode, string(respBody))

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(proxyResp.StatusCode)
		w.Write(respBody)
	})

	// GET /get?key=...
	mux.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "missing key", http.StatusBadRequest)
			return
		}

		resp, err := eng.Submit(r.Context(), store.Command{Op: "get", Key: key})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if !resp.OK {
			http.Error(w, "key not found", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"key":   key,
			"value": resp.Value,
		})
	})

	// GET /stats
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(eng.Stats())
	})

	// GET /raft  (debug)
	mux.HandleFunc("/raft", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id":          node.ID(),
			"state":       node.State().String(),
			"term":        node.Term(),
			"leader":      node.LeaderID(),
			"commitIndex": node.CommitIndex(),
			"lastApplied": node.LastApplied(),
		})
	})

	// GET /health
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})

	// -------------------- Raft RPCs --------------------

	mux.HandleFunc("/raft/requestvote", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var args raft.RequestVoteArgs
		if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		reply := node.HandleRequestVote(args)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(reply)
	})

	mux.HandleFunc("/raft/appendentries", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var args raft.AppendEntriesArgs
		if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		reply := node.HandleAppendEntries(args)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(reply)
	})
}
