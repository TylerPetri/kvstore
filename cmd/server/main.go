package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/TylerPetri/kvstore/internal/raft"
	"github.com/TylerPetri/kvstore/internal/store"
)

var nodeAddrs = map[raft.NodeID]string{
	"n1": "http://localhost:8081",
	"n2": "http://localhost:8082",
	"n3": "http://localhost:8083",
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ----- Build peer list and port list from nodeAddrs -----
	var peers []raft.Peer
	for id, addr := range nodeAddrs {
		peers = append(peers, raft.Peer{
			ID:   id,
			Addr: addr, // stored for later real-network transport
		})
	}

	tr := raft.NewInProcessTransport()

	nodes := make(map[raft.NodeID]*raft.Node)
	engines := make(map[raft.NodeID]*store.Engine)

	// ----- Create & start every Raft node + Engine -----
	for id := range nodeAddrs {
		cfg := raft.Config{
			ID:            id,
			Peers:         peers,
			ElectionTick:  10,
			HeartbeatTick: 3,
		}
		rlog := raft.NewMemoryLog()
		n := raft.NewNode(cfg, tr, rlog)
		tr.Register(n)
		n.Start()
		nodes[id] = n

		eng := store.NewEngineWithRaft(4, n)
		eng.Start(ctx)
		eng.StartApplyLoop(ctx)
		engines[id] = eng
	}

	// ----- HTTP servers (one per node) -----
	servers := make([]*http.Server, 0, len(nodeAddrs))
	for id, fullAddr := range nodeAddrs {
		id := id                   // capture
		addr := portOnly(fullAddr) // ":8081", ":8082", ...

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
	// ---------- /set (with transparent proxy) ----------
	mux.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var body struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		if body.Key == "" {
			http.Error(w, "missing key", http.StatusBadRequest)
			return
		}

		cmd := store.Command{Op: "set", Key: body.Key, Value: body.Value}

		// Try locally first
		_, err := eng.Submit(r.Context(), cmd)
		if err == nil {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
			return
		}

		if err != raft.ErrNotLeader {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Not leader → proxy or redirect
		leaderID := node.LeaderID()
		if leaderID == "" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "not leader, leader unknown",
			})
			return
		}

		leaderAddr, ok := nodeAddrs[leaderID]
		if !ok {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{
				"error":  "not leader",
				"leader": string(leaderID),
			})
			return
		}

		// Transparent proxy
		proxyReq, err := http.NewRequestWithContext(
			r.Context(),
			http.MethodPost,
			leaderAddr+"/set",
			r.Body,
		)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		proxyReq.Header.Set("Content-Type", "application/json")

		proxyResp, err := http.DefaultClient.Do(proxyReq)
		if err != nil {
			http.Error(w, "proxy to leader failed: "+err.Error(), http.StatusBadGateway)
			return
		}
		defer proxyResp.Body.Close()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(proxyResp.StatusCode)
		io.Copy(w, proxyResp.Body)
	})

	// ---------- /get ----------
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

	// ---------- /stats ----------
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(eng.Stats())
	})

	// ---------- /raft (debug) ----------
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

	// ---------- /health ----------
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})
}
