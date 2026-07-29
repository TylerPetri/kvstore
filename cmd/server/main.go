package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/TylerPetri/kvstore/internal/raft"
	"github.com/TylerPetri/kvstore/internal/store"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ----- 3-node Raft cluster (in-process) -----
	ids := []raft.NodeID{"n1", "n2", "n3"}
	peers := []raft.Peer{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}
	tr := raft.NewInProcessTransport()

	nodes := make([]*raft.Node, 0, 3)
	engines := make([]*store.Engine, 0, 3)

	for _, id := range ids {
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
		nodes = append(nodes, n)

		eng := store.NewEngineWithRaft(4, n)
		eng.Start(ctx)
		eng.StartApplyLoop(ctx)
		engines = append(engines, eng)
	}

	// ----- HTTP on three ports -----
	ports := []string{":8081", ":8082", ":8083"}
	for i := range nodes {
		i := i // capture
		mux := http.NewServeMux()
		attachHandlers(mux, engines[i], nodes[i])
		srv := &http.Server{Addr: ports[i], Handler: mux}

		go func(id raft.NodeID, addr string) {
			log.Printf("node %s listening on %s", id, addr)
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Printf("node %s: %v", id, err)
			}
		}(nodes[i].ID(), ports[i])
	}

	// graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("shutting down...")
	cancel()
	for _, n := range nodes {
		n.Stop()
	}
	time.Sleep(200 * time.Millisecond)
	log.Println("bye")
}

func attachHandlers(mux *http.ServeMux, eng *store.Engine, node *raft.Node) {
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

		resp, err := eng.Submit(r.Context(), store.Command{
			Op:    "set",
			Key:   body.Key,
			Value: body.Value,
		})
		if err != nil {
			if err == raft.ErrNotLeader {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusServiceUnavailable)
				json.NewEncoder(w).Encode(map[string]string{
					"error":  "not leader",
					"leader": string(findLeader(node)), // crude helper, see below
				})
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		_ = resp
	})

	mux.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
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
		json.NewEncoder(w).Encode(map[string]string{"key": key, "value": resp.Value})
	})

	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(eng.Stats())
	})

	mux.HandleFunc("/raft", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id":          node.ID(),
			"state":       node.State().String(),
			"term":        node.Term(),
			"commitIndex": node.CommitIndex(),
			"lastApplied": node.LastApplied(),
		})
	})

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})
}

// crude helper – in a real system the leader address would be known via config or redirect
func findLeader(n *raft.Node) raft.NodeID {
	// For the in-process demo we just return empty; the client can try the other ports.
	return ""
}
