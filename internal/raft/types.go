package raft

type NodeID string // use simple Node123 string for now

type Peer struct {
	ID   NodeID
	Addr string
}

// Config is the cluster configuration
type Config struct {
	ID            NodeID
	Peers         []Peer
	ElectionTick  int
	HeartbeatTick int
}

func (c *Config) Majority() int {
	return len(c.Peers)/2 + 1
}
