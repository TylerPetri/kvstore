package raft

// Storage is the durable state that must survive restarts
// Raft paper: currentTerm, votedFor, log[]
type Storage interface {
	// SaveState persists currentTerm and votedFor
	SaveState(term uint64, votedFor NodeID) error

	// State returns the last persisted term and votedFor
	State() (term uint64, votedFor NodeID, err error)

	// SaveLog persists the entire log (or a suffix in a more advanced version)
	SaveLog(entries []Entry) error

	// Log returns all persisted entries (index 1..N)
	Log() ([]Entry, error)
}
