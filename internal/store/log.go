package store

import (
	"fmt"
	"sync"
)

type Entry struct {
	Index uint64
	Term  uint64
	Cmd   Command
}

type Log interface {
	// Append adds entries to the end of the log.
	// Returns the index of the last appended entry.
	Append(entries ...Entry) (lastIndex uint64, err error)

	// At returns the entry at the given index (1-based)
	// ok == false means the index does not exist
	At(index uint64) (Entry, bool)

	// Lastindex returns the entry of the last entry (0 if empty)
	LastIndex() uint64

	// Truncate removes all entries after the given index (inclusive)
	Truncate(after uint64) error
}

type MemoryLog struct {
	mu      sync.Mutex
	entries []Entry
}

func NewMemoryLog() *MemoryLog {
	return &MemoryLog{
		entries: make([]Entry, 1), // sentinel so real data starts at 1
	}
}

func (l *MemoryLog) Append(entries ...Entry) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, e := range entries {
		e.Index = uint64(len(l.entries))
		l.entries = append(l.entries, e)
	}

	return uint64(len(l.entries) - 1), nil
}

func (l *MemoryLog) At(index uint64) (Entry, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if index == 0 || index >= uint64(len(l.entries)) {
		return Entry{}, false
	}

	return l.entries[index], true
}

func (l *MemoryLog) LastIndex() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	return uint64(len(l.entries) - 1)
}

func (l *MemoryLog) Truncate(after uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if after >= uint64(len(l.entries)) {
		return fmt.Errorf("truncate: index %d outside of range", after)
	}

	l.entries = l.entries[:after]
	return nil
}
