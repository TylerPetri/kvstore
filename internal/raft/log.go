package raft

// Log is the Raft log. We keep it minimal.
// Later we will adapt our existing store.MemoryLog or write a thin wrapper.
type Log interface {
	Append(entries ...Entry) (lastIndex uint64, err error)
	At(index uint64) (Entry, bool)
	LastIndex() uint64
	LastTerm() uint64 // convenience: term of the last entry (0 if empty)
	Truncate(after uint64) error
}

// MemoryLog is a trivial implementation for early tests.
type MemoryLog struct {
	entries []Entry // index 0 unused
}

func NewMemoryLog() *MemoryLog {
	return &MemoryLog{entries: make([]Entry, 1)}
}

func (l *MemoryLog) Append(entries ...Entry) (uint64, error) {
	for i := range entries {
		entries[i].Index = uint64(len(l.entries))
		l.entries = append(l.entries, entries[i])
	}
	return uint64(len(l.entries) - 1), nil
}

func (l *MemoryLog) At(index uint64) (Entry, bool) {
	if index == 0 || index >= uint64(len(l.entries)) {
		return Entry{}, false
	}
	return l.entries[index], true
}

func (l *MemoryLog) LastIndex() uint64 {
	return uint64(len(l.entries) - 1)
}

func (l *MemoryLog) LastTerm() uint64 {
	if len(l.entries) <= 1 {
		return 0
	}
	return l.entries[len(l.entries)-1].Term
}

func (l *MemoryLog) Truncate(after uint64) error {
	if after >= uint64(len(l.entries)) {
		return nil
	}
	l.entries = l.entries[:after]
	return nil
}
