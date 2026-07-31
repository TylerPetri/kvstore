package raft

import (
	"encoding/gob"
	"os"
	"path/filepath"
	"sync"
)

type FileStorage struct {
	mu   sync.Mutex
	dir  string
	term uint64
	vote NodeID
	log  []Entry // index 0 unused
}

func NewFileStorage(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &FileStorage{
		dir: dir,
		log: make([]Entry, 1),
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *FileStorage) statePath() string { return filepath.Join(s.dir, "state.gob") }
func (s *FileStorage) logPath() string   { return filepath.Join(s.dir, "log.gob") }

func (s *FileStorage) load() error {
	// state
	if f, err := os.Open(s.statePath()); err == nil {
		defer f.Close()
		var st struct {
			Term uint64
			Vote NodeID
		}
		if err := gob.NewDecoder(f).Decode(&st); err != nil {
			return err
		}
		s.term = st.Term
		s.vote = st.Vote
	}

	// log
	if f, err := os.Open(s.logPath()); err == nil {
		defer f.Close()
		var entries []Entry
		if err := gob.NewDecoder(f).Decode(&entries); err != nil {
			return err
		}
		if len(entries) == 0 {
			entries = make([]Entry, 1)
		}
		s.log = entries
	}
	return nil
}

func (s *FileStorage) SaveState(term uint64, votedFor NodeID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.term = term
	s.vote = votedFor

	f, err := os.Create(s.statePath())
	if err != nil {
		return err
	}
	defer f.Close()
	return gob.NewEncoder(f).Encode(struct {
		Term uint64
		Vote NodeID
	}{term, votedFor})
}

func (s *FileStorage) State() (uint64, NodeID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.term, s.vote, nil
}

func (s *FileStorage) SaveLog(entries []Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log = entries

	f, err := os.Create(s.logPath())
	if err != nil {
		return err
	}
	defer f.Close()
	return gob.NewEncoder(f).Encode(entries)
}

func (s *FileStorage) Log() ([]Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]Entry, len(s.log))
	copy(cp, s.log)
	return cp, nil
}
