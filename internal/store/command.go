package store

type Command struct {
	Op    string
	Key   string
	Value string
}

func (s *Store) Apply(cmd Command) (string, bool) {
	switch cmd.Op {
	case "set":
		s.Set(cmd.Key, cmd.Value)
		return "", true
	case "delete":
		s.Delete(cmd.Key)
		return "", true
	case "get":
		return s.Get(cmd.Key)
	default:
		return "", false
	}
}
