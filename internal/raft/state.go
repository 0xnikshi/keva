package raft

// State is the role a Raft node currently plays.
type State uint8

// The roles a Raft node can hold.
const (
	Follower State = iota
	Candidate
	Leader
)

// String returns the role's name.
func (s State) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}
