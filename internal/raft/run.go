package raft

import (
	"math/rand/v2"
	"time"
)

// Start launches the node's run loop: election timeouts as a follower or
// candidate, and periodic heartbeats as a leader. It is idempotent.
func (r *Raft) Start() {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return
	}
	r.running = true
	r.stopCh = make(chan struct{})
	stop := r.stopCh
	r.resetElectionDeadline()
	interval := r.heartbeatInterval
	r.mu.Unlock()

	go r.run(interval, stop)
}

// Stop halts the run loop. It is idempotent.
func (r *Raft) Stop() {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return
	}
	r.running = false
	close(r.stopCh)
	r.mu.Unlock()
}

func (r *Raft) run(interval time.Duration, stop <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			r.tick()
		}
	}
}

// tick drives one step: a leader replicates/heartbeats, while a follower or
// candidate starts an election once its timeout has elapsed.
func (r *Raft) tick() {
	r.mu.Lock()
	if r.state == Leader {
		r.mu.Unlock()
		r.replicate()
		return
	}
	timedOut := time.Since(r.lastContact) >= r.electionTimeout
	r.mu.Unlock()

	if timedOut {
		if r.startElection() {
			r.replicate() // won: assert leadership immediately
		}
		r.mu.Lock()
		r.resetElectionDeadline()
		r.mu.Unlock()
	}
}

// resetElectionDeadline records a fresh contact time and picks a new random
// timeout in [base, 2*base). Randomization prevents split votes. Caller
// holds r.mu.
func (r *Raft) resetElectionDeadline() {
	r.lastContact = time.Now()
	r.electionTimeout = r.baseTimeout + time.Duration(rand.Int64N(int64(r.baseTimeout)))
}
