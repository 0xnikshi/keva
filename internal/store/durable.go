package store

import "github.com/0xnikshi/keva/internal/keva"

// Durable is a Store that persists every mutation to a write-ahead log
// and rebuilds its in-memory state by replaying that log on open.
type Durable struct {
	mem *Memory
	wal *WAL
}

// compile-time check that *Durable satisfies Store.
var _ Store = (*Durable)(nil)

// OpenDurable opens (creating if needed) a durable store backed by the
// write-ahead log at path, replaying any existing log to recover state.
func OpenDurable(path string) (*Durable, error) {
	cmds, err := ReadAll(path)
	if err != nil {
		return nil, err
	}

	mem := NewMemory()
	for _, cmd := range cmds {
		applyCommand(mem, cmd)
	}

	wal, err := OpenWAL(path)
	if err != nil {
		return nil, err
	}

	return &Durable{mem: mem, wal: wal}, nil
}

// Get returns the value stored at key, or ErrNotFound if it is absent.
func (d *Durable) Get(key keva.Key) (keva.Value, error) {
	return d.mem.Get(key)
}

// Put appends the write to the log, then applies it in memory. The log
// append is durable before the value becomes visible.
func (d *Durable) Put(key keva.Key, value keva.Value) error {
	cmd := keva.Command{Op: keva.OpPut, Key: key, Value: value}
	if err := d.wal.Append(cmd); err != nil {
		return err
	}
	applyCommand(d.mem, cmd)
	return nil
}

// Delete appends the deletion to the log, then applies it in memory.
func (d *Durable) Delete(key keva.Key) error {
	cmd := keva.Command{Op: keva.OpDelete, Key: key}
	if err := d.wal.Append(cmd); err != nil {
		return err
	}
	applyCommand(d.mem, cmd)
	return nil
}

// Close closes the underlying write-ahead log.
func (d *Durable) Close() error {
	return d.wal.Close()
}

// applyCommand maps a Command onto in-memory state. It is the single
// place that translates commands into store mutations, shared by live
// writes and by replay on open.
func applyCommand(mem *Memory, cmd keva.Command) {
	switch cmd.Op {
	case keva.OpPut:
		_ = mem.Put(cmd.Key, cmd.Value)
	case keva.OpDelete:
		_ = mem.Delete(cmd.Key)
	}
}
