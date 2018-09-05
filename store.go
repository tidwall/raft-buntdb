package raftbuntdb

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	"github.com/tidwall/buntdb"
	"github.com/tidwall/raft"
)

type Level int

const (
	Low    Level = -1
	Medium Level = 0
	High   Level = 1
)

var (
	// Bucket names we perform transactions in
	dbLogs = "l:"
	dbConf = "c:"

	// An error indicating a given key does not exist
	ErrKeyNotFound = errors.New("not found")
)

// BuntStore provides access to BuntDB for Raft to store and retrieve
// log entries. It also provides key/value storage, and can be used as
// a LogStore and StableStore.
type BuntStore struct {
	// conn is the underlying handle to the db.
	db *buntdb.DB

	// The path to the Bunt database file
	path string
}

// NewBuntStore takes a file path and returns a connected Raft backend.
func NewBuntStore(path string, durability Level) (*BuntStore, error) {
	// Try to connect
	db, err := buntdb.Open(path)
	if err != nil {
		return nil, err
	}

	// Disable the AutoShrink. Shrinking should only be manually
	// handled following a log compaction.
	var config buntdb.Config
	if err := db.ReadConfig(&config); err != nil {
		db.Close()
		return nil, err
	}
	config.AutoShrinkDisabled = true
	switch durability {
	case Low:
		config.SyncPolicy = buntdb.Never
	case Medium:
		config.SyncPolicy = buntdb.EverySecond
	case High:
		config.SyncPolicy = buntdb.Always
	}
	if err := db.SetConfig(config); err != nil {
		db.Close()
		return nil, err
	}

	// Create the new store
	store := &BuntStore{
		db:   db,
		path: path,
	}
	return store, nil
}

// Close is used to gracefully close the DB connection.
func (b *BuntStore) Close() error {
	return b.db.Close()
}

// Shrink will trigger a shrink operation on the aof file.
// Useful after a log compaction is completed.
func (b *BuntStore) Shrink() error {
	return b.db.Shrink()
}

// FirstIndex returns the first known index from the Raft log.
func (b *BuntStore) FirstIndex() (uint64, error) {
	var num string
	err := b.db.View(func(tx *buntdb.Tx) error {
		return tx.Ascend("",
			func(key, val string) bool {
				if strings.HasPrefix(key, dbLogs) {
					num = key[len(dbLogs):]
					return false
				}
				return true
			},
		)
	})
	if err != nil || num == "" {
		return 0, err
	}
	return stringToUint64(num), nil
}

// LastIndex returns the last known index from the Raft log.
func (b *BuntStore) LastIndex() (uint64, error) {
	var num string
	err := b.db.View(func(tx *buntdb.Tx) error {
		return tx.Descend("",
			func(key, val string) bool {
				if strings.HasPrefix(key, dbLogs) {
					num = key[len(dbLogs):]
					return false
				}
				return true
			},
		)
	})
	if err != nil || num == "" {
		return 0, err
	}
	return stringToUint64(num), nil
}

// GetLog is used to retrieve a log from BuntDB at a given index.
func (b *BuntStore) GetLog(idx uint64, log *raft.Log) error {
	var val string
	var verr error
	err := b.db.View(func(tx *buntdb.Tx) error {
		val, verr = tx.Get(dbLogs + uint64ToString(idx))
		return verr
	})
	if err != nil {
		if err == buntdb.ErrNotFound {
			return raft.ErrLogNotFound
		}
		return err
	}
	return decodeLog(val, log)
}

// StoreLog is used to store a single raft log
func (b *BuntStore) StoreLog(log *raft.Log) error {
	return b.StoreLogs([]*raft.Log{log})
}

// StoreLogs is used to store a set of raft logs
func (b *BuntStore) StoreLogs(logs []*raft.Log) error {
	err := b.db.Update(func(tx *buntdb.Tx) error {
		for _, log := range logs {
			val, err := encodeLog(log)
			if err != nil {
				return err
			}
			if _, _, err := tx.Set(dbLogs+uint64ToString(log.Index),
				string(val), nil); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

// DeleteRange is used to delete logs within a given range inclusively.
func (b *BuntStore) DeleteRange(min, max uint64) error {
	return b.db.Update(func(tx *buntdb.Tx) error {
		for i := min; i <= max; i++ {
			if _, err := tx.Delete(dbLogs + uint64ToString(i)); err != nil {
				if err != buntdb.ErrNotFound {
					return err
				}
			}
		}
		return nil
	})
}

// Set is used to set a key/value set outside of the raft log
func (b *BuntStore) Set(k, v []byte) error {
	return b.db.Update(func(tx *buntdb.Tx) error {
		_, _, err := tx.Set(dbConf+string(k), string(v), nil)
		return err
	})
}

// Get is used to retrieve a value from the k/v store by key
func (b *BuntStore) Get(k []byte) ([]byte, error) {
	var val []byte
	err := b.db.View(func(tx *buntdb.Tx) error {
		sval, err := tx.Get(dbConf + string(k))
		if err != nil {
			return err
		}
		val = []byte(sval)
		return nil
	})
	if err != nil {
		if err == buntdb.ErrNotFound {
			return nil, ErrKeyNotFound
		}
	}
	if err != nil {
		return nil, err
	}
	return val, nil
}

// SetUint64 is like Set, but handles uint64 values
func (b *BuntStore) SetUint64(key []byte, val uint64) error {
	return b.Set(key, []byte(strconv.FormatUint(val, 10)))
}

// GetUint64 is like Get, but handles uint64 values
func (b *BuntStore) GetUint64(key []byte) (uint64, error) {
	val, err := b.Get(key)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(string(val), 10, 64)

}

// Peers returns raft peers
func (b *BuntStore) Peers() ([]string, error) {
	var peers []string
	val, err := b.Get([]byte("peers"))
	if err != nil {
		if err == ErrKeyNotFound {
			return []string{}, nil
		}
		return nil, err
	}
	if err := json.Unmarshal(val, &peers); err != nil {
		return nil, err
	}
	return peers, nil
}

// SetPeers sets raft peers
func (b *BuntStore) SetPeers(peers []string) error {
	data, err := json.Marshal(peers)
	if err != nil {
		return err
	}
	return b.Set([]byte("peers"), data)
}

// Decode reverses the encode operation on a byte slice input
func decodeLog(s string, in *raft.Log) error {
	buf := []byte(s)
	if len(buf) < 17 {
		return errors.New("invalid buffer")
	}
	in.Index = binary.LittleEndian.Uint64(buf[0:8])
	in.Term = binary.LittleEndian.Uint64(buf[8:16])
	in.Type = raft.LogType(buf[16])
	in.Data = buf[17:]
	return nil
}

// Encode writes an encoded object to a new bytes buffer
func encodeLog(in *raft.Log) ([]byte, error) {
	buf := make([]byte, 17+len(in.Data))
	binary.LittleEndian.PutUint64(buf[0:8], in.Index)
	binary.LittleEndian.PutUint64(buf[8:16], in.Term)
	buf[16] = byte(in.Type)
	copy(buf[17:], in.Data)
	return buf, nil
}

// Converts string to an integer
func stringToUint64(s string) uint64 {
	n, _ := strconv.ParseUint(s, 10, 64)
	return n
}

// Converts a uint to a string
func uint64ToString(u uint64) string {
	s := strings.Repeat("0", 20) + strconv.FormatUint(u, 10)
	return s[len(s)-20:]
}
