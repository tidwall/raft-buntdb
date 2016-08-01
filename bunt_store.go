package raftbuntdb

import (
	"errors"
	"strings"

	"github.com/hashicorp/raft"
	"github.com/tidwall/buntdb"
)

const (
	// Permissions to use on the db file. This is only used if the
	// database file does not exist and needs to be created.
	dbFileMode = 0666
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
func NewBuntStore(path string) (*BuntStore, error) {
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
	var snum string
	err := b.db.View(func(tx *buntdb.Tx) error {
		return tx.AscendGreaterOrEqual("", dbLogs,
			func(key, val string) bool {
				snum = key[len(dbLogs):]
				return false
			},
		)
	})
	if err != nil || snum == "" {
		return 0, err
	}
	return stringToUint64(snum), nil
}

// LastIndex returns the last known index from the Raft log.
func (b *BuntStore) LastIndex() (uint64, error) {
	var snum string
	err := b.db.View(func(tx *buntdb.Tx) error {
		return tx.DescendGreaterThan("", dbLogs,
			func(key, val string) bool {
				snum = key[len(dbLogs):]
				return false
			},
		)
	})
	if err != nil || snum == "" {
		return 0, err
	}
	return stringToUint64(snum), nil
}

// AscendLogGreaterOrEqual is used to iterate through log entries.
func (b *BuntStore) AscendLogGreaterOrEqual(pivot uint64, iter func(log *raft.Log) bool) error {
	return b.db.View(func(tx *buntdb.Tx) error {
		var ierr error

		err := tx.AscendGreaterOrEqual("", dbLogs+uint64ToString(pivot),
			func(key, val string) bool {
				if !strings.HasPrefix(key, dbLogs) {
					return false
				}
				var log raft.Log
				if err := decodeLog([]byte(val), &log); err != nil {
					ierr = err
					return false
				}
				return iter(&log)
			},
		)
		if err != nil {
			return err
		}
		return ierr
	})
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
	return decodeLog([]byte(val), log)
}

// StoreLog is used to store a single raft log
func (b *BuntStore) StoreLog(log *raft.Log) error {
	return b.StoreLogs([]*raft.Log{log})
}

// StoreLogs is used to store a set of raft logs
func (b *BuntStore) StoreLogs(logs []*raft.Log) error {
	err := b.db.Update(func(tx *buntdb.Tx) error {
		for _, log := range logs {
			key := make([]byte, 0, 22)
			key = append(key, dbLogs...)
			key = append(key, uint64ToString(log.Index)...)
			val, err := encodeLog(log)
			if err != nil {
				return err
			}
			if _, _, err := tx.Set(string(key), string(val), nil); err != nil {
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
	var rval []byte
	err := b.db.View(func(tx *buntdb.Tx) error {
		key := make([]byte, 0, 64)
		key = append(key, dbConf...)
		key = append(key, k...)
		val, err := tx.Get(string(key))
		if err != nil {
			return err
		}
		rval = make([]byte, len(val))
		copy(rval, []byte(val))
		return nil
	})
	if err != nil {
		if err == buntdb.ErrNotFound {
			return nil, ErrKeyNotFound
		}
	}
	if rval == nil {
		rval = []byte{}
	}
	return rval, nil
}

// SetUint64 is like Set, but handles uint64 values
func (b *BuntStore) SetUint64(key []byte, val uint64) error {
	return b.Set(key, []byte(uint64ToString(val)))
}

// GetUint64 is like Get, but handles uint64 values
func (b *BuntStore) GetUint64(key []byte) (uint64, error) {
	val, err := b.Get(key)
	if err != nil {
		return 0, err
	}
	return stringToUint64(string(val)), nil
}
