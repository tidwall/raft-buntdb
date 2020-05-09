package raftbuntdb

import (
	"bytes"
	"io/ioutil"
	"math/rand"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/tidwall/raft"
)

func testBuntStore(t testing.TB) *BuntStore {
	fh, err := ioutil.TempFile("", "bunt")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	os.Remove(fh.Name())

	// Successfully creates and returns a store
	store, err := NewBuntStore(fh.Name(), Medium)
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	return store
}

func testRaftLog(idx uint64, data string) *raft.Log {
	return &raft.Log{
		Data:  []byte(data),
		Index: idx,
	}
}

func TestBuntStore_Implements(t *testing.T) {
	var store interface{} = &BuntStore{}
	if _, ok := store.(raft.StableStore); !ok {
		t.Fatalf("BuntStore does not implement raft.StableStore")
	}
	if _, ok := store.(raft.LogStore); !ok {
		t.Fatalf("BuntStore does not implement raft.LogStore")
	}
}

func TestNewBuntStore(t *testing.T) {
	fh, err := ioutil.TempFile("", "bunt")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	os.Remove(fh.Name())
	defer os.Remove(fh.Name())

	// Successfully creates and returns a store
	store, err := NewBuntStore(fh.Name(), High)
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure the file was created
	if store.path != fh.Name() {
		t.Fatalf("unexpected file path %q", store.path)
	}
	if _, err := os.Stat(fh.Name()); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Close the store so we can open again
	if err := store.Close(); err != nil {
		t.Fatalf("err: %s", err)
	}

	//// Ensure our tables were created
	//db, err := bunt.Open(fh.Name(), dbFileMode, nil)
	//if err != nil {
	//	t.Fatalf("err: %s", err)
	//}
	//tx, err := db.Begin(true)
	//if err != nil {
	//	t.Fatalf("err: %s", err)
	//}
	//if _, err := tx.CreateBucket([]byte(dbLogs)); err != bunt.ErrBucketExists {
	//	t.Fatalf("bad: %v", err)
	//}
	//if _, err := tx.CreateBucket([]byte(dbConf)); err != bunt.ErrBucketExists {
	//	t.Fatalf("bad: %v", err)
	//}
}

func TestBuntStore_Peers(t *testing.T) {
	store := testBuntStore(t)
	defer store.Close()
	defer os.Remove(store.path)
	peers, err := store.Peers()
	if err != nil {
		t.Fatal(err)
	}
	if len(peers) != 0 {
		t.Fatalf("expected '%v', got '%v'", 0, len(peers))
	}
	v := []string{"1", "2", "3"}
	if err := store.SetPeers(v); err != nil {
		t.Fatal(err)
	}
	peers, err = store.Peers()
	if err != nil {
		t.Fatal(err)
	}
	if len(peers) != 3 {
		t.Fatalf("expected '%v', got '%v'", 3, len(peers))
	}
	if peers[0] != "1" || peers[1] != "2" || peers[2] != "3" {
		t.Fatalf("expected %v, got %v", v, peers)
	}
}

func TestBuntStore_FirstIndex(t *testing.T) {
	store := testBuntStore(t)
	defer store.Close()
	defer os.Remove(store.path)

	// Should get 0 index on empty log
	idx, err := store.FirstIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 0 {
		t.Fatalf("bad: %v", idx)
	}

	// Set a mock raft log
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("bad: %s", err)
	}

	// Fetch the first Raft index
	idx, err = store.FirstIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 1 {
		t.Fatalf("bad: %d", idx)
	}
}

func TestBuntStore_LastIndex(t *testing.T) {
	store := testBuntStore(t)
	defer store.Close()
	defer os.Remove(store.path)

	// Should get 0 index on empty log
	idx, err := store.LastIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 0 {
		t.Fatalf("bad: %v", idx)
	}

	// Set a mock raft log
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("bad: %s", err)
	}

	// Fetch the last Raft index
	idx, err = store.LastIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 3 {
		t.Fatalf("bad: %d", idx)
	}
}

func TestBuntStore_GetLog(t *testing.T) {
	store := testBuntStore(t)
	defer store.Close()
	defer os.Remove(store.path)

	log := new(raft.Log)

	// Should return an error on non-existent log
	if err := store.GetLog(1, log); err != raft.ErrLogNotFound {
		t.Fatalf("expected raft log not found error, got: %v", err)
	}

	// Set a mock raft log
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("bad: %s", err)
	}

	// Should return the proper log
	if err := store.GetLog(2, log); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(log, logs[1]) {
		t.Fatalf("bad: %#v", log)
	}
}

func TestBuntStore_SetLog(t *testing.T) {
	store := testBuntStore(t)
	defer store.Close()
	defer os.Remove(store.path)

	// Create the log
	log := &raft.Log{
		Data:  []byte("log1"),
		Index: 1,
	}

	// Attempt to store the log
	if err := store.StoreLog(log); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Retrieve the log again
	result := new(raft.Log)
	if err := store.GetLog(1, result); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure the log comes back the same
	if !reflect.DeepEqual(log, result) {
		t.Fatalf("bad: %v", result)
	}
}

func TestBuntStore_SetLogs(t *testing.T) {
	store := testBuntStore(t)
	defer store.Close()
	defer os.Remove(store.path)

	// Create a set of logs
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
	}

	// Attempt to store the logs
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure we stored them all
	result1, result2 := new(raft.Log), new(raft.Log)
	if err := store.GetLog(1, result1); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(logs[0], result1) {
		t.Fatalf("bad: %#v", result1)
	}
	if err := store.GetLog(2, result2); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(logs[1], result2) {
		t.Fatalf("bad: %#v", result2)
	}
}

func TestBuntStore_DeleteRange(t *testing.T) {
	store := testBuntStore(t)
	defer store.Close()
	defer os.Remove(store.path)

	// Create a set of logs
	log1 := testRaftLog(1, "log1")
	log2 := testRaftLog(2, "log2")
	log3 := testRaftLog(3, "log3")
	logs := []*raft.Log{log1, log2, log3}

	// Attempt to store the logs
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Attempt to delete a range of logs
	if err := store.DeleteRange(1, 2); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure the logs were deleted
	if err := store.GetLog(1, new(raft.Log)); err != raft.ErrLogNotFound {
		t.Fatalf("should have deleted log1")
	}
	if err := store.GetLog(2, new(raft.Log)); err != raft.ErrLogNotFound {
		t.Fatalf("should have deleted log2")
	}
}

func TestBuntStore_Set_Get(t *testing.T) {
	store := testBuntStore(t)
	defer store.Close()
	defer os.Remove(store.path)

	// Returns error on non-existent key
	if _, err := store.Get([]byte("bad")); err != ErrKeyNotFound {
		t.Fatalf("expected not found error, got: %q", err)
	}

	k, v := []byte("hello"), []byte("world")

	// Try to set a k/v pair
	if err := store.Set(k, v); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Try to read it back
	val, err := store.Get(k)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if !bytes.Equal(val, v) {
		t.Fatalf("bad: %v", val)
	}
}

func TestBuntStore_SetUint64_GetUint64(t *testing.T) {
	store := testBuntStore(t)
	defer store.Close()
	defer os.Remove(store.path)

	// Returns error on non-existent key
	if _, err := store.GetUint64([]byte("bad")); err != ErrKeyNotFound {
		t.Fatalf("expected not found error, got: %q", err)
	}

	k, v := []byte("abc"), uint64(123)

	// Attempt to set the k/v pair
	if err := store.SetUint64(k, v); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Read back the value
	val, err := store.GetUint64(k)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if val != v {
		t.Fatalf("bad: %v", val)
	}
}

func TestUtilHex(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	for i1 := uint64(0); i1 < 1000; i1++ {
		h := uint64ToString(i1)
		i2 := stringToUint64(h)
		if i2 != i1 {
			t.Fatalf("expected: %v, got %v", i1, i2)
		}
	}
	for i := 0; i < 100000; i++ {
		i1 := uint64(rand.Int63())
		h := uint64ToString(i1)
		i2 := stringToUint64(h)
		if i2 != i1 {
			t.Fatalf("expected: %v, got %v", i1, i2)
		}
	}
}
