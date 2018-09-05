package raftbuntdb

import (
	"os"
	"testing"

	"github.com/tidwall/raft/bench"
)

func BenchmarkBuntStore_FirstIndex(b *testing.B) {
	store := testBuntStore(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.FirstIndex(b, store)
}

func BenchmarkBuntStore_LastIndex(b *testing.B) {
	store := testBuntStore(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.LastIndex(b, store)
}

func BenchmarkBuntStore_GetLog(b *testing.B) {
	store := testBuntStore(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.GetLog(b, store)
}

func BenchmarkBuntStore_StoreLog(b *testing.B) {
	store := testBuntStore(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.StoreLog(b, store)
}

func BenchmarkBuntStore_StoreLogs(b *testing.B) {
	store := testBuntStore(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.StoreLogs(b, store)
}

func BenchmarkBuntStore_DeleteRange(b *testing.B) {
	store := testBuntStore(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.DeleteRange(b, store)
}

func BenchmarkBuntStore_Set(b *testing.B) {
	store := testBuntStore(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.Set(b, store)
}

func BenchmarkBuntStore_Get(b *testing.B) {
	store := testBuntStore(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.Get(b, store)
}

func BenchmarkBuntStore_SetUint64(b *testing.B) {
	store := testBuntStore(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.SetUint64(b, store)
}

func BenchmarkBuntStore_GetUint64(b *testing.B) {
	store := testBuntStore(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.GetUint64(b, store)
}
