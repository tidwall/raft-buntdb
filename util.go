package raftbuntdb

import (
	"encoding/binary"
	"errors"
	"strconv"

	"github.com/hashicorp/raft"
)

// Decode reverses the encode operation on a byte slice input
func decodeLog(buf []byte, in *raft.Log) error {
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
	s := ("00000000000000000000" + strconv.FormatUint(u, 10))
	return s[len(s)-20:]
}
