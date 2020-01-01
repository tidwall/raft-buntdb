**This project is deprecated. Please see the [tidwall/raft-wal](https://github.com/tidwall/raft-wal) project for a more advanced raft log.**

raft-buntdb
===========

For a better and faster Raft store please see the [raft-fastlog](https://github.com/tidwall/raft-fastlog) project.

This repository provides the `raftbuntdb` package. 
The package exports the `BuntStore` which is an implementation of both a 
`LogStore` and `StableStore`.

It is meant to be used as a backend for the `raft` 
[package here](https://github.com/hashicorp/raft).

This implementation uses [BuntDB](https://github.com/tidwall/buntdb). 
BuntDB is an in-memory database that persists to disk and is written in pure Go.
It includes transactions, is ACID compliant, and is very fast.

RaftStore Performance Comparison
--------------------------------

BuntDB (This implementation)
```
BenchmarkBuntStore_FirstIndex-8     5000000          282 ns/op
BenchmarkBuntStore_LastIndex-8      5000000          276 ns/op
BenchmarkBuntStore_GetLog-8         3000000          577 ns/op
BenchmarkBuntStore_StoreLog-8        500000         3663 ns/op
BenchmarkBuntStore_StoreLogs-8       200000        10149 ns/op
BenchmarkBuntStore_DeleteRange-8     200000         9768 ns/op
BenchmarkBuntStore_Set-8             500000         2843 ns/op
BenchmarkBuntStore_Get-8            3000000          434 ns/op
BenchmarkBuntStore_SetUint64-8       500000         2942 ns/op
BenchmarkBuntStore_GetUint64-8      3000000          430 ns/op
```

[MDB](https://github.com/hashicorp/raft-mdb)
```
BenchmarkMDBStore_FirstIndex-8  	 500000	        3043 ns/op
BenchmarkMDBStore_LastIndex-8  	     500000	        2941 ns/op
BenchmarkMDBStore_GetLog-8     	     300000	        4665 ns/op
BenchmarkMDBStore_StoreLog-8   	      10000	      183860 ns/op
BenchmarkMDBStore_StoreLogs-8  	      10000	      193783 ns/op
BenchmarkMDBStore_DeleteRange-8	      10000	      199927 ns/op
BenchmarkMDBStore_Set-8        	      10000	      147540 ns/op
BenchmarkMDBStore_Get-8        	     500000	        2324 ns/op
BenchmarkMDBStore_SetUint64-8  	      10000	      162291 ns/op
BenchmarkMDBStore_GetUint64-8  	    1000000	        2451 ns/op
```

[BoltDB](https://github.com/hashicorp/raft-boltdb)
```
BenchmarkBoltStore_FirstIndex-8 	2000000 	     848 ns/op
BenchmarkBoltStore_LastIndex-8  	2000000	         857 ns/op
BenchmarkBoltStore_GetLog-8     	 500000	        3169 ns/op
BenchmarkBoltStore_StoreLog-8   	  10000	      197432 ns/op
BenchmarkBoltStore_StoreLogs-8  	  10000	      205238 ns/op
BenchmarkBoltStore_DeleteRange-8	  10000	      189994 ns/op
BenchmarkBoltStore_Set-8        	  10000	      177010 ns/op
BenchmarkBoltStore_Get-8        	2000000	         983 ns/op
BenchmarkBoltStore_SetUint64-8  	  10000	      175435 ns/op
BenchmarkBoltStore_GetUint64-8  	2000000	         976 ns/op
```
