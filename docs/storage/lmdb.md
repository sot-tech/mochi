# LMDB Storage

This storage uses LMDB persistent key-value database to store peer and/or arbitrary key-value data.

_Note:  **requires** CGO-enabled build (`CGO_ENABLED=1 go install...`), 
because of native bindings to C LMDB code._ 

# Use case

LMDB may be used in local installation to read/write data from/to (local or network mounted) 
filesystem directly, without network stack. 

It supports multi-thread/process (multi-reader, single-writer) access, and DB size larger than RAM,
so *may* be used within several applications as shared store, e.g. external app can modify allowed/denied 
info hashes for `torrentapproval` middleware or mochi cluster with shared DB over NFS.

As it filesystem database, performance is also depends on filesystem/disk performance, so it *may*
be slower than tuned PostgreSQL or Redis.

## Configuration and implementation notes

Basically LDMB environment is a directory with 2 files: data.mdb and lock.mdb, which will be created on start.
Each *environment* may contain several named databases. If name for specific database (for peers and arbitrary data) 
is not provided, default (0-th) database is used, which is *NOT* recommended, because is contains internal information.

Both stored key and value are byte arrays.
Arbitrary DB key format is `<PREFIX>_<KEY>`, value is byte array converted string.

Peers DB format is:

1. Key `<PREFIX>_<INFOHASH>_<PEERID><IPADDRESS><PORT>`, value - BE-encoded unix timestamp. 
Fields: 
   * `<PREFIX>` - `L4`, `L6`, `S4`, `S6` string for leecher with IPv4 or IPv6 address, or seeder with IPv4 or IPv6 address (accordingly)
   * `<INFOHASH>` - 20 or 32 bytes of info hash (V1 or V2 accordingly)
   * `<PEERID>` - 20 bytes of peer ID
   * `<IPADDRESS>` - 16 bytes of BE-encoded IP address (real IPv6 or IPv4-mapped IPv6 address)
   * `<PORT>` - 2 bytes of BE-encoded port
2. Key `<PREFIX>_<INFOHASH>_`, value - BE-encoded unsigned 32-bit integer.
`<PREFIX>` is `LC`, `SC`, `DC` string for leechers, seeders or downloaded count of specified `<INFOHASH>` (20 or 32 bytes).

Write speed may be increased with `no_sync_meta` and `async_write` configuration options,
but the risk of DB corruption is also increase.

With `async_write` option, write speed will be a little faster than locally installed OOTB Redis,
but application will use more RAM (dirty pages).
Without it, you will get dramatically slow write speed: every write transaction will be flushed to disk.

Option `no_sync_meta` **without** `async_write` will increase write speed up to 3 times
(with potential loss of last transaction if application crashed/killed).

Both enabled options don't make much sense and also don't affect read performance.

Benchmarks with combinations of options above:

```
async_write=false, no_sync_meta=false
BenchmarkStorage/BenchmarkPutGradDelete-16                        123                 9000653 ns/op

async_write=false, no_sync_meta=true
BenchmarkStorage/BenchmarkPutDelete1kInfoHash1k-16                306                 4001353 ns/op

async_write=true, no_sync_meta=false
BenchmarkStorage/BenchmarkPutGradDelete-16                        152656              7754 ns/op

async_write=true, no_sync_meta=true
BenchmarkStorage/BenchmarkPutGradDelete-16                        150116              7735 ns/op
```

**Sample configuration:**

```yaml
storage:
    name: lmdb
    config:
        # The frequency which stale peers are removed.
        # This balances between
        # - collecting garbage more often, potentially using more CPU time, but potentially using less memory (lower value)
        # - collecting garbage less frequently, saving CPU time, but keeping old peers long, thus using more memory (higher value).
        gc_interval: 3m
    
        # The amount of time until a peer is considered stale.
        # To avoid churn, keep this slightly larger than `announce_interval`
        peer_lifetime: 31m
    
        # Path to LMDB folder. Required.
        Path: "/some/path/to/lmdb/directory"
    
        # File mode of created database files, default is 0o640
        mode: 0640
    
        # Name of database to store KV data. If not provided, root DB is used (not recommended)
        data_db: "DATA"
    
        # Name of database to store peers data. If not provided, root DB is used (not recommended)
        peers_db: "PEERS"
    
        # Maximum size of database, default is 1GiB.
        # It's better specify enough space, because if environment is full, 
        # storage will fail to add new records and restart and specifying larger size 
        # (or online resizing with external tool) will be required.
        # See: http://www.lmdb.tech/doc/group__mdb.html#gaa2506ec8dab3d969b0e609cd82e619e5
        max_size: 1073741824
    
        # Maximum number of threads/reader slots for the LMDB environment, default is 126.
        # See: http://www.lmdb.tech/doc/group__mdb.html#gae687966c24b790630be2a41573fe40e2
        max_readers: 126
    
        # Set flags to use asynchronous flushes to disk.
        # See: MDB_WRITEMAP and MDB_MAPASYNC description in http://www.lmdb.tech/doc/group__mdb.html#ga32a193c6bf4d7d5c5d579e71f22e9340
        async_write: true
    
        # Set MDB_NOMETASYNC flag. Omit the metadata flush on commit.
        # See: MDB_NOMETASYNC description in http://www.lmdb.tech/doc/group__mdb.html#ga32a193c6bf4d7d5c5d579e71f22e9340
        no_sync_meta: false
```
