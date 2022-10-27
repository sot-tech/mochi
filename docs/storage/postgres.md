# PostgreSQL Storage

This storage uses PostgreSQL-like database to store peer and arbitrary key-value data.

'PostgreSQL-like' means, that you can use any database which _understand_ PostgreSQL protocol
i.e. _real_ [PostgreSQL](https://www.postgresql.org) or [CockroachDB](https://www.cockroachlabs.com).

_(YugabyteDB is not recommended (at the moment), because of some problems with
concurrent inserts while benchmarks.)_

# Use case

Redis is good and fast solution for storing MoChi data, which used across multiple nodes,
but it is in-memory DB and there are some problems with it:

1. on heavy load it consumes a lot of memory, which increased up to 3x times while snapshot (bgsave)
2. if redis reaches memory limit it just stops working
3. when HA is not used and redis instance dies, it loses all data from last snapshot

On the other hand, PostgreSQL relies on persistent storage:
it much slower than in-memory solution (especially in insert operations),
however it can serve about 10 millions MoChi records using OOTB parameters
(128MiB shared buffers and 4MiB work memory consumption)
with suitable enough performance.

So if you don't have a lot of RAM, but have a lot of peers/info hashes,
you might use this store type.

## Configuration and implementation notes

This store type relies on database structure and queries that
you provide in configuration.

Implementation expects next data types:

* Table for peers:
  * info hash - byte array (`bytea`)
  * peer ID - byte array (`bytea`)
  * peer address - `inet` or byte array (`bytea`)
  * peer port - integer or derivative type (`int4`, `integer`)
  * is seeder - boolean (`bool`)
  * is IPv6 - boolean (`bool`)
  * peer creation date and time - `timestamp`
* Table of download counts (optional)
  * info hash - byte array (`bytea`)
  * count - or derivative type (`int4`, `integer`)
* Table for arbitrary data (KV store):
  * context - string (`varchar`, `character varying`)
  * name - byte array (`bytea`)*
  * value - byte array (`bytea`)

(*) in KV table `name` present as byte array because of possibility
to place hash as _raw_ string, which is not supported by PostgreSQL.

Sample script to create tables:

```sql
CREATE TABLE mo_peers
(
    info_hash bytea     NOT NULL,
    peer_id   bytea     NOT NULL,
    address   inet      NOT NULL,
    port      int4      NOT NULL,
    is_seeder bool      NOT NULL,
    is_v6     bool      NOT NULL,
    created   timestamp NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (info_hash, peer_id, address, port)
);

CREATE INDEX mo_peers_created_idx ON mo_peers (created);
CREATE INDEX mo_peers_announce_idx ON mo_peers (info_hash, is_seeder, is_v6);

CREATE TABLE mo_downloads (
	info_hash bytea PRIMARY KEY NOT NULL,
	downloads int NOT NULL DEFAULT 1
);

CREATE TABLE mo_kv
(
    context varchar NOT NULL,
    name    bytea   NOT NULL,
    value   bytea,
    PRIMARY KEY (context, name)
);
```

_Note: CockroachDB currently does not support index
over `inet` type, but it is possible to use `bytea` instead._

```yaml
storage:
    name: pg
    config:
        # Connection string to PostgreSQL.
        # May be URL (postgres://...) or DSN (host=... port=...)
        connection_string: host=127.0.0.1 port=5432 database=... user=...
        announce:
            # Query to select peers by info hash and flags
            query: SELECT peer_id, address, port FROM mo_peers WHERE info_hash=$1 AND is_seeder=$2 AND is_v6=$3 LIMIT $4
            # Column name of peer id in `query` above (case-insensitive). 
            peer_id_column: peer_id
            # Column name of address in `query` above (case-insensitive).
            address_column: address
            # Column name of port in `query` above (case-insensitive).
            port_column: port
        peer:
            # Query to add peer info.
            # Expected arguments: 
            # 1 - info_hash (bytea), 
            # 2 - peer_id (bytea), 
            # 3 - address (bytea/inet)
            # 4 - port (int4), 
            # 5 - is_seeder (bool), 
            # 6 - is_v6 (bool), 
            # 7 - created (timestamp)
            # Query MUST handle situations when tuple 
            # `info hash - peer ID - address - port` is already 
            # exists in table
            add_query: INSERT INTO mo_peers VALUES(@info_hash, @peer_id, @address, @port, @is_seeder, @is_v6, @created) ON CONFLICT (info_hash, peer_id, address, port) DO UPDATE SET created = EXCLUDED.created, is_seeder = EXCLUDED.is_seeder
            # Query to delete peer info.
            # Query SHOULD take into account value of `is_seeder` flag
            del_query: DELETE FROM mo_peers WHERE info_hash=@info_hash AND peer_id=@peer_id AND address=@address AND port=@port AND is_seeder=@is_seeder
            # Query to update leecher to seeder
            graduate_query: UPDATE mo_peers SET is_seeder=TRUE WHERE info_hash=@info_hash AND peer_id=peer_id AND address=@address AND port=@port AND NOT is_seeder
            # Query to get count of peers.
            # Used both for statistics and for scrape (with clause suffix, see next).
            # Only first returned row value used.
            count_query: SELECT COUNT(1) FILTER (WHERE is_seeder) AS seeders, COUNT(1) FILTER (WHERE NOT is_seeder) AS leechers FROM mo_peers
            # Predicate part of `count_query` for get count of peers by info hash
            by_info_hash_clause: WHERE info_hash = @info_hash
            # Column name of seeders count in `count_query` (case-insensitive).
            count_seeders_column: seeders
            # Column name of leechers count in `count_query` (case-insensitive).
            count_leechers_column: leechers
        # Queries to get/increment 'snatched' (downloaded) count
        downloads:
            get_query: SELECT downloads FROM mo_downloads where info_hash=@info_hash
            inc_query: INSERT INTO mo_downloads VALUES(@info_hash) ON CONFLICT(info_hash) DO UPDATE SET downloads = mo_downloads.downloads + 1
        # Queries for KV-store
        data:
            # Query to add data.
            # Expected arguments: 
            # 1 - context (varchar), 
            # 2 - key (bytea), 
            # 3 - value (bytea)
            add_query: INSERT INTO mo_kv VALUES(@context, @key, @value) ON CONFLICT (context, name) DO NOTHING
            # Query to delete data.
            # Note: @key parameter is array, NOT single value
            del_query: DELETE FROM mo_kv WHERE context=@context AND name = ANY(@key)
            # Query to get data.
            # Only first returned row and column value used.
            get_query: SELECT value FROM mo_kv WHERE context=@context AND name=@key
        # Query for check if database is alive (can be omitted)
        ping_query: SELECT 1
        # Query to delete stale peers (peers, which timestamp older than provided argument)
        gc_query: DELETE FROM mo_peers WHERE created <= @created
        # The frequency which stale peers are removed.
        gc_interval: 3m
        # Query to get all info hash count (used for statistics).
        # Only first returned row and column value used.
        info_hash_count_query: SELECT COUNT(DISTINCT info_hash) as info_hashes FROM mo_peers
        # The interval at which metrics about the number of info hashes and peers
        # are collected and posted to Prometheus.
        prometheus_reporting_interval: 1s
```

You can use own database structure and queries, but queries should have
same behaviour and arguments as provided in example above.
