# Redis Storage

This storage implementation separates MoChi from its storage service. MoChi achieves HA by storing all peer data in
Redis. Multiple instances of MoChi can use the same redis instance concurrently. The storage service can get HA by
clustering. If one instance of MoChi goes down, peer data will still be available in Redis.

The HA of storage service is not considered here. In case Redis runs as a single node, peer data will be unavailable if
the node is down. You should consider setting up a Redis sentinel (or KeyDB active-active replication) for MoChi in
production.

This storage implementation is currently orders of magnitude slower than the in-memory implementation.

## Use Case

When one instance of MoChi is down, other instances can continue serving peers from Redis.

## Configuration

```yaml
mochi:
  storage:
    name: redis
    config:
      # The frequency which stale peers are removed.
      # This balances between
      # - collecting garbage more often, potentially using more CPU time, but potentially using less memory (lower value)
      # - collecting garbage less frequently, saving CPU time, but keeping old peers long, thus using more memory (higher value).
      gc_interval: 3m

      # The interval at which metrics about the number of infohashes and peers
      # are collected and posted to Prometheus.
      prometheus_reporting_interval: 1s

      # The amount of time until a peer is considered stale.
      # To avoid churn, keep this slightly larger than `announce_interval`
      peer_lifetime: 31m

      # The addresses of redis storage.
      addresses: ["127.0.0.1:6379"]
      
      # Database number
      db: 0

      # The timeout for reading a command reply from redis.
      read_timeout: 15s

      # The timeout for writing a command to redis.
      write_timeout: 15s

      # The timeout for connecting to redis server.
      connect_timeout: 15s
```

## Implementation

Seeders and Leechers for a particular InfoHash are stored within a redis hash. The InfoHash is used as key, _peer keys_
are the fields, last modified times are values. Peer keys are derived from peers and contain Peer ID, IP, and Port. All
the InfoHashes (swarms) are also stored in a redis hash, with IP family as the key, infohash as field, and last modified
time as value.

Here is an example:

```
- CHI_I
  - CHI_S4_<HASH1>
  - CHI_L4_<HASH1>
- CHI_S4_<HASH1>
  - <peer 1 key>: <modification time in unix nanos>
  - <peer 2 key>: <modification time in unix nanos>
- CHI_L4_<HASH2>
  - <peer 3 key>: <modification time in unix nanos>
...
```

In this case, prometheus would record two swarms, three seeders, and one leecher. These two keys
are used to record the count of seeders and leechers.

```
- CHI_S_C: "3"
- CHI_L_C: "1"
```

Note: `CHI_I` set has a different meaning compared to the `memory` storage:
It represents info hashes reported by seeder, meaning that info hashes without seeders are not counted.
