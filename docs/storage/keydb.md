# KeyDB Storage

This storage mainly the same as Redis and uses some of [redis](redis.md) store logic
with next exceptions:

* peers stored in [sets](https://redis.io/docs/manual/data-types/#sets)
  instead of [hashes](https://redis.io/docs/manual/data-types/#hashes);

* keys such as `CHI_I`, `CHI_S_C` and `CHI_L_C` not used (at all);

* peer TTL relies on KeyDB's [EXPIREMEMBER](https://docs.keydb.dev/docs/commands/#expiremember)
  command, so MoChi does not need to periodically check peer expiration;

* storage does not execute periodical statistics collection (peer/lecher/info hash count)
  because:
	* manual calculation (INC/DEC peers count) is not usable
	* manual scan of all keys is quite expensive operation.

## Use Case

KeyDB is fork of Redis, which allows to create active-active cluster and set `set` member expiration,
so this type of backend can be used to create fully symmetric cluster of tracker nodes with minimum
overkill to garbage collection.

## Configuration

Configuration options are the same as [redis's](redis.md#Configuration), **BUT**:

* `name` should be set to `keydb` instead of `redis`;

* `gc_interval` and `prometheus_reporting_interval` don't have any sense.

```yaml
mochi:
  storage:
    name: keydb
    config:
	...
```

## Implementation

KeyDB storage uses same key names as `redis` (`CHI_S4_<HASH>`, `CHI_L6_<HASH>`...) to store peers,
but it is **impossible** to switch between storage providers without deleting these keys.
You **can** use `redis` storage type with KeyDB instance (KeyDB supports all Redis commands),
but **not** `keydb` storage type with Redis instance.