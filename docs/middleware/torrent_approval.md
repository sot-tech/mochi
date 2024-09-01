# Approved torrents list

Package `torrentapproval` can be used for only allow or block
specified hashes or block specified hashes.

## Functionality

As said above, there are two modes of approval: white list and black list.

If mode is **white list** (`invert` set to `false`), tracker works in
_semi-private_ mode, which means, than peers can could share and receive info
about only specified list of torrents' hashes.

I.e.: if configuration contains hash `AAAA`, but peer announces hash `BBBB`
tracker will return `unapproved torrent` message back to peer.

If mode is **black list** (`invert` set to `true`), tracker will allow all hashes
**except** specified.

## Hash sources

There are two sources of hashes: `list` and `directory`.

* `list` is the static set of hashes, specified in configuration file.

* `directory` will watch for `*.torrent` files in specified path and
  append/delete records from storage. This source will parse all existing
  files at start and then periodically watch for new files to add, or for delete events
  to remove hash from storage.

Note: if storage is not `memory`, and `preserve` option set to `true`, records
will be persisted in storage until _somebody_ or _something_ (different tool with access
to storage) won't delete it.

## Configuration

This middleware provides the following parameters for configuration:

- `initial_source` - source type: `list` or `directory`
- `storage` - storage configuration to store data, structure is same as global `storage` section.
If `name` is empty or `internal` global storage will be used
- `configuration` - options for specified source
	- `list`:
		- `hash_list` - list of HEX encoded hashes
		- `invert` - working mode: `true` - black list, `false` - white list
		- `storage_ctx` - name of storage _context_ where to store data.
		  It may be redis hash key, DB table name etc.
	- `directory`:
		- `path` - directory to watch
        - `period` - time between two directory checks
		- `invert` and `storage_ctx` has the same meanins as `list`'s options

Configuration example:

An example config might look like this:

```yaml
mochi:
    prehooks:
        -   name: torrent approval
                options:
                    initial_source: list
					storage:
						name: internal
						config:
                    configuration:
                        hash_list: [ "AAA", "BBB" ]
						path: "some/path"
						period: 1m
                        invert: false
                        storage_ctx: APPROVED_HASH
```
