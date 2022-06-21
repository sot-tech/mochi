![Mochi (source image: https://www.flaticon.com/free-icon/mochi_5392004)](mochi.svg)

# Modified Chihaya (MoChi)

[![Build Status](https://github.com/sot-tech/mochi/workflows/Build%20&%20Test/badge.svg)](https://github.com/sot-tech/mochi/actions)
[![Docker Repository on Quay](https://quay.io/repository/eramde/mochi/status "Docker Repository on Quay")](https://quay.io/repository/eramde/mochi)
[![License](https://img.shields.io/badge/license-BSD-blue.svg)](https://opensource.org/licenses/BSD-2-Clause)

Modified version of [Chihaya](https://github.com/chihaya/chihaya), an open source BitTorrent tracker written in Go.

## Differences from the original project

* Supports BittorrentV2 hashes (SHA-256 and _hybrid_
  SHA-256-to-160 [BEP52](https://www.bittorrent.org/beps/bep_0052.html), tested with qBittorrent);
* Supports storage in middleware modules to persist useful data;
* Supports [KeyDB](https://keydb.dev) and [PostgreSQL](https://www.postgresql.org) storages;
* Metrics can be turned off (not enabled till it really needed);
* Allows mixed peers: IPv4 requesters can fetch IPv6 peers or vice versa;
* Contains some internal improvements.

_Note: From time to time MoChi fetch modifications from Chihaya but is not
fully compatible with original project (mainly in Redis storage structure),
so it cannot be mixed with Chihaya (i.e. it is impossible create MoChi-Chihaya cluster)._

## Main goal

The main goal of made modifications is to create semi-private tracker like [Hefur](https://github.com/sot-tech/hefur)
but with cluster support (allowed torrents limited by pre-existent `list` middleware and another `directory` middleware
to [limit registered torrents](docs/middleware/torrent_approval.md)) and to maximize torrent swarm by providing maximum
peers as possible (IPv4+IPv6).

## Notice

Modifications made with particular purpose, so if you need _pure_ tracker, with guaranteed stability, please, use
the [original project](https://github.com/chihaya/chihaya).

Developer(s) of this project (MoChi) do not declare themselves as authors of original project (Chihaya) and do not claim
any other rights of original project.

