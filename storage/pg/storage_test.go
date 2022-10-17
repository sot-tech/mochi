package pg

import (
	"context"
	"fmt"
	"testing"

	s "github.com/sot-tech/mochi/storage"
	"github.com/sot-tech/mochi/storage/test"
)

const (
	createTablesQuery = `
DROP TABLE IF EXISTS mo_peers;
CREATE TABLE mo_peers (
	info_hash bytea NOT NULL,
	peer_id bytea NOT NULL,
	address inet NOT NULL,
	port int4 NOT NULL,
	is_seeder bool NOT NULL,
	is_v6 bool NOT NULL,
	created timestamp NOT NULL DEFAULT current_timestamp,
	PRIMARY KEY (info_hash, peer_id, address, port)
);

CREATE INDEX mo_peers_created_idx ON mo_peers(created);
CREATE INDEX mo_peers_announce_idx ON mo_peers(info_hash, is_seeder, is_v6);

DROP TABLE IF EXISTS mo_kv;
CREATE TABLE mo_kv (
	context varchar NOT NULL,
	name bytea NOT NULL,
	value bytea,
	PRIMARY KEY (context, name)
);
`
)

var cfg = Config{
	ConnectionString: "host=127.0.0.1 database=test user=postgres pool_max_conns=50",
	PingQuery:        "SELECT 1",
	Peer: peerQueryConf{
		AddQuery:            "INSERT INTO mo_peers VALUES(@info_hash, @peer_id, @address, @port, @is_seeder, @is_v6, @created) ON CONFLICT (info_hash, peer_id, address, port) DO UPDATE SET created = EXCLUDED.created, is_seeder = EXCLUDED.is_seeder",
		DelQuery:            "DELETE FROM mo_peers WHERE info_hash=@info_hash AND peer_id=peer_id AND address=@address AND port=@port AND is_seeder=$5",
		GraduateQuery:       "UPDATE mo_peers SET is_seeder=TRUE WHERE info_hash=@info_hash AND peer_id=peer_id AND address=@address AND port=@port AND NOT is_seeder",
		CountQuery:          "SELECT COUNT(1) FILTER (WHERE is_seeder) AS seeders, COUNT(1) FILTER (WHERE NOT is_seeder) AS leechers FROM mo_peers",
		CountSeedersColumn:  "seeders",
		CountLeechersColumn: "leechers",
		ByInfoHashClause:    "WHERE info_hash = $1",
	},
	Announce: announceQueryConf{
		Query:         "SELECT peer_id, address, port FROM mo_peers WHERE info_hash=@info_hash AND is_seeder=@is_seeder AND is_v6=@is_v6 LIMIT @count",
		PeerIDColumn:  "peer_id",
		AddressColumn: "address",
		PortColumn:    "port",
	},
	Data: dataQueryConf{
		AddQuery: "INSERT INTO mo_kv VALUES(@context, @key, @value) ON CONFLICT (context, name) DO NOTHING",
		GetQuery: "SELECT value FROM mo_kv WHERE context=@context AND name=@key",
		DelQuery: "DELETE FROM mo_kv WHERE context=@context AND name IN @key",
	},
	GCQuery:            "DELETE FROM mo_peers WHERE created <= @created",
	InfoHashCountQuery: "SELECT COUNT(DISTINCT info_hash) as info_hashes FROM mo_peers",
}

func createNew() s.PeerStorage {
	var ps s.PeerStorage
	var err error
	ps, err = newStore(cfg)
	if err != nil {
		panic(fmt.Sprint("Unable to create PostgreSQL connection: ", err, "\nThis driver needs real PostgreSQL instance"))
	}
	pss := ps.(*store)
	if _, err = pss.Exec(context.Background(), createTablesQuery); err != nil {
		panic(fmt.Sprint("Unable to create test PostgreSQL tables: ", err))
	}
	return ps
}

func TestStorage(t *testing.T) { test.RunTests(t, createNew()) }

func BenchmarkStorage(b *testing.B) { test.RunBenchmarks(b, createNew) }
