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
	port int2 NOT NULL,
	is_seeder bool NOT NULL,
	is_v6 bool NOT NULL,
	created timestamp NOT NULL DEFAULT current_timestamp,
	UNIQUE(info_hash, peer_id, address, port)
);

CREATE INDEX peers_ih_idx ON mo_peers(info_hash);
CREATE INDEX peers_created_idx ON mo_peers(created);
CREATE INDEX peers_announce_idx ON mo_peers(info_hash, is_seeder, is_v6);

DROP TABLE IF EXISTS mo_kv;
CREATE TABLE mo_kv (
	context varchar NOT NULL,
	name varchar NOT NULL,
	value bytea,
	UNIQUE (context, name)
);
`
)

var cfg = Config{
	ConnectionString: "host=127.0.0.1 database=test user=postgres",
	PingQuery:        "SELECT 1",
	Peer: peerQueryConf{
		AddQuery:            "INSERT INTO mo_peers VALUES($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (info_hash, peer_id, address, port) DO UPDATE SET created = EXCLUDED.created, is_seeder = EXCLUDED.is_seeder",
		DelQuery:            "DELETE FROM mo_peers WHERE info_hash=$1 AND peer_id=$2 AND address=$3 AND port=$4 AND is_seeder=$5",
		GraduateQuery:       "UPDATE mo_peers SET is_seeder=TRUE WHERE info_hash=$1 AND peer_id=$2 AND address=$3 AND port=$4 AND NOT is_seeder",
		CountQuery:          "SELECT COUNT(1) FILTER (WHERE is_seeder) AS seeders, COUNT(1) FILTER (WHERE NOT is_seeder) AS leechers FROM mo_peers",
		CountSeedersColumn:  "seeders",
		CountLeechersColumn: "leechers",
		ByInfoHashClause:    "WHERE info_hash = $1",
	},
	Announce: announceQueryConf{
		Query:         "SELECT peer_id, address, port FROM mo_peers WHERE info_hash=$1 AND is_seeder=$2 AND is_v6=$3 LIMIT $4",
		PeerIDColumn:  "peer_id",
		AddressColumn: "address",
		PortColumn:    "port",
	},
	Data: dataQueryConf{
		AddQuery: "INSERT INTO mo_kv VALUES($1, $2, ($3)::bytea) ON CONFLICT (context, name) DO NOTHING",
		GetQuery: "SELECT value FROM mo_kv WHERE context=$1 AND name=$2",
		DelQuery: "DELETE FROM mo_kv WHERE context=$1 AND name=$2",
	},
	GCQuery:            "DELETE FROM mo_peers WHERE created > $1",
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
