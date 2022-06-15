package pg

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/log/zerologadapter"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/rs/zerolog"

	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/pkg/conf"
	"github.com/sot-tech/mochi/pkg/log"
	"github.com/sot-tech/mochi/pkg/stop"
	"github.com/sot-tech/mochi/pkg/timecache"
	"github.com/sot-tech/mochi/storage"
)

const (
	// Name is the name by which this peer store is registered with Conf.
	Name = "pg"

	defaultPingQuery = "SELECT 0"
)

var (
	logger = log.NewLogger(Name)

	errConnectionStringNotProvided = errors.New("database address not provided")
	errRequiredParameterNotSetMsg  = "required parameter not provided: %s"

	tc = timecache.New()
)

func init() {
	// Register the storage builder.
	storage.RegisterBuilder(Name, builder)
}

func builder(icfg conf.MapConfig) (storage.PeerStorage, error) {
	var cfg Config

	if err := icfg.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	cfg, err := cfg.Validate()
	if err != nil {
		return nil, err
	}

	con, err := pgxpool.Connect(context.Background(), cfg.ConnectionString)
	if err != nil {
		return nil, err
	}
	con.Config().ConnConfig.Logger = zerologadapter.NewLogger(logger.Logger)
	con.Config().ConnConfig.LogLevel = pgx.LogLevel(logger.GetLevel())

	return &store{Config: &cfg, Pool: con}, nil
}

// Config holds the configuration of a redis PeerStorage.
type Config struct {
	ConnectionString string `cfg:"connection_string"`
	PingQuery        string `cfg:"ping_query"`
	Peer             struct {
		AddQuery      string `cfg:"add_query"`
		DelQuery      string `cfg:"del_query"`
		GraduateQuery string `cfg:"graduate_query"`
		// SELECT COUNT(1) FILTER (WHERE seeder) AS seeders, COUNT(1) FILTER (WHERE NOT seeder) AS leechers FROM peers
		CountQuery string `cfg:"count_query"`
		// WHERE ih = ?
		ByInfoHashClause string `cfg:"by_info_hash_clause"`
	}
	Announce struct {
		Query         string
		PeerIDColumn  string `cfg:"peer_id_column"`
		AddressColumn string `cfg:"address_column"`
		PortColumn    string `cfg:"port_column"`
	}
	Data struct {
		AddQuery string `cfg:"add_query"`
		GetQuery string `cfg:"get_query"`
		DelQuery string `cfg:"del_query"`
	}
	GCQuery            string `cfg:"gc_query"`
	InfoHashCountQuery string `cfg:"info_hash_count_query"`
}

// MarshalZerologObject writes configuration fields into zerolog event
func (cfg Config) MarshalZerologObject(e *zerolog.Event) {
	e.Str("connectionString", cfg.ConnectionString).
		Str("pingQuery", cfg.PingQuery)
}

// Validate sanity checks values set in a config and returns a new config with
// default values replacing anything that is invalid.
//
// This function warns to the logger when a value is changed.
func (cfg Config) Validate() (Config, error) {
	validCfg := cfg
	validCfg.ConnectionString = strings.TrimSpace(validCfg.ConnectionString)
	if len(validCfg.ConnectionString) == 0 {
		return cfg, errConnectionStringNotProvided
	}

	if len(cfg.PingQuery) == 0 {
		validCfg.PingQuery = defaultPingQuery
		logger.Warn().
			Str("name", "PingQuery").
			Str("provided", cfg.PingQuery).
			Str("default", validCfg.PingQuery).
			Msg("falling back to default configuration")
	}

	fn := func(p *string, name string) (err error) {
		if *p = strings.TrimSpace(*p); len(*p) == 0 {
			err = fmt.Errorf(errRequiredParameterNotSetMsg, name)
		}
		return
	}

	if err := fn(&validCfg.Peer.AddQuery, "Peer.AddQuery"); err != nil {
		return cfg, err
	}

	if err := fn(&validCfg.Peer.DelQuery, "Peer.DelQuery"); err != nil {
		return cfg, err
	}

	if err := fn(&validCfg.Peer.GraduateQuery, "Peer.GraduateQuery"); err != nil {
		return cfg, err
	}

	if err := fn(&validCfg.Peer.CountQuery, "Peer.CountQuery"); err != nil {
		return cfg, err
	}

	if err := fn(&validCfg.Peer.ByInfoHashClause, "Peer.ByInfoHashClause"); err != nil {
		return cfg, err
	}

	if err := fn(&validCfg.Announce.Query, "Announce.Query"); err != nil {
		return cfg, err
	}

	if err := fn(&validCfg.Announce.PeerIDColumn, "Announce.PeerIDColumn"); err != nil {
		return cfg, err
	}

	if err := fn(&validCfg.Announce.AddressColumn, "Announce.AddressColumn"); err != nil {
		return cfg, err
	}

	if err := fn(&validCfg.Announce.PortColumn, "Announce.PortColumn"); err != nil {
		return cfg, err
	}

	if err := fn(&validCfg.Data.AddQuery, "Data.AddQuery"); err != nil {
		return cfg, err
	}

	if err := fn(&validCfg.Data.GetQuery, "Data.GetQuery"); err != nil {
		return cfg, err
	}

	if err := fn(&validCfg.Data.DelQuery, "Data.DelQuery"); err != nil {
		return cfg, err
	}

	return validCfg, nil
}

type store struct {
	*Config
	*pgxpool.Pool
}

func (s store) Put(ctx string, values ...storage.Entry) error {
	// TODO implement me
	panic("implement me")
}

func (s store) Contains(ctx string, key string) (bool, error) {
	// TODO implement me
	panic("implement me")
}

func (s store) Load(ctx string, key string) (any, error) {
	// TODO implement me
	panic("implement me")
}

func (s store) Delete(ctx string, keys ...string) error {
	// TODO implement me
	panic("implement me")
}

func (s store) Preservable() bool {
	return true
}

func (s store) GCAware() bool {
	return len(s.GCQuery) > 0
}

func (s store) ScheduleGC(gcInterval, peerLifeTime time.Duration) {
	// TODO implement me
	panic("implement me")
}

func (s store) StatisticsAware() bool {
	return len(s.InfoHashCountQuery) > 0 && len(s.Peer.CountQuery) > 0
}

func (s store) ScheduleStatisticsCollection(reportInterval time.Duration) {
	// TODO implement me
	panic("implement me")
}

func (s store) putPeer(ih bittorrent.InfoHash, peer bittorrent.Peer, seeder bool) error {
	logger.Trace().
		Stringer("infoHash", ih).
		Object("peer", peer).
		Bool("seeder", seeder).
		Msg("put peer")
	args := []interface{}{ih, peer.ID, net.IP(peer.Addr().AsSlice()), peer.Port(), seeder, peer.Addr().Is6()}
	if s.GCAware() {
		args = append(args, tc.Now())
	}
	_, err := s.Exec(context.TODO(), s.Peer.AddQuery, args...)
	return err
}

func (s store) delPeer(ih bittorrent.InfoHash, peer bittorrent.Peer, seeder bool) error {
	logger.Trace().
		Stringer("infoHash", ih).
		Object("peer", peer).
		Bool("seeder", seeder).
		Msg("del peer")
	_, err := s.Exec(context.TODO(), s.Peer.DelQuery, ih, peer.ID, net.IP(peer.Addr().AsSlice()), peer.Port(), seeder)
	return err
}

func (s store) PutSeeder(ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	return s.putPeer(ih, peer, true)
}

func (s store) DeleteSeeder(ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	return s.delPeer(ih, peer, true)
}

func (s store) PutLeecher(ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	return s.putPeer(ih, peer, false)
}

func (s store) DeleteLeecher(ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	return s.delPeer(ih, peer, false)
}

func (s store) GraduateLeecher(ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	logger.Trace().
		Stringer("infoHash", ih).
		Object("peer", peer).
		Msg("graduate leecher")
	_, err := s.Exec(context.TODO(), s.Peer.GraduateQuery, ih, peer.ID, peer.Addr(), peer.Port())
	return err
}

func (s store) getPeers(ih bittorrent.InfoHash, seeders bool, maxCount int, isV6 bool) (peers []bittorrent.Peer, err error) {
	var rows pgx.Rows
	if rows, err = s.Query(context.TODO(), s.Announce.Query, ih, isV6, seeders, maxCount); err == nil {
		defer rows.Close()
		for rows.Next() && len(peers) < maxCount {
			var peer bittorrent.Peer
			var id []byte
			var ip net.IP
			var port int

			if err = rows.Scan(&id, &ip, &port); err == nil {
				if peer.ID, err = bittorrent.NewPeerID(id); err == nil {
					if netAddr, isOk := netip.AddrFromSlice(ip); isOk {
						peer.AddrPort = netip.AddrPortFrom(netAddr, uint16(port))
					} else {
						err = bittorrent.ErrInvalidIP
					}
				}
			}
			if err == nil {
				peers = append(peers, peer)
			} else {
				logger.Warn().
					Err(err).
					Bytes("peerID", id).
					IPAddr("ip", ip).
					Int("port", port).
					Msg("unable to scan/construct peer")
			}
		}
	}
	return
}

func (s store) AnnouncePeers(ih bittorrent.InfoHash, forSeeder bool, numWant int, v6 bool) (peers []bittorrent.Peer, err error) {
	logger.Trace().
		Stringer("infoHash", ih).
		Bool("forSeeder", forSeeder).
		Int("numWant", numWant).
		Bool("v6", v6).
		Msg("announce peers")
	if forSeeder {
		peers, err = s.getPeers(ih, false, numWant, v6)
	} else {
		if peers, err = s.getPeers(ih, true, numWant, v6); err == nil {
			var addPeers []bittorrent.Peer
			addPeers, err = s.getPeers(ih, false, numWant-len(peers), v6)
			peers = append(peers, addPeers...)
		}
	}

	if l := len(peers); err == nil {
		if l == 0 {
			err = storage.ErrResourceDoesNotExist
		}
	} else if l > 0 {
		err = nil
		logger.Warn().Err(err).Stringer("infoHash", ih).Msg("error occurred while retrieving peers")
	}

	return
}

func (s store) ScrapeSwarm(ih bittorrent.InfoHash) (leechers uint32, seeders uint32, snatched uint32) {
	// TODO implement me
	panic("implement me")
}

func (s store) Ping() error {
	_, err := s.Exec(context.TODO(), s.PingQuery)
	return err
}

func (s *store) Stop() stop.Result {
	c := make(stop.Channel)
	s.Close()
	return c.Result()
}

func (s store) MarshalZerologObject(e *zerolog.Event) {
	e.Str("type", Name).Object("config", *s.Config)
}
