package pg

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/log/zerologadapter"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/rs/zerolog"

	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/pkg/conf"
	"github.com/sot-tech/mochi/pkg/log"
	"github.com/sot-tech/mochi/pkg/metrics"
	"github.com/sot-tech/mochi/pkg/stop"
	"github.com/sot-tech/mochi/pkg/timecache"
	"github.com/sot-tech/mochi/storage"
)

const (
	// Name is the name by which this peer store is registered with Conf.
	Name = "pg"

	defaultPingQuery = "SELECT 0"

	errRequiredParameterNotSetMsg = "required parameter not provided: %s"
	errRequiredColumnsNotFoundMsg = "one or more required columns not found in result set: %v"
	errRollBackMsg                = "error occurred while rolling back failed query: %v, failed query error: %v"
)

var (
	logger = log.NewLogger(Name)

	errConnectionStringNotProvided = errors.New("database connection string not provided")
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

	return newStore(cfg)
}

func newStore(cfg Config) (storage.PeerStorage, error) {
	cfg, err := cfg.Validate()
	if err != nil {
		return nil, err
	}

	pgConf, err := pgxpool.ParseConfig(cfg.ConnectionString)
	if err != nil {
		return nil, err
	}

	pgConf.ConnConfig.Logger = zerologadapter.NewLogger(logger.Logger)
	con, err := pgxpool.Connect(context.Background(), cfg.ConnectionString)
	if err != nil {
		return nil, err
	}

	return &store{Config: cfg, Pool: con, wg: sync.WaitGroup{}, closed: make(chan any)}, nil
}

type peerQueryConf struct {
	AddQuery            string `cfg:"add_query"`
	DelQuery            string `cfg:"del_query"`
	GraduateQuery       string `cfg:"graduate_query"`
	CountQuery          string `cfg:"count_query"`
	CountSeedersColumn  string `cfg:"count_seeders_column"`
	CountLeechersColumn string `cfg:"count_leechers_column"`
	ByInfoHashClause    string `cfg:"by_info_hash_clause"`
}

type announceQueryConf struct {
	Query         string
	PeerIDColumn  string `cfg:"peer_id_column"`
	AddressColumn string `cfg:"address_column"`
	PortColumn    string `cfg:"port_column"`
}

type dataQueryConf struct {
	AddQuery string `cfg:"add_query"`
	GetQuery string `cfg:"get_query"`
	DelQuery string `cfg:"del_query"`
}

// Config holds the configuration of a redis PeerStorage.
type Config struct {
	ConnectionString   string `cfg:"connection_string"`
	PingQuery          string `cfg:"ping_query"`
	Peer               peerQueryConf
	Announce           announceQueryConf
	Data               dataQueryConf
	GCQuery            string `cfg:"gc_query"`
	InfoHashCountQuery string `cfg:"info_hash_count_query"`
}

// MarshalZerologObject writes configuration fields into zerolog event
func (cfg Config) MarshalZerologObject(e *zerolog.Event) {
	e.Str("connectionString", "<hidden>").
		Str("pingQuery", cfg.PingQuery).
		Str("peer.addQuery", cfg.Peer.AddQuery).
		Str("peer.delQuery", cfg.Peer.DelQuery).
		Str("peer.graduateQuery", cfg.Peer.GraduateQuery).
		Str("peer.countQuery", cfg.Peer.CountQuery).
		Str("peer.countSeedersColumn", cfg.Peer.CountSeedersColumn).
		Str("peer.countLeechersColumn", cfg.Peer.CountLeechersColumn).
		Str("peer.byInfoHashClause", cfg.Peer.ByInfoHashClause).
		Str("announce.query", cfg.Announce.Query).
		Str("announce.peerIDColumn", cfg.Announce.PeerIDColumn).
		Str("announce.addressColumn", cfg.Announce.AddressColumn).
		Str("announce.portColumn", cfg.Announce.PortColumn).
		Str("data.addQuery", cfg.Data.AddQuery).
		Str("data.getQuery", cfg.Data.GetQuery).
		Str("data.delQuery", cfg.Data.DelQuery).
		Str("gcQuery", cfg.GCQuery).
		Str("infoHashCountQuery", cfg.InfoHashCountQuery)
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

	if err := fn(&validCfg.Peer.AddQuery, "peer.addQuery"); err != nil {
		return cfg, err
	}

	if err := fn(&validCfg.Peer.DelQuery, "peer.aelQuery"); err != nil {
		return cfg, err
	}

	if err := fn(&validCfg.Peer.GraduateQuery, "peer.graduateQuery"); err != nil {
		return cfg, err
	}

	if err := fn(&validCfg.Peer.CountQuery, "peer.countQuery"); err != nil {
		return cfg, err
	}

	if err := fn(&validCfg.Peer.CountSeedersColumn, "peer.countSeedersColumn"); err != nil {
		return cfg, err
	}

	if err := fn(&validCfg.Peer.CountLeechersColumn, "peer.countLeechersColumn"); err != nil {
		return cfg, err
	}

	if err := fn(&validCfg.Peer.ByInfoHashClause, "peer.byInfoHashClause"); err != nil {
		return cfg, err
	}

	if err := fn(&validCfg.Announce.Query, "announce.query"); err != nil {
		return cfg, err
	}

	if err := fn(&validCfg.Announce.PeerIDColumn, "announce.peerIDColumn"); err != nil {
		return cfg, err
	}

	if err := fn(&validCfg.Announce.AddressColumn, "announce.addressColumn"); err != nil {
		return cfg, err
	}

	if err := fn(&validCfg.Announce.PortColumn, "announce.portColumn"); err != nil {
		return cfg, err
	}

	if err := fn(&validCfg.Data.AddQuery, "data.addQuery"); err != nil {
		return cfg, err
	}

	if err := fn(&validCfg.Data.GetQuery, "data.getQuery"); err != nil {
		return cfg, err
	}

	if err := fn(&validCfg.Data.DelQuery, "data.delQuery"); err != nil {
		return cfg, err
	}

	validCfg.Announce.PeerIDColumn = strings.ToUpper(validCfg.Announce.PeerIDColumn)
	validCfg.Announce.AddressColumn = strings.ToUpper(validCfg.Announce.AddressColumn)
	validCfg.Announce.PortColumn = strings.ToUpper(validCfg.Announce.PortColumn)

	validCfg.Peer.CountSeedersColumn = strings.ToUpper(validCfg.Peer.CountSeedersColumn)
	validCfg.Peer.CountLeechersColumn = strings.ToUpper(validCfg.Peer.CountLeechersColumn)

	return validCfg, nil
}

type store struct {
	Config
	*pgxpool.Pool
	wg     sync.WaitGroup
	closed chan any
}

func (s *store) Put(ctx string, values ...storage.Entry) (err error) {
	var tx pgx.Tx
	if tx, err = s.Begin(context.TODO()); err == nil {
		for _, v := range values {
			val := v.Value
			switch tOut := val.(type) {
			case string:
				val = []byte(tOut)
			}
			if _, err = tx.Exec(context.TODO(), s.Data.AddQuery, ctx, []byte(v.Key), val); err != nil {
				break
			}
		}
		if err == nil {
			err = tx.Commit(context.TODO())
		} else {
			if txErr := tx.Rollback(context.TODO()); txErr != nil {
				err = fmt.Errorf(errRollBackMsg, txErr, err)
			}
		}
	}
	return
}

func (s *store) Contains(ctx string, key string) (contains bool, err error) {
	var rows pgx.Rows
	if rows, err = s.Query(context.TODO(), s.Data.GetQuery, ctx, []byte(key)); err == nil {
		defer rows.Close()
		contains = rows.Next()
	}
	return
}

func (s *store) Load(ctx string, key string) (out any, err error) {
	var rows pgx.Rows
	if rows, err = s.Query(context.TODO(), s.Data.GetQuery, ctx, []byte(key)); err == nil {
		defer rows.Close()
		if rows.Next() {
			var values []any
			if values, err = rows.Values(); err == nil && len(values) > 0 {
				out = values[0]
				switch tOut := out.(type) {
				case []byte:
					out = string(tOut)
				}
			}
		}
	}
	return
}

func (s *store) Delete(ctx string, keys ...string) (err error) {
	var tx pgx.Tx
	if tx, err = s.Begin(context.TODO()); err == nil {
		for _, k := range keys {
			if _, err = tx.Exec(context.TODO(), s.Data.DelQuery, ctx, []byte(k)); err != nil {
				break
			}
		}
		if err == nil {
			err = tx.Commit(context.TODO())
		} else {
			if txErr := tx.Rollback(context.TODO()); txErr != nil {
				err = fmt.Errorf(errRollBackMsg, txErr, err)
			}
		}
	}
	return
}

func (s *store) Preservable() bool {
	return true
}

func (s *store) GCAware() bool {
	return len(s.GCQuery) > 0
}

func (s *store) ScheduleGC(gcInterval, peerLifeTime time.Duration) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		t := time.NewTimer(gcInterval)
		defer t.Stop()
		for {
			select {
			case <-s.closed:
				return
			case <-t.C:
				start := time.Now()
				_, err := s.Exec(context.Background(), s.GCQuery, time.Now().Add(-peerLifeTime))
				duration := time.Since(start)
				if err != nil {
					logger.Error().Err(err).Msg("error occurred while GC")
				} else {
					logger.Debug().Dur("timeTaken", duration).Msg("GC complete")
				}
				storage.PromGCDurationMilliseconds.Observe(float64(duration.Milliseconds()))
				t.Reset(gcInterval)
			}
		}
	}()
}

func (s *store) StatisticsAware() bool {
	return len(s.InfoHashCountQuery) > 0
}

func (s *store) ScheduleStatisticsCollection(reportInterval time.Duration) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		t := time.NewTicker(reportInterval)
		for {
			select {
			case <-s.closed:
				t.Stop()
				return
			case <-t.C:
				if metrics.Enabled() {
					before := time.Now()
					sc, lc := s.countPeers(bittorrent.NoneInfoHash)
					var hc int
					row := s.QueryRow(context.Background(), s.InfoHashCountQuery)
					if err := row.Scan(&hc); err != nil && !errors.Is(err, pgx.ErrNoRows) {
						logger.Error().Err(err).Msg("error occurred while get info hash count")
					}

					storage.PromInfoHashesCount.Set(float64(hc))
					storage.PromSeedersCount.Set(float64(sc))
					storage.PromLeechersCount.Set(float64(lc))
					logger.Debug().TimeDiff("timeTaken", time.Now(), before).Msg("populate prom complete")
				}
			}
		}
	}()
}

func (s *store) putPeer(ih bittorrent.InfoHash, peer bittorrent.Peer, seeder bool) error {
	logger.Trace().
		Stringer("infoHash", ih).
		Object("peer", peer).
		Bool("seeder", seeder).
		Msg("put peer")
	args := []any{[]byte(ih), peer.ID[:], net.IP(peer.Addr().AsSlice()), peer.Port(), seeder, peer.Addr().Is6()}
	if s.GCAware() {
		args = append(args, timecache.Now())
	}
	_, err := s.Exec(context.TODO(), s.Peer.AddQuery, args...)
	return err
}

func (s *store) delPeer(ih bittorrent.InfoHash, peer bittorrent.Peer, seeder bool) error {
	logger.Trace().
		Stringer("infoHash", ih).
		Object("peer", peer).
		Msg("del peer")
	_, err := s.Exec(context.TODO(), s.Peer.DelQuery, []byte(ih), peer.ID[:], net.IP(peer.Addr().AsSlice()), peer.Port(), seeder)
	return err
}

func (s *store) PutSeeder(ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	return s.putPeer(ih, peer, true)
}

func (s *store) DeleteSeeder(ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	return s.delPeer(ih, peer, true)
}

func (s *store) PutLeecher(ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	return s.putPeer(ih, peer, false)
}

func (s *store) DeleteLeecher(ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	return s.delPeer(ih, peer, false)
}

func (s *store) GraduateLeecher(ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	logger.Trace().
		Stringer("infoHash", ih).
		Object("peer", peer).
		Msg("graduate leecher")
	_, err := s.Exec(context.TODO(), s.Peer.GraduateQuery, []byte(ih), peer.ID[:], net.IP(peer.Addr().AsSlice()), peer.Port())
	return err
}

func (s *store) getPeers(ih bittorrent.InfoHash, seeders bool, maxCount int, isV6 bool) (peers []bittorrent.Peer, err error) {
	var rows pgx.Rows
	if rows, err = s.Query(context.TODO(), s.Announce.Query, []byte(ih), seeders, isV6, maxCount); err == nil {
		defer rows.Close()
		idIndex, ipIndex, portIndex := -1, -1, -1
		for i, field := range rows.FieldDescriptions() {
			name := strings.ToUpper(string(field.Name))
			switch name {
			case s.Announce.PeerIDColumn:
				idIndex = i
			case s.Announce.AddressColumn:
				ipIndex = i
			case s.Announce.PortColumn:
				portIndex = i
			}
		}
		if idIndex < 0 || ipIndex < 0 || portIndex < 0 {
			err = fmt.Errorf(errRequiredColumnsNotFoundMsg, []string{s.Announce.PeerIDColumn, s.Announce.AddressColumn, s.Announce.PortColumn})
			return
		}
		var maxIndex int
		switch {
		case idIndex >= ipIndex && idIndex >= portIndex:
			maxIndex = idIndex
		case ipIndex >= idIndex && ipIndex >= portIndex:
			maxIndex = ipIndex
		case portIndex >= idIndex && portIndex >= ipIndex:
			maxIndex = portIndex
		}

		for rows.Next() && len(peers) < maxCount {
			var peer bittorrent.Peer
			var id []byte
			var ip net.IP
			var port int
			into := make([]any, maxIndex+1)
			into[idIndex], into[ipIndex], into[portIndex] = &id, &ip, &port

			if err = rows.Scan(into...); err == nil {
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

func (s *store) AnnouncePeers(ih bittorrent.InfoHash, forSeeder bool, numWant int, v6 bool) (peers []bittorrent.Peer, err error) {
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

func (s *store) countPeers(ih bittorrent.InfoHash) (seeders int, leechers int) {
	var rows pgx.Rows
	var err error
	if ih == bittorrent.NoneInfoHash {
		rows, err = s.Query(context.TODO(), s.Peer.CountQuery)
	} else {
		rows, err = s.Query(context.TODO(), s.Peer.CountQuery+" "+s.Peer.ByInfoHashClause, []byte(ih))
	}
	if err == nil {
		defer rows.Close()
		if rows.Next() {
			si, li := -1, -1
			for i, field := range rows.FieldDescriptions() {
				name := strings.ToUpper(string(field.Name))
				switch name {
				case s.Peer.CountSeedersColumn:
					si = i
				case s.Peer.CountLeechersColumn:
					li = i
				}
			}
			if si < 0 || li < 0 {
				err = fmt.Errorf(errRequiredColumnsNotFoundMsg, []string{s.Peer.CountSeedersColumn, s.Peer.CountLeechersColumn})
			} else {
				var mi int
				if si > li {
					mi = si
				} else {
					mi = li
				}
				into := make([]any, mi+1)
				into[si], into[li] = &seeders, &leechers

				err = rows.Scan(into...)
			}
		}
	}
	if err != nil {
		logger.Error().Err(err).Stringer("infoHash", ih).Msg("unable to get peers count")
	}
	return
}

func (s *store) ScrapeSwarm(ih bittorrent.InfoHash) (leechers uint32, seeders uint32, snatched uint32) {
	logger.Trace().
		Stringer("infoHash", ih).
		Msg("scrape swarm")
	sc, lc := s.countPeers(ih)
	seeders, leechers = uint32(sc), uint32(lc)
	return
}

func (s *store) Ping() error {
	_, err := s.Exec(context.TODO(), s.PingQuery)
	return err
}

func (s *store) Stop() stop.Result {
	c := make(stop.Channel)
	go func() {
		if s.closed != nil {
			close(s.closed)
		}
		s.wg.Wait()
		if s.Pool != nil {
			logger.Info().Msg("pg exiting. mochi does not clear data in database when exiting.")
			s.Close()
			s.Pool = nil
		}
		c.Done()
	}()
	return c.Result()
}

func (s *store) MarshalZerologObject(e *zerolog.Event) {
	e.Str("type", Name).Object("config", s.Config)
}
