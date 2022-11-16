// Package pg implements PostgreSQL-like storage interface.
// This implementation does not use ORM and relies on database structure
// and queries provided in configuration.
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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/pkg/conf"
	"github.com/sot-tech/mochi/pkg/log"
	"github.com/sot-tech/mochi/pkg/metrics"
	"github.com/sot-tech/mochi/pkg/timecache"
	"github.com/sot-tech/mochi/storage"
)

const (
	defaultPingQuery = "SELECT 0"

	errRequiredParameterNotSetMsg = "required parameter not provided: %s"
	errRequiredColumnsNotFoundMsg = "one or more required columns not found in result set: %v"
	errRollBackMsg                = "error occurred while rolling back failed query: %v, failed query error: %v"

	pCtx      = "context"
	pKey      = "key"
	pValue    = "value"
	pInfoHash = "info_hash"
	pPeerID   = "peer_id"
	pAddress  = "address"
	pPort     = "port"
	pV6       = "is_v6"
	pSeeder   = "is_seeder"
	pCreated  = "created"
	pCount    = "count"
)

var (
	logger                         = log.NewLogger("storage/pg")
	errConnectionStringNotProvided = errors.New("database connection string not provided")
)

func init() {
	// Register the storage builder.
	storage.RegisterDriver("pg", builder)
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

	con, err := pgxpool.New(context.Background(), cfg.ConnectionString)
	if err != nil {
		return nil, err
	}

	return &store{
		Config:     cfg,
		Pool:       con,
		wg:         sync.WaitGroup{},
		closed:     make(chan any),
		onceCloser: sync.Once{},
	}, nil
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

type downloadQueryConf struct {
	GetQuery       string `cfg:"get_query"`
	IncrementQuery string `cfg:"inc_query"`
}

// Config holds the configuration of a redis PeerStorage.
type Config struct {
	ConnectionString   string `cfg:"connection_string"`
	PingQuery          string `cfg:"ping_query"`
	Peer               peerQueryConf
	Announce           announceQueryConf
	Downloads          downloadQueryConf
	Data               dataQueryConf
	GCQuery            string `cfg:"gc_query"`
	InfoHashCountQuery string `cfg:"info_hash_count_query"`
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
	wg         sync.WaitGroup
	closed     chan any
	onceCloser sync.Once
}

func (s *store) txBatch(ctx context.Context, batch *pgx.Batch) (err error) {
	var tx pgx.Tx
	if tx, err = s.Begin(ctx); err == nil {
		if err = tx.SendBatch(ctx, batch).Close(); err == nil {
			err = tx.Commit(ctx)
		} else {
			if txErr := tx.Rollback(ctx); txErr != nil {
				err = fmt.Errorf(errRollBackMsg, txErr, err)
			}
		}
	}
	return
}

func (s *store) Put(ctx context.Context, storeCtx string, values ...storage.Entry) (err error) {
	switch len(values) {
	case 0:
		// ignore
	case 1:
		_, err = s.Exec(ctx, s.Data.AddQuery, pgx.NamedArgs{pCtx: storeCtx, pKey: []byte(values[0].Key), pValue: values[0].Value})
	default:
		var batch pgx.Batch
		for _, v := range values {
			batch.Queue(s.Data.AddQuery, pgx.NamedArgs{pCtx: storeCtx, pKey: []byte(v.Key), pValue: v.Value})
		}
		err = s.txBatch(ctx, &batch)
	}
	return
}

func (s *store) Contains(ctx context.Context, storeCtx string, key string) (contains bool, err error) {
	var rows pgx.Rows
	if rows, err = s.Query(ctx, s.Data.GetQuery, pgx.NamedArgs{pCtx: storeCtx, pKey: []byte(key)}); err == nil {
		defer rows.Close()
		contains = rows.Next()
		err = rows.Err()
	}
	return
}

func (s *store) Load(ctx context.Context, storeCtx string, key string) (out []byte, err error) {
	if err = s.QueryRow(ctx, s.Data.GetQuery, pgx.NamedArgs{pCtx: storeCtx, pKey: []byte(key)}).Scan(&out); errors.Is(err, pgx.ErrNoRows) {
		err = nil
	}
	return
}

func (s *store) Delete(ctx context.Context, storeCtx string, keys ...string) (err error) {
	if len(keys) > 0 {
		baKeys := make([][]byte, len(keys))
		for i, k := range keys {
			baKeys[i] = []byte(k)
		}
		_, err = s.Exec(ctx, s.Data.DelQuery, pgx.NamedArgs{pCtx: storeCtx, pKey: baKeys})
	}
	return
}

func (s *store) Preservable() bool {
	return true
}

func (s *store) ScheduleGC(gcInterval, peerLifeTime time.Duration) {
	if len(s.GCQuery) == 0 {
		return
	}
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
				_, err := s.Exec(context.Background(), s.GCQuery, pgx.NamedArgs{pCreated: time.Now().Add(-peerLifeTime)})
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

func (s *store) ScheduleStatisticsCollection(reportInterval time.Duration) {
	if len(s.InfoHashCountQuery) == 0 {
		return
	}
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
					sc, lc := s.countPeers(context.Background(), nil)
					var hc int
					if err := s.QueryRow(context.Background(), s.InfoHashCountQuery).Scan(&hc); err != nil && !errors.Is(err, pgx.ErrNoRows) {
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

func (s *store) putPeer(ctx context.Context, ih bittorrent.InfoHash, peer bittorrent.Peer, seeder bool) (err error) {
	logger.Trace().
		Stringer("infoHash", ih).
		Object("peer", peer).
		Bool("seeder", seeder).
		Msg("put peer")
	_, err = s.Exec(ctx, s.Peer.AddQuery, pgx.NamedArgs{
		pInfoHash: []byte(ih),
		pPeerID:   peer.ID[:],
		pAddress:  net.IP(peer.Addr().AsSlice()),
		pPort:     peer.Port(),
		pSeeder:   seeder,
		pV6:       peer.Addr().Is6(),
		pCreated:  timecache.Now(),
	})
	return
}

func (s *store) delPeer(ctx context.Context, ih bittorrent.InfoHash, peer bittorrent.Peer, seeder bool) (err error) {
	logger.Trace().
		Stringer("infoHash", ih).
		Object("peer", peer).
		Msg("del peer")
	_, err = s.Exec(ctx, s.Peer.DelQuery, pgx.NamedArgs{
		pInfoHash: []byte(ih),
		pPeerID:   peer.ID[:],
		pAddress:  net.IP(peer.Addr().AsSlice()),
		pPort:     peer.Port(),
		pSeeder:   seeder,
	})
	return
}

func (s *store) PutSeeder(ctx context.Context, ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	return s.putPeer(ctx, ih, peer, true)
}

func (s *store) DeleteSeeder(ctx context.Context, ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	return s.delPeer(ctx, ih, peer, true)
}

func (s *store) PutLeecher(ctx context.Context, ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	return s.putPeer(ctx, ih, peer, false)
}

func (s *store) DeleteLeecher(ctx context.Context, ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	return s.delPeer(ctx, ih, peer, false)
}

func (s *store) GraduateLeecher(ctx context.Context, ih bittorrent.InfoHash, peer bittorrent.Peer) error {
	logger.Trace().
		Stringer("infoHash", ih).
		Object("peer", peer).
		Msg("graduate leecher")
	var batch pgx.Batch
	ihb := []byte(ih)
	batch.Queue(s.Peer.GraduateQuery, pgx.NamedArgs{
		pInfoHash: ihb,
		pPeerID:   peer.ID[:],
		pAddress:  net.IP(peer.Addr().AsSlice()),
		pPort:     peer.Port(),
	})
	batch.Queue(s.Downloads.IncrementQuery, pgx.NamedArgs{pInfoHash: ihb})
	return s.txBatch(ctx, &batch)
}

func (s *store) getPeers(ctx context.Context, ih bittorrent.InfoHash, seeders bool, maxCount int, isV6 bool) (peers []bittorrent.Peer, err error) {
	var rows pgx.Rows
	if rows, err = s.Query(ctx, s.Announce.Query, pgx.NamedArgs{
		pInfoHash: []byte(ih),
		pSeeder:   seeders,
		pV6:       isV6,
		pCount:    maxCount,
	}); err == nil {
		defer rows.Close()
		idIndex, ipIndex, portIndex := -1, -1, -1
		for i, field := range rows.FieldDescriptions() {
			name := strings.ToUpper(field.Name)
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
			err = fmt.Errorf(errRequiredColumnsNotFoundMsg, []string{
				s.Announce.PeerIDColumn,
				s.Announce.AddressColumn,
				s.Announce.PortColumn,
			})
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

func (s *store) AnnouncePeers(ctx context.Context, ih bittorrent.InfoHash, forSeeder bool, numWant int, v6 bool) (peers []bittorrent.Peer, err error) {
	logger.Trace().
		Stringer("infoHash", ih).
		Bool("forSeeder", forSeeder).
		Int("numWant", numWant).
		Bool("v6", v6).
		Msg("announce peers")
	if forSeeder {
		peers, err = s.getPeers(ctx, ih, false, numWant, v6)
	} else {
		if peers, err = s.getPeers(ctx, ih, true, numWant, v6); err == nil {
			var addPeers []bittorrent.Peer
			addPeers, err = s.getPeers(ctx, ih, false, numWant-len(peers), v6)
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

func (s *store) countPeers(ctx context.Context, ih []byte) (seeders uint32, leechers uint32) {
	var rows pgx.Rows
	var err error
	if len(ih) == 0 {
		rows, err = s.Query(ctx, s.Peer.CountQuery)
	} else {
		rows, err = s.Query(ctx, s.Peer.CountQuery+" "+s.Peer.ByInfoHashClause, pgx.NamedArgs{pInfoHash: ih})
	}
	if err == nil {
		defer rows.Close()
		if rows.Next() {
			si, li := -1, -1
			for i, field := range rows.FieldDescriptions() {
				name := strings.ToUpper(field.Name)
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
		logger.Error().Err(err).Bytes("infoHash", ih).Msg("unable to get peers count")
	}
	return
}

func (s *store) ScrapeSwarm(ctx context.Context, ih bittorrent.InfoHash) (leechers uint32, seeders uint32, snatched uint32) {
	logger.Trace().
		Stringer("infoHash", ih).
		Msg("scrape swarm")
	ihb := []byte(ih)
	seeders, leechers = s.countPeers(ctx, ihb)
	if len(s.Downloads.GetQuery) > 0 {
		if err := s.QueryRow(ctx, s.Downloads.GetQuery, pgx.NamedArgs{pInfoHash: ihb}).Scan(&snatched); err != nil && !errors.Is(err, pgx.ErrNoRows) {
			logger.Error().Stringer("infoHash", ih).Err(err).Msg("error occurred while get info downloads count")
		}
	}

	return
}

func (s *store) Ping(ctx context.Context) error {
	_, err := s.Exec(ctx, s.PingQuery)
	return err
}

func (s *store) Close() error {
	go func() {
		close(s.closed)
		s.wg.Wait()
		logger.Info().Msg("pg exiting. mochi does not clear data in database when exiting.")
		s.Pool.Close()
	}()
	return nil
}
