// Package directory implements container which
// checks if hash present in any of torrent file
// placed in some directory.
// Note: Unlike List, this container also stores torrent name as value
package directory

import (
	"fmt"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/util/dirwatch"
	"github.com/minio/sha256-simd"

	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/middleware/torrentapproval/container"
	"github.com/sot-tech/mochi/middleware/torrentapproval/container/list"
	"github.com/sot-tech/mochi/pkg/conf"
	"github.com/sot-tech/mochi/pkg/log"
	"github.com/sot-tech/mochi/pkg/stop"
	"github.com/sot-tech/mochi/storage"
)

// Name of this container for registry
const Name = "directory"

var logger = log.NewLogger("torrent approval directory")

func init() {
	container.Register(Name, build)
}

// Config - implementation of directory container configuration.
// Extends list.Config because uses the same storage and Approved function.
type Config struct {
	list.Config
	// Path in filesystem where torrent files stored and should be watched
	Path string
}

func build(conf conf.MapConfig, st storage.DataStorage) (container.Container, error) {
	c := new(Config)
	if err := conf.Unmarshal(c); err != nil {
		return nil, fmt.Errorf("unable to deserialise configuration: %w", err)
	}
	var err error
	d := &directory{
		List: list.List{
			Invert:     c.Invert,
			Storage:    st,
			StorageCtx: c.StorageCtx,
		},
		watcher: nil,
	}
	var w *dirwatch.Instance
	if w, err = dirwatch.New(c.Path); err != nil {
		return nil, fmt.Errorf("unable to initialize directory watch: %w", err)
	}
	d.watcher = w
	if len(d.StorageCtx) == 0 {
		logger.Warn().
			Str("name", "StorageCtx").
			Str("provided", d.StorageCtx).
			Str("default", container.DefaultStorageCtxName).
			Msg("falling back to default configuration")
		d.StorageCtx = container.DefaultStorageCtxName
	}
	go func() {
		for event := range d.watcher.Events {
			var mi *metainfo.MetaInfo
			if mi, err = metainfo.LoadFromFile(event.TorrentFilePath); err == nil {
				s256 := sha256.New()
				s256.Write(mi.InfoBytes)
				v2hash, _ := bittorrent.NewInfoHash(s256.Sum(nil))
				switch event.Change {
				case dirwatch.Added:
					var name string
					if info, err := mi.UnmarshalInfo(); err == nil {
						name = info.Name
					} else {
						logger.Error().
							Err(err).
							Str("file", event.TorrentFilePath).
							Stringer("infoHash", event.InfoHash).
							Stringer("infoHashV2", v2hash).
							Msg("unable to unmarshal torrent info")
					}
					if len(name) == 0 {
						name = list.DUMMY
					}
					bName := []byte(name)
					logger.Err(d.Storage.Put(d.StorageCtx,
						storage.Entry{
							Key:   event.InfoHash.AsString(),
							Value: bName,
						}, storage.Entry{
							Key:   v2hash.RawString(),
							Value: bName,
						}, storage.Entry{
							Key:   v2hash.TruncateV1().RawString(),
							Value: bName,
						})).
						Str("action", "add").
						Str("file", event.TorrentFilePath).
						Stringer("infoHash", event.InfoHash).
						Stringer("infoHashV2", v2hash).
						Msg("approval torrent watcher event")
				case dirwatch.Removed:
					logger.Err(d.Storage.Delete(c.StorageCtx,
						event.InfoHash.AsString(),
						v2hash.RawString(),
						v2hash.TruncateV1().RawString(),
					)).
						Str("action", "delete").
						Str("file", event.TorrentFilePath).
						Stringer("infoHash", event.InfoHash).
						Stringer("infoHashV2", v2hash).
						Msg("approval torrent watcher event")
				}
			} else {
				logger.Error().Err(err).
					Str("file", event.TorrentFilePath).
					Msg("unable to load torrent file")
			}
		}
	}()
	return d, err
}

type directory struct {
	list.List
	watcher *dirwatch.Instance
}

// Stop closes watching of torrent directory
func (d *directory) Stop() stop.Result {
	st := make(stop.Channel)
	d.watcher.Close()
	st.Done()
	return st.Result()
}
