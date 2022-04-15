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

func build(conf conf.MapConfig, st storage.Storage) (container.Container, error) {
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
		log.Info("storage context not set, using default value: " + container.DefaultStorageCtxName)
		d.StorageCtx = container.DefaultStorageCtxName
	}
	go func() {
		for event := range d.watcher.Events {
			var mi *metainfo.MetaInfo
			lf := log.Fields{
				"file":   event.TorrentFilePath,
				"v1hash": event.InfoHash,
			}
			if mi, err = metainfo.LoadFromFile(event.TorrentFilePath); err == nil {
				s256 := sha256.New()
				s256.Write(mi.InfoBytes)
				v2hash, _ := bittorrent.NewInfoHash(s256.Sum(nil))
				lf["v2hash"] = v2hash
				lf["v2to1hash"] = v2hash.TruncateV1()
				switch event.Change {
				case dirwatch.Added:
					var name string
					if info, err := mi.UnmarshalInfo(); err == nil {
						name = info.Name
					} else {
						lf["error"] = err
						log.Warn("unable to unmarshal torrent info", lf)
						delete(lf, "error")
					}
					if len(name) == 0 {
						name = list.DUMMY
					}
					if err := d.Storage.BulkPut(d.StorageCtx,
						storage.Entry{
							Key:   event.InfoHash.AsString(),
							Value: name,
						}, storage.Entry{
							Key:   v2hash.RawString(),
							Value: name,
						}, storage.Entry{
							Key:   v2hash.TruncateV1().RawString(),
							Value: name,
						}); err != nil {
						lf["error"] = err
					}
					log.Debug("approval torrent added", lf)
				case dirwatch.Removed:
					if err := d.Storage.Delete(c.StorageCtx,
						event.InfoHash.AsString(),
						v2hash.RawString(),
						v2hash.TruncateV1().RawString(),
					); err != nil {
						lf["error"] = err
					}
					log.Debug("approval torrent deleted", lf)
				}
			} else {
				lf["error"] = err
				log.Error("unable to load torrent file", lf)
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
	d.watcher.Close()
	return stop.AlreadyStopped
}
