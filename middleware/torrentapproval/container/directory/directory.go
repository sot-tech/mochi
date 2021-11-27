// Package directory implements container which
// checks if hash present in any of torrent file
// placed in some directory.
// Note: Unlike List, this container also stores torrent name as value
package directory

import (
	"fmt"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/util/dirwatch"
	"github.com/chihaya/chihaya/bittorrent"
	"github.com/chihaya/chihaya/middleware/torrentapproval/container"
	"github.com/chihaya/chihaya/middleware/torrentapproval/container/list"
	"github.com/chihaya/chihaya/pkg/log"
	"github.com/chihaya/chihaya/pkg/stop"
	"github.com/chihaya/chihaya/storage"
	"github.com/minio/sha256-simd"
	"gopkg.in/yaml.v2"
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
	Path string `yaml:"path"`
}

func build(confBytes []byte, st storage.Storage) (container.Container, error) {
	c := new(Config)
	if err := yaml.Unmarshal(confBytes, c); err != nil {
		return nil, fmt.Errorf("unable to deserialise configuration: %v", err)
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
		return nil, fmt.Errorf("unable to initialize directory watch: %v", err)
	}
	d.watcher = w
	if len(d.StorageCtx) == 0 {
		log.Info("storage context not set, using default value: " + container.DefaultStorageCtxName)
		d.StorageCtx = container.DefaultStorageCtxName
	}
	go func() {
		for event := range d.watcher.Events {
			var mi *metainfo.MetaInfo
			if mi, err = metainfo.LoadFromFile(event.TorrentFilePath); err == nil {
				s256 := sha256.New()
				s256.Write(mi.InfoBytes)
				v2hash, _ := bittorrent.NewInfoHash(s256.Sum(nil))
				lf := log.Fields{
					"file":      event.TorrentFilePath,
					"v1hash":    event.InfoHash.String(),
					"v2hash":    v2hash.String(),
					"v2to1hash": v2hash.TruncateV1().String(),
				}
				switch event.Change {
				case dirwatch.Added:
					var name string
					if info, err := mi.UnmarshalInfo(); err == nil {
						name = info.Name
					} else {
						log.Warn(err)
					}
					if len(name) == 0 {
						name = list.DUMMY
					}
					d.Storage.BulkPut(d.StorageCtx,
						storage.Pair{
							Left:  event.InfoHash.AsString(),
							Right: name,
						}, storage.Pair{
							Left:  v2hash.RawString(),
							Right: name,
						}, storage.Pair{
							Left:  v2hash.TruncateV1().RawString(),
							Right: name,
						})
					log.Debug("approval torrent added", lf)
				case dirwatch.Removed:
					d.Storage.Delete(c.StorageCtx,
						event.InfoHash.AsString(),
						v2hash.RawString(),
						v2hash.TruncateV1().RawString(),
					)
					log.Debug("approval torrent deleted", lf)
				}
			} else {
				log.Err(err)
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
