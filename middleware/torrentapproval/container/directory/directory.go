// Package directory implements container which
// checks if hash present in any of *.torrent file
// placed in some directory
package directory

import (
	"crypto/sha256"
	"fmt"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/util/dirwatch"
	"github.com/chihaya/chihaya/bittorrent"
	"github.com/chihaya/chihaya/middleware/torrentapproval/container"
	"github.com/chihaya/chihaya/middleware/torrentapproval/container/list"
	"github.com/chihaya/chihaya/pkg/log"
	"github.com/chihaya/chihaya/pkg/stop"
	"github.com/chihaya/chihaya/storage"
	"gopkg.in/yaml.v2"
)

const Name = "directory"

func init() {
	container.Register(Name, build)
}

type Config struct {
	list.Config
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
		log.Info("Storage context not set, using default value: " + container.DefaultStorageCtxName)
		d.StorageCtx = container.DefaultStorageCtxName
	}
	go func() {
		for event := range d.watcher.Events {
			switch event.Change {
			case dirwatch.Added:
				data := make([]storage.Pair, 1, 2)
				data[0] = storage.Pair{Left: event.InfoHash[:], Right: list.DUMMY}
				if v2ih, err := v2InfoHash(event.TorrentFilePath); err == nil {
					data = append(data, storage.Pair{Left: v2ih, Right: list.DUMMY})
				} else {
					log.Err(err)
				}
				d.Storage.BulkPut(c.StorageCtx, data...)
			case dirwatch.Removed:
				data := make([]interface{}, 1, 2)
				data[0] = event.InfoHash[:]
				if v2ih, err := v2InfoHash(event.TorrentFilePath); err == nil {
					data = append(data, v2ih)
				} else {
					log.Err(err)
				}
				d.Storage.Delete(c.StorageCtx, data...)
			}
		}
	}()
	return d, err
}

func v2InfoHash(path string) (ih bittorrent.InfoHash, err error) {
	var mi *metainfo.MetaInfo
	if mi, err = metainfo.LoadFromFile(path); err == nil {
		hash := sha256.New()
		hash.Write(mi.InfoBytes)
		ih, err = bittorrent.NewInfoHash(hash.Sum(nil))
	}
	return
}

type directory struct {
	list.List
	watcher *dirwatch.Instance
}

func (d *directory) Stop() stop.Result {
	d.watcher.Close()
	return stop.AlreadyStopped
}
