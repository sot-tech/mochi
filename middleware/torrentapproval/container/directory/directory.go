package directory

import (
	"fmt"
	"github.com/anacrolix/torrent/util/dirwatch"
	"github.com/chihaya/chihaya/middleware/torrentapproval/container"
	"github.com/chihaya/chihaya/middleware/torrentapproval/container/list"
	"github.com/chihaya/chihaya/pkg/stop"
	"sync"
)

func init() {
	container.Register("list", func() container.Configuration {
		return Config{}
	})
}

type Config struct {
	WhitelistPath string `yaml:"whitelist_path"`
	BlacklistPath string `yaml:"blacklist_path"`
}

func (b Config) Build() (container.Container, error) {
	if len(b.WhitelistPath) > 0 && len(b.BlacklistPath) > 0 {
		return nil, fmt.Errorf("using both whitelist and blacklist is invalid")
	}
	var err error
	lst := &directory{
		List: list.List{
			Hashes: sync.Map{},
			Invert: len(b.WhitelistPath) == 0,
		},
		watcher: nil,
	}
	dir := b.WhitelistPath
	if lst.Invert {
		dir = b.BlacklistPath
	}
	var w *dirwatch.Instance
	w, err = dirwatch.New(dir)
	if w, err = dirwatch.New(dir); err != nil {
		return nil, fmt.Errorf("unable to initialize directory watch")
	}
	lst.watcher = w
	go func() {
		for event := range lst.watcher.Events {
			switch event.Change {
			case dirwatch.Added:
				lst.Hashes.Store(event.InfoHash, list.DUMMY)
			case dirwatch.Removed:
				lst.Hashes.Delete(event.InfoHash)
			}
		}
	}()
	return lst, err
}

type directory struct {
	list.List
	watcher *dirwatch.Instance
}

func (d *directory) Stop() stop.Result {
	d.watcher.Close()
	return stop.AlreadyStopped
}
