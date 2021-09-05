package directory

import (
	"fmt"
	"github.com/anacrolix/torrent/util/dirwatch"
	"github.com/chihaya/chihaya/middleware/torrentapproval/container"
	"github.com/chihaya/chihaya/middleware/torrentapproval/container/list"
	"github.com/chihaya/chihaya/pkg/stop"
	"gopkg.in/yaml.v2"
	"sync"
)

func init() {
	container.Register("directory", builder{})
}

type builder struct {}

type Config struct {
	WhitelistPath string `yaml:"whitelist_path"`
	BlacklistPath string `yaml:"blacklist_path"`
}

func (b builder) Build(confBytes []byte) (container.Container, error) {
	c := new(Config)
	if err := yaml.Unmarshal(confBytes, c); err != nil {
		return nil, fmt.Errorf("unable to deserialise configuration: %v", err)
	}
	if len(c.WhitelistPath) > 0 && len(c.BlacklistPath) > 0 {
		return nil, fmt.Errorf("using both whitelist and blacklist is invalid")
	}
	var err error
	lst := &directory{
		List: list.List{
			Hashes: sync.Map{},
			Invert: len(c.WhitelistPath) == 0,
		},
		watcher: nil,
	}
	dir := c.WhitelistPath
	if lst.Invert {
		dir = c.BlacklistPath
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
