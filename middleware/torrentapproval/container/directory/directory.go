package directory

import (
	"fmt"
	"github.com/chihaya/chihaya/bittorrent"
	"github.com/chihaya/chihaya/middleware/torrentapproval/container"
	"github.com/chihaya/chihaya/middleware/torrentapproval/container/list"
	"github.com/chihaya/chihaya/pkg/log"
	"github.com/chihaya/chihaya/pkg/stop"
	"github.com/fsnotify/fsnotify"
	"os"
	"path/filepath"
	"sync"
)

func init() {
	container.Register("list", builder{})
}

type builder struct {
	WhitelistPath string `yaml:"whitelist_path"`
	BlacklistPath string `yaml:"blacklist_path"`
}

func (b builder) New() (container.Container, error) {
	if len(b.WhitelistPath) > 0 && len(b.BlacklistPath) > 0 {
		return nil, fmt.Errorf("using both whitelist and blacklist is invalid")
	}
	var err error
	dirLister := &directory{
		List: list.List{
			Hashes: sync.Map{},
			Invert: len(b.WhitelistPath) == 0,
		},
		files:   sync.Map{},
		root:    b.WhitelistPath,
		watcher: nil,
	}
	if dirLister.Invert {
		dirLister.root = b.BlacklistPath
	}
	var w *fsnotify.Watcher
	if w, err = fsnotify.NewWatcher(); err != nil {
		return nil, fmt.Errorf("unable to initialize fsnotify mechanism")
	}
	if dirContent, err := os.ReadDir(dirLister.root); err != nil {
		return nil, err
	} else {
		for _, f := range dirContent {
			if !f.IsDir() {
				if err = dirLister.processFile(f.Name(), false); err != nil {
					log.Warn(err)
				}
			}
		}
	}
	if err = w.Add(dirLister.root); err != nil {
		_ = w.Close()
		dirLister = nil
	}
	return dirLister, err
}

func (d *directory) watch() {
	go func() {
		for err := range d.watcher.Errors {
			log.Error(err)
		}
	}()
	go func() {
		for event := range d.watcher.Events {
			log.Debug(event.String())
			//todo: implement event type parsing
		}
	}()
}

func (d *directory) processFile(name string, delete bool) error {
	fullName := filepath.Join(d.root, name)
	if delete {
		if hash, found := d.files.Load(fullName); found{
			d.Hashes.Delete(hash)
		}
	} else {
		var hashBytes []byte
		info := bittorrent.InfoHashFromBytes(hashBytes)
		d.files.Store(fullName, info)
		d.Hashes.Store(info, list.DUMMY)
	}
	return nil
}

type directory struct {
	list.List
	files   sync.Map
	root    string
	watcher *fsnotify.Watcher
}

func (d *directory) Stop() stop.Result {
	ch := make(stop.Channel)
	go ch.Done(d.watcher.Close())
	return ch.Result()
}
