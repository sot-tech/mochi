package list

import (
	"encoding/hex"
	"fmt"
	"github.com/chihaya/chihaya/bittorrent"
	"github.com/chihaya/chihaya/middleware/torrentapproval/container"
	"gopkg.in/yaml.v2"
	"sync"
)

func init() {
	container.Register("list", build)
}

type Config struct {
	Whitelist []string `yaml:"whitelist"`
	Blacklist []string `yaml:"blacklist"`
}

var DUMMY struct{}

func build(confBytes []byte) (container.Container, error) {
	c := new(Config)
	if err := yaml.Unmarshal(confBytes, c); err != nil {
		return nil, fmt.Errorf("unable to deserialise configuration: %v", err)
	}
	if len(c.Whitelist) > 0 && len(c.Blacklist) > 0 {
		return nil, fmt.Errorf("using both whitelist and blacklist is invalid")
	}
	l := &List{
		Hashes: sync.Map{},
		Invert: len(c.Whitelist) == 0,
	}

	hashList := c.Whitelist
	if l.Invert {
		l.Invert = true
		hashList = c.Blacklist
	}

	for _, hashString := range hashList {
		hashinfo, err := hex.DecodeString(hashString)
		if err != nil {
			return nil, fmt.Errorf("whitelist : invalid hash %s", hashString)
		}
		if len(hashinfo) != 20 {
			return nil, fmt.Errorf("whitelist : hash %s is not 20 byes", hashString)
		}
		l.Hashes.Store(bittorrent.InfoHashFromBytes(hashinfo), DUMMY)
	}
	return l, nil
}

type List struct {
	Invert bool
	Hashes sync.Map
}

func (l *List) Contains(hash bittorrent.InfoHash) bool {
	_, result := l.Hashes.Load(hash)
	return result != l.Invert
}
