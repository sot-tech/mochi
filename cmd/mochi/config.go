package main

import (
	"errors"
	"os"
	"time"

	"gopkg.in/yaml.v3"

	fh "github.com/sot-tech/mochi/frontend/http"
	fu "github.com/sot-tech/mochi/frontend/udp"
	"github.com/sot-tech/mochi/pkg/conf"

	// Seed math random
	_ "github.com/sot-tech/mochi/pkg/randseed"

	// Imports to register middleware hooks.
	_ "github.com/sot-tech/mochi/middleware/clientapproval"
	_ "github.com/sot-tech/mochi/middleware/jwt"
	_ "github.com/sot-tech/mochi/middleware/torrentapproval"
	_ "github.com/sot-tech/mochi/middleware/varinterval"

	// Imports to register storage drivers.
	_ "github.com/sot-tech/mochi/storage/keydb"
	sm "github.com/sot-tech/mochi/storage/memory"
	_ "github.com/sot-tech/mochi/storage/pg"
	_ "github.com/sot-tech/mochi/storage/redis"
)

// Config represents the configuration used for Server start.
type Config struct {
	// TODO(jzelinskie): Evaluate whether we would like to make
	//  AnnounceInterval and MinAnnounceInterval optional.
	// We can make Conf extensible enough that you can program a new response
	// generator at the cost of making it possible for users to create config that
	// won't compose a functional tracker.
	AnnounceInterval    time.Duration         `yaml:"announce_interval"`
	MinAnnounceInterval time.Duration         `yaml:"min_announce_interval"`
	MetricsAddr         string                `yaml:"metrics_addr"`
	Frontends           []conf.NamedMapConfig `yaml:"frontends"`
	Storage             conf.NamedMapConfig   `yaml:"storage"`
	PreHooks            []conf.NamedMapConfig `yaml:"prehooks"`
	PostHooks           []conf.NamedMapConfig `yaml:"posthooks"`
}

// QuickConfig is the simple configuration for quick start without config file.
// Includes in-memory store, http and udp frontends without any middleware.
var QuickConfig = &Config{
	Frontends: []conf.NamedMapConfig{
		{
			Name:   fh.Name,
			Config: conf.MapConfig{},
		},
		{
			Name:   fu.Name,
			Config: conf.MapConfig{},
		},
	},
	Storage: conf.NamedMapConfig{
		Name:   sm.Name,
		Config: conf.MapConfig{},
	},
	PreHooks:  []conf.NamedMapConfig{},
	PostHooks: []conf.NamedMapConfig{},
}

// ParseConfigFile returns a new Config given the path to a YAML
// configuration file.
//
// It supports relative and absolute paths and environment variables.
func ParseConfigFile(path string) (*Config, error) {
	if path == "" {
		return nil, errors.New("no config path specified")
	}

	f, err := os.Open(os.ExpandEnv(path))
	if err == nil {
		defer f.Close()
		cfgFile := new(Config)
		err = yaml.NewDecoder(f).Decode(cfgFile)
		return cfgFile, err
	}
	return nil, err
}
