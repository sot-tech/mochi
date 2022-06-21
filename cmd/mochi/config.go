package main

import (
	"errors"
	"os"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/sot-tech/mochi/pkg/conf"

	// Imports to register middleware drivers.
	_ "github.com/sot-tech/mochi/middleware/clientapproval"
	_ "github.com/sot-tech/mochi/middleware/jwt"
	_ "github.com/sot-tech/mochi/middleware/torrentapproval"
	_ "github.com/sot-tech/mochi/middleware/varinterval"

	// Imports to register storage drivers.
	_ "github.com/sot-tech/mochi/storage/keydb"
	_ "github.com/sot-tech/mochi/storage/memory"
	_ "github.com/sot-tech/mochi/storage/pg"
	_ "github.com/sot-tech/mochi/storage/redis"
)

// Config represents the configuration used for executing Conf.
type Config struct {
	// TODO(jzelinskie): Evaluate whether we would like to make
	//  AnnounceInterval and MinAnnounceInterval optional.
	// We can make Conf extensible enough that you can program a new response
	// generator at the cost of making it possible for users to create config that
	// won't compose a functional tracker.
	AnnounceInterval    time.Duration  `yaml:"announce_interval"`
	MinAnnounceInterval time.Duration  `yaml:"min_announce_interval"`
	MetricsAddr         string         `yaml:"metrics_addr"`
	HTTPConfig          conf.MapConfig `yaml:"http"`
	UDPConfig           conf.MapConfig `yaml:"udp"`
	Storage             struct {
		Name   string         `yaml:"name"`
		Config conf.MapConfig `yaml:"config"`
	} `yaml:"storage"`
	PreHooks  []conf.MapConfig `yaml:"prehooks"`
	PostHooks []conf.MapConfig `yaml:"posthooks"`
}

// ConfigFile represents a namespaced YAML configation file.
type ConfigFile struct {
	Conf Config `yaml:"mochi"`
}

// ParseConfigFile returns a new ConfigFile given the path to a YAML
// configuration file.
//
// It supports relative and absolute paths and environment variables.
func ParseConfigFile(path string) (*ConfigFile, error) {
	if path == "" {
		return nil, errors.New("no config path specified")
	}

	f, err := os.Open(os.ExpandEnv(path))
	if err == nil {
		defer f.Close()
		cfgFile := new(ConfigFile)
		err = yaml.NewDecoder(f).Decode(cfgFile)
		return cfgFile, err
	}
	return nil, err
}
