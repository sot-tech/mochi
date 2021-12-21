package main

import (
	"errors"
	"os"

	"gopkg.in/yaml.v2"

	"github.com/sot-tech/mochi/frontend/http"
	"github.com/sot-tech/mochi/frontend/udp"
	"github.com/sot-tech/mochi/middleware"

	// Imports to register middleware drivers.
	_ "github.com/sot-tech/mochi/middleware/clientapproval"
	_ "github.com/sot-tech/mochi/middleware/jwt"
	_ "github.com/sot-tech/mochi/middleware/torrentapproval"
	_ "github.com/sot-tech/mochi/middleware/varinterval"

	// Imports to register storage drivers.
	_ "github.com/sot-tech/mochi/storage/memory"
	_ "github.com/sot-tech/mochi/storage/redis"
)

type storageConfig struct {
	Name   string      `yaml:"name"`
	Config interface{} `yaml:"config"`
}

// Config represents the configuration used for executing Conf.
type Config struct {
	middleware.ResponseConfig `yaml:",inline"`
	MetricsAddr               string              `yaml:"metrics_addr"`
	HTTPConfig                http.Config         `yaml:"http"`
	UDPConfig                 udp.Config          `yaml:"udp"`
	Storage                   storageConfig       `yaml:"storage"`
	PreHooks                  []middleware.Config `yaml:"prehooks"`
	PostHooks                 []middleware.Config `yaml:"posthooks"`
}

// PreHookNames returns only the names of the configured middleware.
func (cfg Config) PreHookNames() (names []string) {
	for _, hook := range cfg.PreHooks {
		names = append(names, hook.Name)
	}

	return
}

// PostHookNames returns only the names of the configured middleware.
func (cfg Config) PostHookNames() (names []string) {
	for _, hook := range cfg.PostHooks {
		names = append(names, hook.Name)
	}

	return
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
