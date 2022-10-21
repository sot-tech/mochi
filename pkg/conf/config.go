// Package conf contains alias for map encoded configuration
// and structure unmarshaller
package conf

import (
	"errors"

	"github.com/mitchellh/mapstructure"
	"github.com/rs/zerolog"
)

// TagName is a tag name, used for decoder customization
const TagName = "cfg"

// ErrNilConfigMap returned if unmarshalling map is nil and could not be
// decoded into structure
var ErrNilConfigMap = errors.New("unable to process nil map")

// MapConfig is just alias for map[string]any
type MapConfig map[string]any

// MarshalZerologObject writes map into zerolog event
func (m MapConfig) MarshalZerologObject(e *zerolog.Event) {
	e.Fields(map[string]any(m))
}

// Unmarshal decodes receiver map into provided structure.
// Decoder configured to automatically unmarshal inherited structures,
// convert string-ed duration (1s, 2m, 3h...) into time.Duration and
// string representation IP into net.IP.
// Tag used for decode customization is conf.TagName.
func (m MapConfig) Unmarshal(into any) (err error) {
	if m != nil {
		if len(m) > 0 {
			conf := &mapstructure.DecoderConfig{
				DecodeHook: mapstructure.ComposeDecodeHookFunc(
					mapstructure.StringToTimeDurationHookFunc(),
					mapstructure.StringToIPHookFunc(),
				),
				Squash:  true,
				Result:  into,
				TagName: TagName,
			}
			var decoder *mapstructure.Decoder
			if decoder, err = mapstructure.NewDecoder(conf); err == nil {
				err = decoder.Decode(m)
			}
		}
	} else {
		err = ErrNilConfigMap
	}
	return
}

type NamedMapConfig struct {
	Name   string
	Config MapConfig
}

func (nm NamedMapConfig) MarshalZerologObject(e *zerolog.Event) {
	e.Str("name", nm.Name).Dict("config", zerolog.Dict().EmbedObject(nm.Config))
}
