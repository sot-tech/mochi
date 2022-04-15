// Package conf contains alias for map encoded configuration
// and structure unmarshaller
package conf

import (
	"errors"

	"github.com/mitchellh/mapstructure"

	"github.com/sot-tech/mochi/pkg/log"
)

// TagName is a tag name, used for decoder customization
const TagName = "cfg"

// ErrNilConfigMap returned if unmarshalling map is nil and could not be
// decoded into structure
var ErrNilConfigMap = errors.New("unable to process nil map")

// MapConfig is just alias for map[string]any
type MapConfig map[string]any

// LogFields just returns this map as a set of Logrus fields.
func (m MapConfig) LogFields() log.Fields {
	return log.Fields(m)
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
