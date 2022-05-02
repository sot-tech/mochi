// Package jwt implements a Hook that fails an Announce if the client's request
// is missing a valid JSON Web Token.
//
// JWTs are validated against the standard claims in RFC7519 along with an
// extra "infohash" claim that verifies the client has access to the Swarm.
// RS256 keys are asychronously rotated from a provided JWK Set HTTP endpoint.
package jwt

import (
	"context"
	"crypto"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	jc "github.com/SermoDigital/jose/crypto"
	"github.com/SermoDigital/jose/jws"
	"github.com/SermoDigital/jose/jwt"
	"github.com/mendsley/gojwk"

	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/middleware"
	"github.com/sot-tech/mochi/pkg/conf"
	"github.com/sot-tech/mochi/pkg/log"
	"github.com/sot-tech/mochi/pkg/stop"
	"github.com/sot-tech/mochi/storage"
)

// Name is the name by which this middleware is registered with Conf.
const Name = "jwt"

func init() {
	middleware.RegisterBuilder(Name, build)
}

var (
	logger = log.NewLogger(Name)
	// ErrMissingJWT is returned when a JWT is missing from a request.
	ErrMissingJWT = bittorrent.ClientError("unapproved request: missing jwt")

	// ErrInvalidJWT is returned when a JWT fails to verify.
	ErrInvalidJWT = bittorrent.ClientError("unapproved request: invalid jwt")

	errInvalidInfoHashClaim = errors.New("claim \"infohash\" is invalid")

	errInvalidKid = errors.New("invalid kid")

	errUnknownKidSigner = errors.New("signed by unknown kid")
)

// Config represents all the values required by this middleware to fetch JWKs
// and verify JWTs.
type Config struct {
	Issuer            string
	Audience          string
	JWKSetURL         string        `cfg:"jwk_set_url"`
	JWKUpdateInterval time.Duration `cfg:"jwk_set_update_interval"`
}

type hook struct {
	cfg        Config
	publicKeys map[string]crypto.PublicKey
	closing    chan struct{}
}

func build(options conf.MapConfig, _ storage.PeerStorage) (middleware.Hook, error) {
	var cfg Config

	if err := options.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("middleware %s: %w", Name, err)
	}

	logger.Debug().Object("options", options).Msg("creating new JWT middleware")
	h := &hook{
		cfg:        cfg,
		publicKeys: map[string]crypto.PublicKey{},
		closing:    make(chan struct{}),
	}

	logger.Debug().Msg("performing initial fetch of JWKs")
	if err := h.updateKeys(); err != nil {
		return nil, fmt.Errorf("failed to fetch initial JWK Set: %w", err)
	}

	go func() {
		for {
			select {
			case <-h.closing:
				return
			case <-time.After(cfg.JWKUpdateInterval):
				logger.Debug().Msg("performing fetch of JWKs")
				_ = h.updateKeys()
			}
		}
	}()

	return h, nil
}

func (h *hook) updateKeys() error {
	resp, err := http.Get(h.cfg.JWKSetURL)
	if err != nil {
		logger.Error().Err(err).Msg("failed to fetch JWK Set")
		return err
	}
	defer resp.Body.Close()
	var parsedJWKs gojwk.Key
	err = json.NewDecoder(resp.Body).Decode(&parsedJWKs)
	if err != nil {
		logger.Error().Err(err).Msg("failed to decode JWK JSON")
		return err
	}

	keys := map[string]crypto.PublicKey{}
	for _, parsedJWK := range parsedJWKs.Keys {
		publicKey, err := parsedJWK.DecodePublicKey()
		if err != nil {
			logger.Error().Err(err).Msg("failed to decode JWK into public key")
			return err
		}
		keys[parsedJWK.Kid] = publicKey
	}
	h.publicKeys = keys

	logger.Debug().Msg("successfully fetched JWK Set")
	return nil
}

func (h *hook) Stop() stop.Result {
	logger.Debug().Msg("attempting to shutdown JWT middleware")
	select {
	case <-h.closing:
		return stop.AlreadyStopped
	default:
	}
	c := make(stop.Channel)
	go func() {
		close(h.closing)
		c.Done()
	}()
	return c.Result()
}

func (h *hook) HandleAnnounce(ctx context.Context, req *bittorrent.AnnounceRequest, _ *bittorrent.AnnounceResponse) (context.Context, error) {
	if req.Params == nil {
		return ctx, ErrMissingJWT
	}

	jwtParam, ok := req.Params.String("jwt")
	if !ok {
		return ctx, ErrMissingJWT
	}

	if err := validateJWT(req.InfoHash, []byte(jwtParam), h.cfg.Issuer, h.cfg.Audience, h.publicKeys); err != nil {
		return ctx, ErrInvalidJWT
	}

	return ctx, nil
}

func (h *hook) HandleScrape(ctx context.Context, _ *bittorrent.ScrapeRequest, _ *bittorrent.ScrapeResponse) (context.Context, error) {
	// Scrapes don't require any protection.
	return ctx, nil
}

func validateJWT(ih bittorrent.InfoHash, jwtBytes []byte, cfgIss, cfgAud string, publicKeys map[string]crypto.PublicKey) error {
	parsedJWT, err := jws.ParseJWT(jwtBytes)
	if err != nil {
		return err
	}

	claims := parsedJWT.Claims()
	if iss, ok := claims.Issuer(); !ok || iss != cfgIss {
		logger.Debug().
			Bool("exists", ok).
			Str("claim", iss).
			Str("config", cfgIss).
			Msg("unequal or missing issuer when validating JWT")
		return jwt.ErrInvalidISSClaim
	}

	if auds, ok := claims.Audience(); !ok || !in(cfgAud, auds) {
		logger.Debug().
			Bool("exists", ok).
			Strs("claim", auds).
			Str("config", cfgAud).
			Msg("unequal or missing audience when validating JWT")
		return jwt.ErrInvalidAUDClaim
	}

	ihHex := hex.EncodeToString([]byte(ih))
	if ihClaim, ok := claims.Get("infohash").(string); !ok || ihClaim != ihHex {
		logger.Debug().
			Bool("exists", ok).
			Str("claim", ihClaim).
			Str("request", ihHex).
			Msg("unequal or missing infohash when validating JWT")
		return errInvalidInfoHashClaim
	}

	parsedJWS := parsedJWT.(jws.JWS)
	kid, ok := parsedJWS.Protected().Get("kid").(string)
	if !ok {
		logger.Debug().
			Bool("exists", ok).
			Str("claim", kid).
			Msg("missing kid when validating JWT")
		return errInvalidKid
	}
	publicKey, ok := publicKeys[kid]
	if !ok {
		logger.Debug().Str("claim", kid).Msg("missing public key forkid when validating JWT")
		return errUnknownKidSigner
	}

	err = parsedJWS.Verify(publicKey, jc.SigningMethodRS256)
	if err != nil {
		logger.Debug().Err(err).Msg("failed to verify signature of JWT")
		return err
	}

	return nil
}

func in(x string, xs []string) bool {
	for _, y := range xs {
		if x == y {
			return true
		}
	}
	return false
}
