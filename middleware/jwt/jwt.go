// Package jwt implements a Hook that fails on Announce or Scrape if the client's request
// is missing a valid JSON Web Token.
//
// JWTs are validated against the standard claims in RFC7519 along with an
// extra "infohash(es)" claim that verifies the client has access to the Swarm.
package jwt

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/MicahParks/keyfunc"
	"github.com/golang-jwt/jwt/v4"

	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/middleware"
	"github.com/sot-tech/mochi/pkg/conf"
	"github.com/sot-tech/mochi/pkg/log"
	"github.com/sot-tech/mochi/pkg/stop"
	"github.com/sot-tech/mochi/storage"
)

// Name is the name by which this middleware is registered with Conf.
const (
	Name                = "jwt"
	authorizationHeader = "authorization"
	bearerAuthPrefix    = "bearer "
)

func init() {
	middleware.RegisterBuilder(Name, build)
}

var (
	logger = log.NewLogger(Name)
	// ErrMissingJWT is returned when a JWT is missing from a request.
	ErrMissingJWT = bittorrent.ClientError("unapproved request: missing jwt")

	// ErrInvalidJWT is returned when a JWT fails to verify.
	ErrInvalidJWT = bittorrent.ClientError("unapproved request: invalid jwt")

	errJWKsNotSet = errors.New("required parameters not provided: Issuer, Audience and/or JWKSetURL")

	hmacAlgorithms = jwt.WithValidMethods([]string{
		jwt.SigningMethodHS256.Alg(), jwt.SigningMethodHS384.Alg(), jwt.SigningMethodHS512.Alg(),
		jwt.SigningMethodRS256.Alg(), jwt.SigningMethodRS384.Alg(), jwt.SigningMethodRS512.Alg(),
		jwt.SigningMethodPS256.Alg(), jwt.SigningMethodPS384.Alg(), jwt.SigningMethodPS512.Alg(),
		jwt.SigningMethodES256.Alg(), jwt.SigningMethodES384.Alg(), jwt.SigningMethodES512.Alg(),
		jwt.SigningMethodEdDSA.Alg(),
	})
)

// Config represents all the values required by this middleware to fetch JWKs
// and verify JWTs.
type Config struct {
	Header            string
	Issuer            string
	Audience          string
	JWKSetURL         string        `cfg:"jwk_set_url"`
	JWKUpdateInterval time.Duration `cfg:"jwk_set_update_interval"`
	HandleAnnounce    bool          `cfg:"handle_announce"`
	HandleScrape      bool          `cfg:"handle_scrape"`
}

type hook struct {
	cfg  Config
	jwks *keyfunc.JWKS
}

func build(options conf.MapConfig, _ storage.PeerStorage) (h middleware.Hook, err error) {
	var cfg Config

	logger.Debug().Object("options", options).Msg("creating new JWT middleware")

	if err = options.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("middleware %s: %w", Name, err)
	}

	if len(cfg.JWKSetURL) > 0 && len(cfg.Issuer) > 0 && len(cfg.Audience) > 0 {
		if len(cfg.Header) == 0 {
			cfg.Header = authorizationHeader
			logger.Warn().
				Str("name", "Header").
				Str("default", cfg.Header).
				Msg("falling back to default configuration")
		}

		var jwks *keyfunc.JWKS
		if cfg.HandleAnnounce || cfg.HandleScrape {
			jwks, err = keyfunc.Get(cfg.JWKSetURL, keyfunc.Options{
				Ctx: context.Background(),
				RefreshErrorHandler: func(err error) {
					logger.Error().Err(err).Msg("error occurred while updating JWKs")
				},
				RefreshInterval:   cfg.JWKUpdateInterval,
				RefreshUnknownKID: true,
			})
		} else {
			logger.Warn().Msg("both announce and scrape handle disabled")
		}
		if err == nil {
			h = &hook{
				cfg:  cfg,
				jwks: jwks,
			}
		}
	} else {
		err = errJWKsNotSet
	}

	return
}

func (h *hook) Stop() stop.Result {
	logger.Debug().Msg("attempting to shutdown JWT middleware")
	c := make(stop.Channel)
	if h.jwks != nil {
		go h.jwks.EndBackground()
	}
	return c.Result()
}

type compatibleClaims interface {
	Valid() error
	ToRegisteredClaims() jwt.RegisteredClaims
}

type announceClaims struct {
	jwt.RegisteredClaims
	InfoHash string `json:"infohash,omitempty"`
}

func (ac announceClaims) ToRegisteredClaims() jwt.RegisteredClaims {
	return ac.RegisteredClaims
}

func (h *hook) HandleAnnounce(ctx context.Context, req *bittorrent.AnnounceRequest, _ *bittorrent.AnnounceResponse) (context.Context, error) {
	if !h.cfg.HandleAnnounce {
		return ctx, nil
	}
	var err error

	if jwtParam := h.getJWT(req.Params); len(jwtParam) == 0 {
		err = ErrMissingJWT
	} else {
		claims := new(announceClaims)
		if errs := h.validateBaseJWT(jwtParam, claims); len(errs) > 0 {
			logger.Info().
				Errs("errors", errs).
				Object("source", req.RequestPeer).
				Msg("JWT validation failed")
			err = ErrInvalidJWT
		} else {
			var claimIH bittorrent.InfoHash
			if claimIH, err = bittorrent.NewInfoHash(claims.InfoHash); err != nil {
				logger.Info().
					Err(err).
					Object("source", req.RequestPeer).
					Msg("InfoHash claim parse failed")
				err = ErrInvalidJWT
			}
			if req.InfoHash != claimIH {
				logger.Info().
					Stringer("provided", claimIH).
					Stringer("required", req.InfoHash).
					Object("source", req.RequestPeer).
					Msg("InfoHash claim not equals to request InfoHash")
				err = ErrInvalidJWT
			}
		}
	}

	return ctx, err
}

type scrapeClaims struct {
	jwt.RegisteredClaims
	InfoHashes []string `json:"infohashes,omitempty"`
}

func (sc scrapeClaims) ToRegisteredClaims() jwt.RegisteredClaims {
	return sc.RegisteredClaims
}

func (h *hook) HandleScrape(ctx context.Context, req *bittorrent.ScrapeRequest, _ *bittorrent.ScrapeResponse) (context.Context, error) {
	if !h.cfg.HandleScrape {
		return ctx, nil
	}

	var err error

	if jwtParam := h.getJWT(req.Params); len(jwtParam) == 0 {
		err = ErrMissingJWT
	} else {
		claims := new(scrapeClaims)
		if errs := h.validateBaseJWT(jwtParam, claims); len(errs) > 0 {
			logger.Info().
				Errs("errors", errs).
				Array("source", req.RequestAddresses).
				Msg("JWT validation failed")
			err = ErrInvalidJWT
		} else {
			var claimIHs bittorrent.InfoHashes
			for _, s := range claims.InfoHashes {
				if providedIh, err := bittorrent.NewInfoHash(s); err == nil {
					claimIHs = append(claimIHs, providedIh)
				} else {
					logger.Info().
						Err(err).
						Array("source", req.RequestAddresses).
						Msg("InfoHash claim parse failed")
				}
			}
			eq := len(req.InfoHashes) == len(claimIHs)
			if eq {
				for _, rIH := range req.InfoHashes {
					found := false
					for _, cIH := range claimIHs {
						if rIH == cIH {
							found = true
							break
						}
					}
					if !found {
						eq = false
						break
					}
				}
			}
			if !eq {
				logger.Info().
					Array("provided", claimIHs).
					Array("required", req.InfoHashes).
					Array("source", req.RequestAddresses).
					Msg("InfoHashes claim not equals to request InfoHashes")
				err = ErrInvalidJWT
			}
		}
	}

	return ctx, err
}

func (h *hook) getJWT(params bittorrent.Params) (jwt string) {
	if params != nil {
		var found bool
		if jwt, found = params.String(h.cfg.Header); found {
			if strings.HasPrefix(strings.ToLower(jwt), bearerAuthPrefix) {
				jwt = jwt[len(bearerAuthPrefix):]
			}
		}
	}
	return
}

func (h *hook) validateBaseJWT(jwtParam string, claims compatibleClaims) (errs []error) {
	if strings.HasPrefix(strings.ToLower(jwtParam), bearerAuthPrefix) {
		jwtParam = jwtParam[len(bearerAuthPrefix):]
	}
	if _, err := jwt.ParseWithClaims(jwtParam, claims, h.jwks.Keyfunc, hmacAlgorithms); err != nil {
		errs = append(errs, err)
	}
	if err := claims.Valid(); err != nil {
		errs = append(errs, err)
	}
	if errs0 := h.validateRegisteredClaims(claims); len(errs0) > 0 {
		errs = append(errs, errs0...)
	}
	return
}

func (h *hook) validateRegisteredClaims(cl compatibleClaims) (errs []error) {
	rc := cl.ToRegisteredClaims()
	if !rc.VerifyIssuer(h.cfg.Issuer, true) {
		logger.Debug().
			Str("provided", rc.Issuer).
			Str("required", h.cfg.Issuer).
			Msg("unequal or missing issuer when validating JWT")
		errs = append(errs, jwt.ErrTokenInvalidIssuer)
	}

	if !rc.VerifyAudience(h.cfg.Audience, true) {
		logger.Debug().
			Strs("provided", rc.Audience).
			Str("required", h.cfg.Audience).
			Msg("unequal or missing audience when validating JWT")
		errs = append(errs, jwt.ErrTokenInvalidAudience)
	}
	return
}
