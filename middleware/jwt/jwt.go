// Package jwt implements a Hook that fails an Announce if the client's request
// is missing a valid JSON Web Token.
//
// JWTs are validated against the standard claims in RFC7519 along with an
// extra "infohash" claim that verifies the client has access to the Swarm.
// RS256 keys are asynchronously rotated from a provided JWK Set HTTP endpoint.
package jwt

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
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

	errInvalidInfoHashClaim = errors.New("token has invalid \"infohash\" claim")

	errJWKsNotSet = errors.New("required parameters not provided: Issuer, Audience and/or JWKSetURL")

	hmacAlgorithms = jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Name, jwt.SigningMethodHS384.Name, jwt.SigningMethodHS512.Name})
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
	cfg  Config
	jwks *keyfunc.JWKS
}

type claims struct {
	jwt.RegisteredClaims
	InfoHash string `json:"infohash,omitempty"`
}

func build(options conf.MapConfig, _ storage.PeerStorage) (h middleware.Hook, err error) {
	var cfg Config

	if err = options.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("middleware %s: %w", Name, err)
	}

	logger.Debug().Object("options", options).Msg("creating new JWT middleware")

	if len(cfg.JWKSetURL) > 0 {
		var jwks *keyfunc.JWKS
		jwks, err = keyfunc.Get(cfg.JWKSetURL, keyfunc.Options{
			Ctx: context.Background(),
			RefreshErrorHandler: func(err error) {
				logger.Error().Err(err).Msg("error occurred while updating JWKs")
			},
			RefreshInterval:   cfg.JWKUpdateInterval,
			RefreshUnknownKID: true,
		})
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

func (h *hook) HandleAnnounce(ctx context.Context, req *bittorrent.AnnounceRequest, _ *bittorrent.AnnounceResponse) (context.Context, error) {
	if req.Params == nil {
		return ctx, ErrMissingJWT
	}

	jwtParam, ok := req.Params.String("jwt")
	if !ok {
		return ctx, ErrMissingJWT
	}

	if errs := h.validateJWT(req.InfoHash, jwtParam); len(errs) > 0 {
		logger.Info().
			Errs("errors", errs).
			Object("source", req.RequestPeer).
			Msg("JWT validation failed")
		return ctx, ErrInvalidJWT
	}

	return ctx, nil
}

func (h *hook) HandleScrape(ctx context.Context, _ *bittorrent.ScrapeRequest, _ *bittorrent.ScrapeResponse) (context.Context, error) {
	// Scrapes don't require any protection.
	return ctx, nil
}

func (h *hook) validateJWT(ih bittorrent.InfoHash, rawJwt string) []error {
	// KeyFunc will check KID, Parse will check ALG and signature
	errs := make([]error, 0, 4)
	token, err := jwt.ParseWithClaims(rawJwt, claims{}, h.jwks.Keyfunc, hmacAlgorithms)
	if err != nil {
		return []error{err}
	}

	if err = token.Claims.Valid(); err != nil {
		errs = append(errs, err)
	}

	claims := token.Claims.(claims)

	if !claims.VerifyIssuer(h.cfg.Issuer, true) {
		logger.Debug().
			Str("provided", claims.Issuer).
			Str("required", h.cfg.Issuer).
			Msg("unequal or missing issuer when validating JWT")
		errs = append(errs, jwt.ErrTokenInvalidIssuer)
	}

	if !claims.VerifyAudience(h.cfg.Audience, true) {
		logger.Debug().
			Strs("provided", claims.Audience).
			Str("required", h.cfg.Audience).
			Msg("unequal or missing audience when validating JWT")
		errs = append(errs, jwt.ErrTokenInvalidAudience)
	}

	providedIh, err := bittorrent.NewInfoHash(claims.InfoHash)
	if err != nil {
		errs = append(errs, err)
	}
	if subtle.ConstantTimeCompare([]byte(providedIh), []byte(ih)) != 0 {
		logger.Error().
			Err(err).
			Stringer("provided", providedIh).
			Stringer("required", ih).
			Msg("invalid or unequal info hash when validating JWT")
		errs = append(errs, errInvalidInfoHashClaim)
	}

	return errs
}
