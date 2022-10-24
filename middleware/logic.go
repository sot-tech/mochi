package middleware

import (
	"context"
	"sort"
	"time"

	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/pkg/stop"
	"github.com/sot-tech/mochi/storage"
)

// Logic used by a frontend in order to: (1) generate a
// response from a parsed request, and (2) asynchronously observe anything
// after the response has been delivered to the client.
type Logic struct {
	announceInterval    time.Duration
	minAnnounceInterval time.Duration
	preHooks            []Hook
	postHooks           []Hook
	pingers             []Pinger
}

// NewLogic creates a new instance of a Logic that executes the provided
// middleware hooks.
func NewLogic(annInterval, minAnnInterval time.Duration, peerStore storage.PeerStorage, preHooks, postHooks []Hook) *Logic {
	l := &Logic{
		announceInterval:    annInterval,
		minAnnounceInterval: minAnnInterval,
		preHooks:            append(preHooks, &responseHook{store: peerStore}),
		postHooks:           append(postHooks, &swarmInteractionHook{store: peerStore}),
		pingers:             make([]Pinger, 0, 1),
	}
	for _, h := range l.preHooks {
		if ph, isOk := h.(Pinger); isOk {
			l.pingers = append(l.pingers, ph)
		}
	}
	return l
}

// HandleAnnounce generates a response for an Announce.
//
// Returns the updated context, the generated AnnounceResponse and no error
// on success; nil and error on failure.
func (l *Logic) HandleAnnounce(ctx context.Context, req *bittorrent.AnnounceRequest) (_ context.Context, resp *bittorrent.AnnounceResponse, err error) {
	logger.Debug().Object("request", req).Msg("new announce request")
	resp = &bittorrent.AnnounceResponse{
		Interval:    l.announceInterval,
		MinInterval: l.minAnnounceInterval,
		Compact:     req.Compact,
	}
	for _, h := range l.preHooks {
		if ctx, err = h.HandleAnnounce(ctx, req, resp); err != nil {
			return nil, nil, err
		}
	}

	logger.Debug().Object("response", resp).Msg("generated announce response")
	return ctx, resp, nil
}

// AfterAnnounce does something with the results of an Announce after it
// has been completed.
func (l *Logic) AfterAnnounce(ctx context.Context, req *bittorrent.AnnounceRequest, resp *bittorrent.AnnounceResponse) {
	var err error
	for _, h := range l.postHooks {
		if ctx, err = h.HandleAnnounce(ctx, req, resp); err != nil {
			logger.Error().Err(err).
				Object("request", req).
				Object("response", resp).
				Msg("post-announce hooks failed")
			return
		}
	}
}

// HandleScrape generates a response for a Scrape.
//
// Returns the updated context, the generated AnnounceResponse and no error
// on success; nil and error on failure.
func (l *Logic) HandleScrape(ctx context.Context, req *bittorrent.ScrapeRequest) (_ context.Context, resp *bittorrent.ScrapeResponse, err error) {
	logger.Debug().Object("request", req).Msg("new scrape request")
	resp = &bittorrent.ScrapeResponse{
		Files: make([]bittorrent.Scrape, 0, len(req.InfoHashes)),
	}
	for _, h := range l.preHooks {
		if ctx, err = h.HandleScrape(ctx, req, resp); err != nil {
			return nil, nil, err
		}
	}
	sort.Sort(&resp.Files)

	logger.Debug().Object("response", resp).Msg("generated scrape response")
	return ctx, resp, nil
}

// AfterScrape does something with the results of a Scrape after it has been completed.
func (l *Logic) AfterScrape(ctx context.Context, req *bittorrent.ScrapeRequest, resp *bittorrent.ScrapeResponse) {
	var err error
	for _, h := range l.postHooks {
		if ctx, err = h.HandleScrape(ctx, req, resp); err != nil {
			logger.Error().
				Err(err).
				Object("request", req).
				Object("response", resp).
				Msg("post-scrape hooks failed")
			return
		}
	}
}

// Ping executes checks if all Hook-s are operational
func (l *Logic) Ping(ctx context.Context) (err error) {
	for _, p := range l.pingers {
		if err = p.Ping(ctx); err != nil {
			break
		}
	}
	return
}

// Stop stops the Logic.
//
// This stops any hooks that implement stop.Stopper.
func (l *Logic) Stop() stop.Result {
	stopGroup := stop.NewGroup()
	for _, hook := range l.preHooks {
		stoppable, ok := hook.(stop.Stopper)
		if ok {
			stopGroup.Add(stoppable)
		}
	}

	for _, hook := range l.postHooks {
		stoppable, ok := hook.(stop.Stopper)
		if ok {
			stopGroup.Add(stoppable)
		}
	}

	return stopGroup.Stop()
}
