package bittorrent

import (
	"strings"
)

// ErrUnknownEvent is returned when New fails to return an event.
var ErrUnknownEvent = ClientError("unknown event")

// Event represents an event done by a BitTorrent client.
type Event uint8

const (
	// None is the event when a BitTorrent client announces due to time lapsed
	// since the previous announce.
	None Event = iota

	// Started is the event sent by a BitTorrent client when it joins a swarm.
	Started

	// Stopped is the event sent by a BitTorrent client when it leaves a swarm.
	Stopped

	// Completed is the event sent by a BitTorrent client when it finishes
	// downloading all of the required chunks.
	Completed

	// NoneStr string representation of None event
	NoneStr = "none"

	// StartedStr string representation of Started event
	StartedStr = "started"

	// StoppedStr string representation of Stopped event
	StoppedStr = "stopped"

	// CompletedStr string representation of Completed event
	CompletedStr = "completed"
)

// NewEvent returns the proper Event given a string.
func NewEvent(eventStr string) (evt Event, err error) {
	switch strings.ToLower(eventStr) {
	case NoneStr, "":
		evt = None
	case StartedStr:
		evt = Started
	case StoppedStr:
		evt = Stopped
	case CompletedStr:
		evt = Completed
	default:
		evt, err = None, ErrUnknownEvent
	}
	return evt, err
}

// String implements Stringer for an event.
func (e Event) String() (s string) {
	switch e {
	case None:
		s = NoneStr
	case Started:
		s = StartedStr
	case Stopped:
		s = StoppedStr
	case Completed:
		s = CompletedStr
	default:
		s = "<unknown>"
	}
	return s
}
