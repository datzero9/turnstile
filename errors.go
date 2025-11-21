package turnstile

import "errors"

var (
	// Configuration errors
	ErrNoBrokers             = errors.New("turnstile: no brokers specified")
	ErrNoGroupID             = errors.New("turnstile: no group ID specified")
	ErrNoTopic               = errors.New("turnstile: no topic specified")
	ErrNoHandler             = errors.New("turnstile: no message handler specified")
	// Runtime errors
	ErrConsumerClosed  = errors.New("turnstile: consumer is closed")
	ErrContextCanceled = errors.New("turnstile: context canceled")
)
