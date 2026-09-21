// Package gerr defines distinguishable governance error codes.
//
// Every failure produced by the governance layer carries a stable Code
// so callers and tests can distinguish failure *reasons* (no instances,
// timeout, open circuit, retry budget exhausted, ...) instead of only
// matching strings.
package gerr

import (
	"context"
	"errors"
)

// Code is a stable, distinguishable error reason code.
type Code string

// Defined error reason codes.
const (
	CodeUnknown               Code = "unknown"
	CodeNoInstance            Code = "no_instance"
	CodeInstanceUnhealthy     Code = "instance_unhealthy"
	CodeDeadlineExceeded      Code = "deadline_exceeded"
	CodeCanceled              Code = "canceled"
	CodeTimeoutBeforeDispatch Code = "timeout_before_dispatch"
	CodeRetryBudgetExhausted  Code = "retry_budget_exhausted"
	CodeNonRetryable          Code = "non_retryable"
	CodeCircuitOpen           Code = "circuit_open"
	CodeConcurrencyLimit      Code = "concurrency_limit"
	CodeQueueLimit            Code = "queue_limit"
	CodeRejected              Code = "rejected"
	CodeConfigInvalid         Code = "config_invalid"
	CodeDownstream            Code = "downstream_error"
)

// Error is a governance error carrying a stable reason code.
type Error struct {
	Code    Code
	Message string
	Cause   error
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	s := string(e.Code)
	if e.Message != "" {
		s += ": " + e.Message
	}
	if e.Cause != nil {
		s += ": " + e.Cause.Error()
	}
	return s
}

// Unwrap exposes the wrapped cause for errors.Is/As.
func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// New builds a governance error without a cause.
func New(code Code, msg string) *Error {
	return &Error{Code: code, Message: msg}
}

// Wrap builds a governance error wrapping a cause.
func Wrap(code Code, msg string, cause error) *Error {
	return &Error{Code: code, Message: msg, Cause: cause}
}

// CodeOf extracts the governance code from an error.
// context.Canceled / context.DeadlineExceeded are mapped to their
// governance codes; anything unknown maps to CodeUnknown.
func CodeOf(err error) Code {
	if err == nil {
		return ""
	}
	var ge *Error
	if errors.As(err, &ge) {
		return ge.Code
	}
	if errors.Is(err, context.Canceled) {
		return CodeCanceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return CodeDeadlineExceeded
	}
	return CodeUnknown
}
