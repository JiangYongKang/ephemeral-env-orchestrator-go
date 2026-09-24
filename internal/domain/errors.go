package domain

import (
	"errors"
	"fmt"
)

// ErrorKind classifies every failure so callers can distinguish illegal
// input, authorization problems, quota problems, idempotent duplicates and
// backend/transient failures.
type ErrorKind string

const (
	KindValidation   ErrorKind = "validation_error"
	KindUnauthorized ErrorKind = "unauthorized"
	KindForbidden    ErrorKind = "forbidden"
	KindNotFound     ErrorKind = "not_found"
	KindConflict     ErrorKind = "conflict"
	KindQuota        ErrorKind = "quota_exceeded"
	KindImmutable    ErrorKind = "immutable_field"
	KindDuplicate    ErrorKind = "duplicate"
	KindBackend      ErrorKind = "backend_error"
	KindTerminal     ErrorKind = "terminal_env"
)

// Error is the classified error type used across the orchestrator.
type Error struct {
	Kind ErrorKind
	Msg  string
	// Distinct lets the caller tell apart different rejection reasons of the
	// same kind, e.g. tenant-quota vs global-capacity.
	Distinct string
}

func (e *Error) Error() string {
	if e.Distinct != "" {
		return fmt.Sprintf("%s [%s]: %s", e.Kind, e.Distinct, e.Msg)
	}
	return string(e.Kind) + ": " + e.Msg
}

// NewError builds a classified error.
func NewError(kind ErrorKind, distinct, format string, args ...any) *Error {
	return &Error{Kind: kind, Distinct: distinct, Msg: fmt.Sprintf(format, args...)}
}

// AsError extracts *Error from err, or nil.
func AsError(err error) *Error {
	var e *Error
	if errors.As(err, &e) {
		return e
	}
	return nil
}

// IsKind reports whether err carries the given classification.
func IsKind(err error, kind ErrorKind) bool {
	if e := AsError(err); e != nil {
		return e.Kind == kind
	}
	return false
}
