// Package errors defines classified, explainable error types so callers can
// distinguish validation failures, permission denials, quota exhaustion,
// conflicts and transient backend failures.
package errors

// Kind classifies an error cause.
type Kind string

const (
	KindValidation   Kind = "ValidationError"
	KindNotFound     Kind = "NotFound"
	KindUnauthorized Kind = "Unauthorized"
	KindForbidden    Kind = "Forbidden"
	KindQuota        Kind = "QuotaExceeded"
	KindConflict     Kind = "Conflict"
	KindImmutable    Kind = "ImmutableField"
	KindTransient    Kind = "TransientFailure"
	KindInternal     Kind = "Internal"
)

// Error is the classified error type used across the orchestrator.
type Error struct {
	Kind    Kind
	Message string
}

func (e *Error) Error() string { return string(e.Kind) + ": " + e.Message }

// New builds a classified error.
func New(kind Kind, msg string) *Error { return &Error{Kind: kind, Message: msg} }

// Is reports whether err has the given kind.
func Is(err error, kind Kind) bool {
	e, ok := err.(*Error)
	return ok && e.Kind == kind
}
