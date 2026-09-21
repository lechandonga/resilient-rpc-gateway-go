package registry

import "errors"

// Registry errors.
var (
	ErrInvalidInstance  = errors.New("registry: invalid instance (empty service/id or negative weight)")
	ErrInstanceNotFound = errors.New("registry: instance not found")
)
