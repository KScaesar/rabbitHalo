package rabbitHalo

import (
	"errors"
)

var (
	ErrNotFoundHandler = errors.New("not found handler")
	ErrResourceClosed  = errors.New("resource closed")
)
