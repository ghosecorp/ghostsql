package util

import "fmt"

type ErrorCode int

const (
	ErrOK ErrorCode = iota
	ErrInternal
	ErrNotFound
	ErrAlreadyExists
	ErrInvalidArgument
	ErrIO
	ErrCorrupted
)

type GhostError struct {
	Code    ErrorCode
	Message string
	Cause   error
}

func (e *GhostError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("[%d] %s: %v", e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("[%d] %s", e.Code, e.Message)
}

func NewError(code ErrorCode, message string, cause error) *GhostError {
	return &GhostError{
		Code:    code,
		Message: message,
		Cause:   cause,
	}
}
