package utils

import (
	"errors"
	"fmt"

	"k8s.io/apimachinery/pkg/types"
)

type NeedRetryWithApiReaderError struct {
	ErrMsg string
	// cause keeps the original error so that wrapped errors (e.g. stale cache errors)
	// remain discoverable through errors.As/Is after the retry marker is attached.
	cause error
}

var _ error = &NeedRetryWithApiReaderError{}

// Error implements the Error interface.
func (e *NeedRetryWithApiReaderError) Error() string {
	return e.ErrMsg
}

// Unwrap returns the original error that triggers the retry with the API reader.
func (e *NeedRetryWithApiReaderError) Unwrap() error {
	return e.cause
}

func IsNeedRetryWithApiReaderError(err error) bool {
	if err == nil {
		return false
	}
	var retryErr *NeedRetryWithApiReaderError
	return errors.As(err, &retryErr)
}

func NewNeedRetryWithApiReaderError(err error) *NeedRetryWithApiReaderError {
	if err == nil {
		return nil
	}
	return &NeedRetryWithApiReaderError{
		ErrMsg: err.Error(),
		cause:  err,
	}
}

// StaleRuntimeInfoCacheError indicates that a runtime info cache entry failed identity
// validation against the current PVC/Dataset/Runtime objects (for example because one of
// them was updated, deleted, recreated or changed type). When this happens, no partially
// refreshed result may be used: the whole pod mutation must be redone from the original
// pod with the direct API reader, which bypasses the cache and rebuilds one consistent
// snapshot. If the direct API read fails as well, the admission must be rejected.
type StaleRuntimeInfoCacheError struct {
	Key    types.NamespacedName
	Reason string
}

var _ error = &StaleRuntimeInfoCacheError{}

// Error implements the Error interface.
func (e *StaleRuntimeInfoCacheError) Error() string {
	return fmt.Sprintf("stale runtime info cache entry for PVC %q: %s", e.Key.String(), e.Reason)
}

// IsStaleRuntimeInfoCacheError reports whether err is a StaleRuntimeInfoCacheError,
// unwrapping wrapped errors when necessary.
func IsStaleRuntimeInfoCacheError(err error) bool {
	if err == nil {
		return false
	}
	var staleErr *StaleRuntimeInfoCacheError
	return errors.As(err, &staleErr)
}
