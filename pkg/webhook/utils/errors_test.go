package utils

import (
	"strings"
	"testing"

	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/types"
)

func TestNeedRetryWithApiReaderErrorWrapsCause(t *testing.T) {
	staleErr := &StaleRuntimeInfoCacheError{
		Key:    types.NamespacedName{Name: "pvc", Namespace: "default"},
		Reason: "PVC resourceVersion changed",
	}

	wrapped := NewNeedRetryWithApiReaderError(errors.Wrapf(staleErr, "failed to collect runtime infos from PVCs"))
	if wrapped == nil {
		t.Fatalf("expected a non-nil retry error")
	}
	if !IsNeedRetryWithApiReaderError(wrapped) {
		t.Fatalf("expected the error to be recognized as a retry-with-api-reader error")
	}
	if !IsStaleRuntimeInfoCacheError(wrapped) {
		t.Fatalf("expected the wrapped stale error to remain discoverable through errors.As")
	}
	if wrapped.Error() == "" {
		t.Fatalf("expected a non-empty error message")
	}
}

func TestNewNeedRetryWithApiReaderErrorNilInput(t *testing.T) {
	if NewNeedRetryWithApiReaderError(nil) != nil {
		t.Fatalf("expected nil retry error for nil input")
	}
}

func TestStaleRuntimeInfoCacheErrorMessage(t *testing.T) {
	err := &StaleRuntimeInfoCacheError{
		Key:    types.NamespacedName{Name: "pvc", Namespace: "default"},
		Reason: "Dataset UID changed",
	}
	if !IsStaleRuntimeInfoCacheError(err) {
		t.Fatalf("expected the error to be recognized as stale")
	}
	if IsStaleRuntimeInfoCacheError(nil) {
		t.Fatalf("nil error must not be recognized as stale")
	}
	msg := err.Error()
	for _, fragment := range []string{"default/pvc", "Dataset UID changed"} {
		if !strings.Contains(msg, fragment) {
			t.Fatalf("expected error message %q to contain %q", msg, fragment)
		}
	}
}
