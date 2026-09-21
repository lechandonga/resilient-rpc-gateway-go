package gerr

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestNewWrapAndCodeOf(t *testing.T) {
	e := New(CodeNoInstance, "empty")
	if e.Error() != "no_instance: empty" {
		t.Fatalf("got %q", e.Error())
	}
	w := Wrap(CodeDownstream, "call failed", context.Canceled)
	if !errors.Is(w, context.Canceled) {
		t.Fatal("Unwrap must expose the cause")
	}
	cases := map[error]Code{
		context.Canceled:          CodeCanceled,
		context.DeadlineExceeded:  CodeDeadlineExceeded,
		New(CodeCircuitOpen, "x"): CodeCircuitOpen,
		fmt.Errorf("plain"):       CodeUnknown,
		nil:                       "",
	}
	for in, want := range cases {
		if got := CodeOf(in); got != want {
			t.Errorf("CodeOf(%v) = %s, want %s", in, got, want)
		}
	}
}
