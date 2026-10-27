package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"regexp"
	"syscall"
	"testing"

	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"
)

func TestStartErrorsAreNamedByStageAndClassNeverByText(t *testing.T) {
	fixed := regexp.MustCompile(`^[a-z0-9_.]{1,64}$`)
	cases := []struct {
		stage string
		err   error
		want  string
	}{
		{"load_page", fmt.Errorf("get page: %w", context.DeadlineExceeded), "load_page_timeout"},
		{"load_page", &net.DNSError{Err: "no such host", Name: "www.facebook.com"}, "load_page_dns"},
		{"connect", fmt.Errorf("dial: %w", syscall.ECONNREFUSED), "connect_refused"},
		{"load_page", errors.New("unexpected page shape containing secret-ish text"), "load_page_other"},
	}
	for _, c := range cases {
		err := classifyStartError(c.stage, c.err)
		var ce *codeErr
		if !errors.As(err, &ce) || ce.code != c.want || !fixed.MatchString(ce.code) {
			t.Fatalf("%v: got %v, want code %q", c.err, err, c.want)
		}
	}
	// An auth answer keeps its auth meaning: the connector asks the user to sign in again.
	var auth *authErr
	if !errors.As(classifyStartError("load_page", httpclient.ErrTokenInvalidated), &auth) {
		t.Fatal("an invalidated token must stay an auth answer")
	}
}
