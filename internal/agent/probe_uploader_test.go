package agent

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestProbeUploaderRetriesRetryableStatusesWithPersistentExponentialBackoff(t *testing.T) {
	for _, status := range []int{http.StatusInternalServerError, http.StatusInsufficientStorage} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			now := time.Date(2026, 7, 17, 5, 0, 0, 0, time.UTC)
			clock := now
			var requests atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if requests.Add(1) <= 2 {
					http.Error(w, "controller-body-secret", status)
					return
				}
				w.WriteHeader(http.StatusAccepted)
			}))
			defer server.Close()
			spool := newTestProbeSpool(t, func() time.Time { return clock })
			id, err := spool.Enqueue([]ProbeRound{testSpoolRound(now, "stable-retry-round")})
			if err != nil {
				t.Fatal(err)
			}
			var caps []time.Duration
			uploader := NewProbeUploader(spool, newTestClient(server.URL, "node", "token"), ProbeUploaderOptions{
				Now: func() time.Time { return clock },
				Jitter: func(cap time.Duration) time.Duration {
					caps = append(caps, cap)
					return cap
				},
			})
			if err := uploader.UploadOne(context.Background()); err == nil || strings.Contains(err.Error(), "controller-body-secret") {
				t.Fatalf("first retry error = %v", err)
			}
			item, err := spool.Load(id)
			if err != nil || item.Attempt != 1 || !item.NextAt.Equal(now.Add(time.Second)) {
				t.Fatalf("first retry metadata = %#v, %v", item, err)
			}
			// Restarting the spool must preserve the retry gate.
			restarted := restartTestProbeSpool(t, spool, func() time.Time { return clock })
			if due, err := restarted.Next(clock); err != nil || due != nil {
				t.Fatalf("retry became due before next_at after restart: %#v, %v", due, err)
			}
			clock = now.Add(time.Second)
			uploader = NewProbeUploader(restarted, newTestClient(server.URL, "node", "token"), ProbeUploaderOptions{
				Now: func() time.Time { return clock },
				Jitter: func(cap time.Duration) time.Duration {
					caps = append(caps, cap)
					return cap
				},
			})
			if err := uploader.UploadOne(context.Background()); err == nil {
				t.Fatal("second retry returned nil")
			}
			item, err = restarted.Load(id)
			if err != nil || item.Attempt != 2 || !item.NextAt.Equal(clock.Add(2*time.Second)) {
				t.Fatalf("second retry metadata = %#v, %v", item, err)
			}
			if len(caps) != 2 || caps[0] != time.Second || caps[1] != 2*time.Second {
				t.Fatalf("backoff caps = %v, want [1s 2s]", caps)
			}
			clock = item.NextAt
			if err := uploader.UploadOne(context.Background()); err != nil {
				t.Fatalf("successful retry: %v", err)
			}
			if _, err := restarted.Load(id); !os.IsNotExist(err) {
				t.Fatalf("2xx did not ACK/unlink item: %v", err)
			}
		})
	}
}

func TestProbeUploaderUsesClampedRetryAfterFor429(t *testing.T) {
	now := time.Date(2026, 7, 17, 6, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "600")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()
	spool := newTestProbeSpool(t, func() time.Time { return now })
	id, err := spool.Enqueue([]ProbeRound{testSpoolRound(now, "rate-limited")})
	if err != nil {
		t.Fatal(err)
	}
	uploader := NewProbeUploader(spool, newTestClient(server.URL, "node", "token"), ProbeUploaderOptions{
		Now: func() time.Time { return now }, Jitter: func(time.Duration) time.Duration { return 0 },
	})
	if err := uploader.UploadOne(context.Background()); err == nil {
		t.Fatal("429 returned nil")
	}
	item, err := spool.Load(id)
	if err != nil || !item.NextAt.Equal(now.Add(5*time.Minute)) {
		t.Fatalf("Retry-After metadata = %#v, %v; want clamped 5m", item, err)
	}
}

func TestProbeUploaderRetriesTimeoutAndDoesNotLeakTransportText(t *testing.T) {
	now := time.Date(2026, 7, 17, 7, 0, 0, 0, time.UTC)
	spool := newTestProbeSpool(t, func() time.Time { return now })
	id, err := spool.Enqueue([]ProbeRound{testSpoolRound(now, "timeout-round")})
	if err != nil {
		t.Fatal(err)
	}
	client := newTestClient("http://127.0.0.1:12345", "node", "token")
	client.http = &http.Client{Transport: roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		<-request.Context().Done()
		return nil, request.Context().Err()
	})}
	uploader := NewProbeUploader(spool, client, ProbeUploaderOptions{
		Now: func() time.Time { return now }, Jitter: func(cap time.Duration) time.Duration { return cap },
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := uploader.UploadOne(ctx); err == nil {
		t.Fatal("timeout returned nil")
	}
	item, err := spool.Load(id)
	if err != nil || item.Attempt != 1 || !item.NextAt.Equal(now.Add(time.Second)) {
		t.Fatalf("timeout retry metadata = %#v, %v", item, err)
	}
}

func TestProbeUploaderQuarantinesTerminalStatuses(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusConflict, http.StatusRequestEntityTooLarge} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			now := time.Date(2026, 7, 17, 8, 0, 0, 0, time.UTC)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "terminal-body-secret", status)
			}))
			defer server.Close()
			spool := newTestProbeSpool(t, func() time.Time { return now })
			id, err := spool.Enqueue([]ProbeRound{testSpoolRound(now, "terminal-round")})
			if err != nil {
				t.Fatal(err)
			}
			uploader := NewProbeUploader(spool, newTestClient(server.URL, "node", "token"), ProbeUploaderOptions{Now: func() time.Time { return now }})
			if err := uploader.UploadOne(context.Background()); err == nil || strings.Contains(err.Error(), "terminal-body-secret") {
				t.Fatalf("terminal error = %v", err)
			}
			if _, err := spool.Load(id); !os.IsNotExist(err) {
				t.Fatalf("terminal item remained pending: %v", err)
			}
			entries, err := os.ReadDir(spool.quarantineDir)
			if err != nil || len(entries) != 1 {
				t.Fatalf("terminal quarantine = %v, %v", entries, err)
			}
		})
	}
}

func TestProbeUploaderResponseLossRetriesIdenticalBodyAndRoundIDs(t *testing.T) {
	now := time.Date(2026, 7, 17, 9, 0, 0, 0, time.UTC)
	clock := now
	var bodies [][]byte
	var mu sync.Mutex
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, append([]byte(nil), body...))
		mu.Unlock()
		if requests.Add(1) == 1 {
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				t.Error("server does not support hijacking")
				return
			}
			connection, _, err := hijacker.Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			_ = connection.Close()
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	spool := newTestProbeSpool(t, func() time.Time { return clock })
	id, err := spool.Enqueue([]ProbeRound{testSpoolRound(now, "response-loss-round")})
	if err != nil {
		t.Fatal(err)
	}
	uploader := NewProbeUploader(spool, newTestClient(server.URL, "node", "token"), ProbeUploaderOptions{
		Now: func() time.Time { return clock }, Jitter: func(cap time.Duration) time.Duration { return cap },
	})
	if err := uploader.UploadOne(context.Background()); err == nil {
		t.Fatal("lost response returned nil")
	}
	clock = now.Add(time.Second)
	if err := uploader.UploadOne(context.Background()); err != nil {
		t.Fatalf("retry after lost response: %v", err)
	}
	if _, err := spool.Load(id); !os.IsNotExist(err) {
		t.Fatalf("ACKed response-loss item still pending: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(bodies) != 2 || string(bodies[0]) != string(bodies[1]) || !strings.Contains(string(bodies[1]), "response-loss-round") {
		t.Fatalf("response-loss bodies changed: %q", bodies)
	}
}

func TestProbeUploaderRetries408AndQuarantinesRedirect(t *testing.T) {
	for _, test := range []struct {
		name      string
		status    int
		retryable bool
	}{
		{name: "request timeout", status: http.StatusRequestTimeout, retryable: true},
		{name: "redirect", status: http.StatusFound, retryable: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := time.Date(2026, 7, 17, 9, 30, 0, 0, time.UTC)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Location", "/must-not-follow")
				w.WriteHeader(test.status)
			}))
			defer server.Close()

			spool := newTestProbeSpool(t, func() time.Time { return now })
			id, err := spool.Enqueue([]ProbeRound{testSpoolRound(now, "status-classification")})
			if err != nil {
				t.Fatal(err)
			}
			uploader := NewProbeUploader(spool, newTestClient(server.URL, "node", "token"), ProbeUploaderOptions{
				Now: func() time.Time { return now }, Jitter: func(cap time.Duration) time.Duration { return cap },
			})
			err = uploader.UploadOne(context.Background())
			if err == nil || !isAgentAPIStatus(err, test.status) {
				t.Fatalf("UploadOne error = %v; want typed status %d", err, test.status)
			}

			if test.retryable {
				item, loadErr := spool.Load(id)
				if loadErr != nil || item.Attempt != 1 || !item.NextAt.Equal(now.Add(time.Second)) {
					t.Fatalf("408 retry metadata = %#v, %v", item, loadErr)
				}
				return
			}
			if _, loadErr := spool.Load(id); !os.IsNotExist(loadErr) {
				t.Fatalf("non-2xx item remained pending: %v", loadErr)
			}
			quarantined, readErr := os.ReadDir(spool.quarantineDir)
			if readErr != nil || len(quarantined) != 1 {
				t.Fatalf("redirect quarantine = %v, %v", quarantined, readErr)
			}
		})
	}
}

func TestProbeUploaderConflictReturnsTypedStaleError(t *testing.T) {
	now := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":"stale_probe_config"}`))
	}))
	defer server.Close()

	spool := newTestProbeSpool(t, func() time.Time { return now })
	if _, err := spool.Enqueue([]ProbeRound{testSpoolRound(now, "stale-config")}); err != nil {
		t.Fatal(err)
	}
	uploader := NewProbeUploader(spool, newTestClient(server.URL, "node", "token"), ProbeUploaderOptions{Now: func() time.Time { return now }})
	err := uploader.UploadOne(context.Background())
	var staleErr *ProbeUploadStaleError
	if !errors.As(err, &staleErr) || !isAgentAPIStatus(err, http.StatusConflict) {
		t.Fatalf("conflict error = %T %v; want typed stale/status error", err, err)
	}
}

func TestProbeUploaderRoundConflictIsDistinctTerminalOutcome(t *testing.T) {
	now := time.Date(2026, 7, 17, 10, 15, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":"probe_round_conflict"}`))
	}))
	defer server.Close()
	spool := newTestProbeSpool(t, func() time.Time { return now })
	if _, err := spool.Enqueue([]ProbeRound{testSpoolRound(now, "round-conflict")}); err != nil {
		t.Fatal(err)
	}
	uploader := NewProbeUploader(spool, newTestClient(server.URL, "node", "token"), ProbeUploaderOptions{Now: func() time.Time { return now }})
	err := uploader.UploadOne(context.Background())
	var conflict *ProbeUploadRoundConflictError
	var stale *ProbeUploadStaleError
	if !errors.As(err, &conflict) || errors.As(err, &stale) || !isAgentAPIStatus(err, http.StatusConflict) {
		t.Fatalf("conflict error = %T %v", err, err)
	}
}

func TestProbeUploaderSanitizesTransportErrorAndPersistsNetworkRetry(t *testing.T) {
	now := time.Date(2026, 7, 17, 10, 30, 0, 0, time.UTC)
	spool := newTestProbeSpool(t, func() time.Time { return now })
	id, err := spool.Enqueue([]ProbeRound{testSpoolRound(now, "network-failure")})
	if err != nil {
		t.Fatal(err)
	}
	client := newTestClient("http://127.0.0.1:12345", "node", "token")
	client.http = &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("transport-token-secret")
	})}
	uploader := NewProbeUploader(spool, client, ProbeUploaderOptions{
		Now: func() time.Time { return now }, Jitter: func(cap time.Duration) time.Duration { return cap },
	})
	err = uploader.UploadOne(context.Background())
	if err == nil || strings.Contains(err.Error(), "transport-token-secret") {
		t.Fatalf("sanitized transport error = %v", err)
	}
	item, loadErr := spool.Load(id)
	if loadErr != nil || item.Attempt != 1 || !item.NextAt.Equal(now.Add(time.Second)) {
		t.Fatalf("network retry metadata = %#v, %v", item, loadErr)
	}
}

func TestProbeUploaderCancellationDoesNotConsumeAttempt(t *testing.T) {
	now := time.Date(2026, 7, 17, 11, 0, 0, 0, time.UTC)
	spool := newTestProbeSpool(t, func() time.Time { return now })
	id, err := spool.Enqueue([]ProbeRound{testSpoolRound(now, "cancelled")})
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	client := newTestClient("http://127.0.0.1:12345", "node", "token")
	client.http = &http.Client{Transport: roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		close(started)
		<-request.Context().Done()
		return nil, request.Context().Err()
	})}
	uploader := NewProbeUploader(spool, client, ProbeUploaderOptions{Now: func() time.Time { return now }})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- uploader.UploadOne(ctx) }()
	<-started
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("UploadOne cancellation = %v", err)
	}
	item, loadErr := spool.Load(id)
	if loadErr != nil || item.Attempt != 0 || !item.NextAt.Equal(now) {
		t.Fatalf("cancellation changed retry metadata = %#v, %v", item, loadErr)
	}
}

func TestProbeUploaderRunStopsWhileIdleOnContextCancellation(t *testing.T) {
	now := time.Date(2026, 7, 17, 11, 30, 0, 0, time.UTC)
	spool := newTestProbeSpool(t, func() time.Time { return now })
	uploader := NewProbeUploader(spool, newTestClient("http://127.0.0.1:12345", "node", "token"), ProbeUploaderOptions{
		Now: func() time.Time { return now }, PollInterval: time.Hour,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- uploader.Run(ctx) }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run cancellation = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not stop on context cancellation")
	}
}

func TestProbeUploaderRunDistinguishesRetryable502AndTimeoutFromTerminalQuarantine(t *testing.T) {
	now := time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC)
	var step atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch step.Add(1) {
		case 1:
			http.Error(w, "upstream bad gateway", http.StatusBadGateway)
		case 2:
			http.Error(w, "upstream unavailable", http.StatusServiceUnavailable)
		default:
			http.Error(w, "unauthorized", http.StatusUnauthorized)
		}
	}))
	defer server.Close()

	clockMu := sync.Mutex{}
	clock := now
	nowFn := func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return clock
	}
	advanceClock := func(d time.Duration) {
		clockMu.Lock()
		clock = clock.Add(d)
		clockMu.Unlock()
	}

	spool := newTestProbeSpool(t, nowFn)
	id, err := spool.Enqueue([]ProbeRound{testSpoolRound(now, "retry-then-quarantine")})
	if err != nil {
		t.Fatal(err)
	}

	retryCh := make(chan *ProbeUploadRetryError, 4)
	rejectedCh := make(chan error, 4)
	uploader := NewProbeUploader(spool, newTestClient(server.URL, "node", "token"), ProbeUploaderOptions{
		Now:          nowFn,
		Jitter:       func(cap time.Duration) time.Duration { return cap },
		PollInterval: 5 * time.Millisecond,
		OnRetry: func(_ context.Context, retryErr *ProbeUploadRetryError) {
			retryCh <- retryErr
		},
		OnRejected: func(_ context.Context, rejectErr error) {
			rejectedCh <- rejectErr
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- uploader.Run(ctx) }()

	// Attempt 1 -> HTTP 502: must invoke OnRetry (NOT OnRejected) and stay in spool.
	select {
	case got := <-retryCh:
		if got.StatusCode != http.StatusBadGateway || got.Attempt != 1 {
			t.Fatalf("first retry callback = %+v, want 502 attempt 1", got)
		}
	case err := <-rejectedCh:
		t.Fatalf("502 retryable response unexpectedly triggered OnRejected: %v", err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for 502 retry callback")
	}
	if entries, err := os.ReadDir(spool.quarantineDir); err != nil || len(entries) != 0 {
		t.Fatalf("502 item moved to quarantine = %v, %v", entries, err)
	}
	if item, err := spool.Load(id); err != nil || item == nil {
		t.Fatalf("502 item missing from pending spool: %#v, %v", item, err)
	}

	// Attempt 2 -> HTTP 503: still retryable, OnRetry only.
	advanceClock(2 * time.Second)
	select {
	case got := <-retryCh:
		if got.StatusCode != http.StatusServiceUnavailable || got.Attempt != 2 {
			t.Fatalf("second retry callback = %+v, want 503 attempt 2", got)
		}
	case err := <-rejectedCh:
		t.Fatalf("503 retryable response unexpectedly triggered OnRejected: %v", err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for 503 retry callback")
	}

	// Attempt 3 -> HTTP 401: terminal rejection, moved to quarantine and OnRejected invoked.
	advanceClock(5 * time.Second)
	select {
	case got := <-rejectedCh:
		if !isAgentAPIStatus(got, http.StatusUnauthorized) {
			t.Fatalf("terminal rejection error = %v, want 401 status error", got)
		}
	case retryErr := <-retryCh:
		t.Fatalf("401 terminal response unexpectedly triggered OnRetry: %+v", retryErr)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for 401 rejection callback")
	}
	if entries, err := os.ReadDir(spool.quarantineDir); err != nil || len(entries) != 1 {
		t.Fatalf("401 terminal item quarantine entries = %v, %v; want 1", entries, err)
	}
	cancel()
	<-done
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (fn roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func newTestProbeSpool(t *testing.T, now func() time.Time) *ProbeSpool {
	t.Helper()
	spool, err := NewProbeSpool(t.TempDir(), ProbeSpoolOptions{Now: now, FreeBytes: func(string) (uint64, error) { return 1 << 40, nil }})
	if err != nil {
		t.Fatal(err)
	}
	return spool
}

func restartTestProbeSpool(t *testing.T, old *ProbeSpool, now func() time.Time) *ProbeSpool {
	t.Helper()
	dataDir := strings.TrimSuffix(old.root, string(os.PathSeparator)+"probe-spool")
	spool, err := NewProbeSpool(dataDir, ProbeSpoolOptions{Now: now, FreeBytes: func(string) (uint64, error) { return 1 << 40, nil }})
	if err != nil {
		t.Fatal(err)
	}
	return spool
}

var _ net.Error
