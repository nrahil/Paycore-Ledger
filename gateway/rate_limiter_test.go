package main

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestRateLimiterEnforcesBurstAcrossConcurrentRequests(t *testing.T) {
	const burst = 10
	limiter := newRateLimiter(1, burst)
	var allowed atomic.Int64
	var workers sync.WaitGroup
	for i := 0; i < 100; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			if ok, _, _ := limiter.allow("same-merchant"); ok {
				allowed.Add(1)
			}
		}()
	}
	workers.Wait()
	if got := allowed.Load(); got != burst {
		t.Fatalf("expected exactly %d requests within burst, got %d", burst, got)
	}
}
