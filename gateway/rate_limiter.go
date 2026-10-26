package main

import (
	"math"
	"sync"
	"time"
)

// rateLimiter owns per-credential token buckets and their bounded-memory eviction.
type rateLimiter struct {
	ratePerSecond float64
	burst         int
	bucketsMu     sync.Mutex
	buckets       map[string]*tokenBucket
}

type tokenBucket struct {
	mu         sync.Mutex
	tokens     float64
	lastRefill time.Time
}

func newRateLimiter(ratePerSecond float64, burst int) *rateLimiter {
	return &rateLimiter{ratePerSecond: ratePerSecond, burst: burst, buckets: make(map[string]*tokenBucket)}
}

// allow locks the map before each bucket so eviction cannot detach a bucket in use.
func (l *rateLimiter) allow(key string) (bool, int, int) {
	l.bucketsMu.Lock()
	bucket := l.buckets[key]
	if bucket == nil {
		bucket = &tokenBucket{tokens: float64(l.burst), lastRefill: time.Now()}
		l.buckets[key] = bucket
	}
	bucket.mu.Lock()
	l.bucketsMu.Unlock()
	defer bucket.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(bucket.lastRefill).Seconds()
	if elapsed > 0 {
		bucket.tokens = math.Min(float64(l.burst), bucket.tokens+elapsed*l.ratePerSecond)
		bucket.lastRefill = now
	}
	if bucket.tokens >= 1 {
		bucket.tokens--
		return true, int(math.Floor(bucket.tokens)), 0
	}
	retrySeconds := int(math.Ceil((1 - bucket.tokens) / l.ratePerSecond))
	if retrySeconds < 1 {
		retrySeconds = 1
	}
	return false, 0, retrySeconds
}

func (l *rateLimiter) startBucketEvictionLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for now := range ticker.C {
		l.evictStaleBuckets(now)
	}
}

func (l *rateLimiter) evictStaleBuckets(now time.Time) int {
	idleTTL := time.Duration(math.Max(60, 10*float64(l.burst)/l.ratePerSecond) * float64(time.Second))
	evicted := 0
	l.bucketsMu.Lock()
	defer l.bucketsMu.Unlock()
	for key, bucket := range l.buckets {
		bucket.mu.Lock()
		if now.Sub(bucket.lastRefill) > idleTTL {
			delete(l.buckets, key)
			evicted++
		}
		bucket.mu.Unlock()
	}
	return evicted
}
