package ratelimiter

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const (
	testPerSecond = 2
	testPerMinute = 50
	testPerHour   = 300
)

func newTestLimiter(t *testing.T) *RateLimiter {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal("redis is required — start docker containers first: docker compose up -d")
	}
	client.Del(ctx,
		"ratelimit:per_second",
		"ratelimit:per_minute",
		"ratelimit:per_hour",
	)
	logger, _ := zap.NewDevelopment()
	return New(client, Config{
		PerSecond: testPerSecond,
		PerMinute: testPerMinute,
		PerHour:   testPerHour,
	}, logger)
}

func newClientForTest(t *testing.T) *redis.Client {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatal("redis is required — start docker containers first: docker compose up -d")
	}
	return client
}

func cleanKeys(client *redis.Client) {
	client.Del(context.Background(),
		"ratelimit:per_second",
		"ratelimit:per_minute",
		"ratelimit:per_hour",
	)
}

// Test 1: first 2 requests succeed, 3rd waits for bucket reset
func TestPerSecondLimit(t *testing.T) {
	rl := newTestLimiter(t)
	ctx := context.Background()

	if err := rl.Acquire(ctx); err != nil {
		t.Fatalf("request 1 failed: %v", err)
	}
	if err := rl.Acquire(ctx); err != nil {
		t.Fatalf("request 2 failed: %v", err)
	}

	start := time.Now()
	if err := rl.Acquire(ctx); err != nil {
		t.Fatalf("request 3 failed: %v", err)
	}
	elapsed := time.Since(start)

	if elapsed < 400*time.Millisecond {
		t.Errorf("3rd request should have waited, elapsed: %v", elapsed)
	}
	t.Logf("✅ 3rd request correctly waited: %v", elapsed)
}

// Test 2: per-minute bucket exhausts at limit=50
func TestPerMinuteLimit(t *testing.T) {
	rl := newTestLimiter(t)
	ctx := context.Background()
	timeoutCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	for i := 0; i < testPerMinute; i++ {
		if err := rl.Acquire(timeoutCtx); err != nil {
			t.Fatalf("request %d failed: %v", i+1, err)
		}
	}

	status := rl.Status(ctx)
	minuteStatus := status["per_minute"].(map[string]int)
	if minuteStatus["remaining"] != 0 {
		t.Errorf("expected 0 remaining, got %d", minuteStatus["remaining"])
	}
	t.Logf("✅ per_minute exhausted: remaining=%d", minuteStatus["remaining"])
}

// Test 3: concurrent goroutines — Lua atomicity verified
func TestConcurrentAccess(t *testing.T) {
	rl := newTestLimiter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	var mu sync.Mutex
	success := 0

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := rl.Acquire(ctx); err == nil {
				mu.Lock()
				success++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	if success != 10 {
		t.Errorf("expected 10 successes, got %d", success)
	}
	t.Logf("✅ concurrent acquisitions: %d/10", success)
}

// Test 4: state survives Go process restart
func TestStatePersistence(t *testing.T) {
	client := newClientForTest(t)
	ctx := context.Background()
	cleanKeys(client)

	logger, _ := zap.NewDevelopment()

	rl1 := New(client, Config{PerSecond: testPerSecond, PerMinute: testPerMinute, PerHour: testPerHour}, logger)
	if err := rl1.Acquire(ctx); err != nil {
		t.Fatal(err)
	}

	rl2 := New(client, Config{PerSecond: testPerSecond, PerMinute: testPerMinute, PerHour: testPerHour}, logger)
	status := rl2.Status(ctx)

	minuteStatus := status["per_minute"].(map[string]int)
	expected := testPerMinute - 1
	if minuteStatus["remaining"] != expected {
		t.Errorf("expected %d remaining after restart, got %d", expected, minuteStatus["remaining"])
	}
	t.Logf("✅ state persisted: per_minute remaining=%d", minuteStatus["remaining"])
}

// Test 5: no sliding window — TTL decreases, not reset on each request
func TestNoSlidingWindow(t *testing.T) {
	rl := newTestLimiter(t)
	ctx := context.Background()

	// Fire 2 requests to initialize the window
	for i := 0; i < testPerSecond; i++ {
		if err := rl.Acquire(ctx); err != nil {
			t.Fatalf("request %d failed: %v", i+1, err)
		}
	}

	// Wait 3 seconds so TTL decreases noticeably
	t.Log("waiting 3 seconds to observe TTL decrease...")
	time.Sleep(3 * time.Second)

	// Fire 2 more requests
	for i := 0; i < testPerSecond; i++ {
		if err := rl.Acquire(ctx); err != nil {
			t.Fatalf("request after wait %d failed: %v", i+1, err)
		}
	}

	// TTL should be ~57s (60 - 3s elapsed), NOT reset back to 60s
	// If sliding window bug existed, each DECRBY/SET would reset TTL to 60s
	ttl := rl.client.TTL(ctx, "ratelimit:per_minute").Val()
	t.Logf("per_minute TTL after 3s wait + 2 more requests: %v", ttl)

	if ttl > 58*time.Second {
		t.Errorf("sliding window bug! TTL %v was reset — should be ~57s not ~60s", ttl)
	}
	if ttl < 50*time.Second {
		t.Errorf("TTL %v too low — something else is wrong", ttl)
	}
	t.Logf("✅ no sliding window: TTL correctly decreased to %v (not reset to 60s)", ttl)

	// Same check for per_hour bucket
	ttlHour := rl.client.TTL(ctx, "ratelimit:per_hour").Val()
	t.Logf("per_hour TTL: %v", ttlHour)
	if ttlHour > 3597*time.Second {
		t.Errorf("sliding window bug on hour bucket! TTL %v was reset", ttlHour)
	}
	t.Logf("✅ no sliding window on hour bucket: TTL=%v", ttlHour)
}

// Test 6: fresh Redis gives full bucket on first request
func TestFirstEverStart(t *testing.T) {
	client := newClientForTest(t)
	ctx := context.Background()
	cleanKeys(client)

	logger, _ := zap.NewDevelopment()
	rl := New(client, Config{PerSecond: testPerSecond, PerMinute: testPerMinute, PerHour: testPerHour}, logger)

	if err := rl.Acquire(ctx); err != nil {
		t.Fatalf("first request failed: %v", err)
	}

	status := rl.Status(ctx)
	minuteStatus := status["per_minute"].(map[string]int)
	if minuteStatus["remaining"] != testPerMinute-1 {
		t.Errorf("expected %d remaining, got %d", testPerMinute-1, minuteStatus["remaining"])
	}
	t.Logf("✅ first start gets full bucket: remaining=%d", minuteStatus["remaining"])
}

// Test 7: after window expires, fresh window starts automatically
func TestWindowExpiry(t *testing.T) {
	client := newClientForTest(t)
	ctx := context.Background()
	cleanKeys(client)

	logger, _ := zap.NewDevelopment()
	rl := New(client, Config{PerSecond: testPerSecond, PerMinute: testPerMinute, PerHour: testPerHour}, logger)

	// Consume 1 token
	if err := rl.Acquire(ctx); err != nil {
		t.Fatal(err)
	}

	// Simulate window expiry by deleting the key
	client.Del(ctx, "ratelimit:per_minute")

	// Next request should get fresh full bucket
	if err := rl.Acquire(ctx); err != nil {
		t.Fatal(err)
	}

	status := rl.Status(ctx)
	minuteStatus := status["per_minute"].(map[string]int)
	if minuteStatus["remaining"] != testPerMinute-1 {
		t.Errorf("expected fresh bucket %d, got %d", testPerMinute-1, minuteStatus["remaining"])
	}
	t.Logf("✅ window expiry creates fresh bucket: remaining=%d", minuteStatus["remaining"])
}
