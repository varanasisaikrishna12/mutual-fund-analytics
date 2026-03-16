package ratelimiter

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const luaScript = `
local second_key = KEYS[1]
local minute_key = KEYS[2]
local hour_key   = KEYS[3]

local per_second = tonumber(ARGV[1])
local per_minute = tonumber(ARGV[2])
local per_hour   = tonumber(ARGV[3])

-- Read current counts
-- nil means key expired or first request = fresh window = full tokens
local s = tonumber(redis.call('GET', second_key))
local m = tonumber(redis.call('GET', minute_key))
local h = tonumber(redis.call('GET', hour_key))

-- If key absent, initialize fresh window with full tokens
if s == nil then
    redis.call('SET', second_key, per_second, 'EX', 1)
    s = per_second
end
if m == nil then
    redis.call('SET', minute_key, per_minute, 'EX', 60)
    m = per_minute
end
if h == nil then
    redis.call('SET', hour_key, per_hour, 'EX', 3600)
    h = per_hour
end

-- Check all three
if s <= 0 or m <= 0 or h <= 0 then
    return {0, s, m, h}  -- denied
end

-- Decrement all three — DECRBY preserves TTL automatically
redis.call('DECRBY', second_key, 1)
redis.call('DECRBY', minute_key, 1)
redis.call('DECRBY', hour_key,   1)

return {1, s - 1, m - 1, h - 1}  -- allowed
`

type RateLimiter struct {
	client    *redis.Client
	script    *redis.Script
	perSecond int
	perMinute int
	perHour   int
	logger    *zap.Logger
}

type Config struct {
	PerSecond int
	PerMinute int
	PerHour   int
}

func New(client *redis.Client, cfg Config, logger *zap.Logger) *RateLimiter {
	return &RateLimiter{
		client:    client,
		script:    redis.NewScript(luaScript),
		perSecond: cfg.PerSecond,
		perMinute: cfg.PerMinute,
		perHour:   cfg.PerHour,
		logger:    logger,
	}
}

func (rl *RateLimiter) Acquire(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		allowed, waitDur, err := rl.tryAcquire(ctx)
		if err != nil {
			return fmt.Errorf("rate limiter error: %w", err)
		}
		if allowed {
			return nil
		}

		rl.logger.Info("rate limit hit, waiting", zap.Duration("wait", waitDur))

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(waitDur):
		}
	}
}

func (rl *RateLimiter) tryAcquire(ctx context.Context) (bool, time.Duration, error) {
	keys := []string{
		"ratelimit:per_second",
		"ratelimit:per_minute",
		"ratelimit:per_hour",
	}
	args := []interface{}{rl.perSecond, rl.perMinute, rl.perHour}

	result, err := rl.script.Run(ctx, rl.client, keys, args...).Int64Slice()
	if err != nil {
		return false, 0, fmt.Errorf("lua script error: %w", err)
	}

	allowed := result[0] == 1
	remainingSecond := result[1]
	remainingMinute := result[2]
	remainingHour := result[3]

	rl.logger.Info("rate limit check",
		zap.Bool("allowed", allowed),
		zap.Int64("remaining_second", remainingSecond),
		zap.Int64("remaining_minute", remainingMinute),
		zap.Int64("remaining_hour", remainingHour),
	)

	if allowed {
		return true, 0, nil
	}
	return false, rl.waitDuration(remainingSecond, remainingMinute, remainingHour), nil
}

func (rl *RateLimiter) waitDuration(s, m, h int64) time.Duration {
	if s <= 0 {
		return 500 * time.Millisecond
	}
	if m <= 0 {
		return 5 * time.Second
	}
	if h <= 0 {
		return 5 * time.Minute
	}
	return 500 * time.Millisecond
}

func (rl *RateLimiter) Status(ctx context.Context) map[string]interface{} {
	pipe := rl.client.Pipeline()
	sCmd := pipe.Get(ctx, "ratelimit:per_second")
	mCmd := pipe.Get(ctx, "ratelimit:per_minute")
	hCmd := pipe.Get(ctx, "ratelimit:per_hour")
	pipe.Exec(ctx)

	getVal := func(cmd *redis.StringCmd, max int) int {
		val, err := cmd.Int()
		if err != nil {
			return max
		}
		return val
	}

	return map[string]interface{}{
		"per_second": map[string]int{"remaining": getVal(sCmd, rl.perSecond), "limit": rl.perSecond},
		"per_minute": map[string]int{"remaining": getVal(mCmd, rl.perMinute), "limit": rl.perMinute},
		"per_hour":   map[string]int{"remaining": getVal(hCmd, rl.perHour), "limit": rl.perHour},
	}
}
