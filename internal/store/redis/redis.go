package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/varanasisaikrishna12/mutual-fund-analytics/internal/analytics"
)

type Store struct {
	Client *redis.Client
}

func New(ctx context.Context, addr, password string) (*Store, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
	})
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("connect redis: %w", err)
	}
	return &Store{Client: client}, nil
}

func (s *Store) Close() error {
	return s.Client.Close()
}

// SetAnalytics stores analytics result in Redis with 25hr TTL
// Key: fund:{schemeCode}:analytics:{window}
func (s *Store) SetAnalytics(ctx context.Context, result *analytics.AnalyticsResult) error {
	data, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal analytics: %w", err)
	}

	key := fmt.Sprintf("fund:%s:analytics:%s", result.SchemeCode, result.Window)
	return s.Client.Set(ctx, key, data, 25*time.Hour).Err()
}

// GetAnalytics reads analytics result from Redis
// Returns nil, nil if key does not exist (cache miss)
func (s *Store) GetAnalytics(ctx context.Context, schemeCode, window string) (*analytics.AnalyticsResult, error) {
	key := fmt.Sprintf("fund:%s:analytics:%s", schemeCode, window)
	data, err := s.Client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, nil // cache miss
	}
	if err != nil {
		return nil, fmt.Errorf("redis get analytics: %w", err)
	}

	var result analytics.AnalyticsResult
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// UpdateRanking updates the sorted set for fund ranking
// Key: rank:{category}:{window}
// Score: CAGR median (higher = better rank)
func (s *Store) UpdateRanking(ctx context.Context, category, window, schemeCode string, cagrMedian float64) error {
	key := fmt.Sprintf("rank:%s:%s",
		normalizeCategory(category),
		window,
	)
	return s.Client.ZAdd(ctx, key, redis.Z{
		Score:  cagrMedian,
		Member: schemeCode,
	}).Err()
}

// GetRanking returns top N funds for a category+window sorted by CAGR median
func (s *Store) GetRanking(ctx context.Context, category, window string, limit int) ([]redis.Z, error) {
	key := fmt.Sprintf("rank:%s:%s",
		normalizeCategory(category),
		window,
	)
	// ZRevRangeWithScores → highest score first
	return s.Client.ZRevRangeWithScores(ctx, key, 0, int64(limit-1)).Result()
}

// normalizeCategory lowercases and replaces spaces with underscores
// "Mid Cap" → "mid_cap"
func normalizeCategory(category string) string {
	result := ""
	for _, c := range category {
		if c == ' ' {
			result += "_"
		} else if c >= 'A' && c <= 'Z' {
			result += string(c + 32)
		} else {
			result += string(c)
		}
	}
	return result
}
