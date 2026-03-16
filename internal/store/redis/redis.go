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
func (s *Store) SetAnalytics(ctx context.Context, result *analytics.AnalyticsResult) error {
	data, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal analytics: %w", err)
	}
	key := fmt.Sprintf("fund:%s:analytics:%s", result.SchemeCode, result.Window)
	return s.Client.Set(ctx, key, data, 25*time.Hour).Err()
}

// GetAnalytics reads analytics result from Redis
func (s *Store) GetAnalytics(ctx context.Context, schemeCode, window string) (*analytics.AnalyticsResult, error) {
	key := fmt.Sprintf("fund:%s:analytics:%s", schemeCode, window)
	data, err := s.Client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, nil
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

// RankEntry is a single entry in a ranking list
type RankEntry struct {
	Rank         int     `json:"rank"`
	FundCode     string  `json:"fund_code"`
	FundName     string  `json:"fund_name"`
	AMC          string  `json:"amc"`
	MedianReturn float64 `json:"median_return"`
	MaxDrawdown  float64 `json:"max_drawdown"`
	CurrentNAV   float64 `json:"current_nav"`
	LastUpdated  string  `json:"last_updated"`
}

// RankingResult is the full ranking response stored in Redis
type RankingResult struct {
	Category   string      `json:"category"`
	Window     string      `json:"window"`
	SortBy     string      `json:"sorted_by"`
	TotalFunds int         `json:"total_funds"`
	ComputedAt time.Time   `json:"computed_at"`
	Funds      []RankEntry `json:"funds"`
}

// SetRanking stores a full ranking result in Redis with 25hr TTL
// key: rank:{category}:{window}:{sort_by}
func (s *Store) SetRanking(ctx context.Context, result *RankingResult) error {
	data, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal ranking: %w", err)
	}
	key := rankingKey(result.Category, result.Window, result.SortBy)
	return s.Client.Set(ctx, key, data, 25*time.Hour).Err()
}

// GetRanking reads a ranking result from Redis
// Returns nil, nil on cache miss
func (s *Store) GetRanking(ctx context.Context, category, window, sortBy string) (*RankingResult, error) {
	key := rankingKey(category, window, sortBy)
	data, err := s.Client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("redis get ranking: %w", err)
	}
	var result RankingResult
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// rankingKey builds the Redis key for a ranking
func rankingKey(category, window, sortBy string) string {
	return fmt.Sprintf("rank:%s:%s:%s",
		normalizeCategory(category),
		window,
		sortBy,
	)
}

// normalizeCategory lowercases and replaces spaces with underscores
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
