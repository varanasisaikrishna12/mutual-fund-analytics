package fetcher

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/varanasisaikrishna12/mutual-fund-analytics/internal/ratelimiter"
	"go.uber.org/zap"
)

type FundInfo struct {
	SchemeCode    string
	FundName      string
	AMC           string
	Category      string
	InceptionDate *time.Time
}

type NAVPoint struct {
	SchemeCode string
	Date       time.Time
	NAV        float64
}

type Fetcher struct {
	rl     *ratelimiter.RateLimiter
	client *http.Client
	logger *zap.Logger
}

func New(rl *ratelimiter.RateLimiter, logger *zap.Logger) *Fetcher {
	return &Fetcher{
		rl:     rl,
		client: &http.Client{Timeout: 30 * time.Second},
		logger: logger,
	}
}

type mfAPIResponse struct {
	Meta struct {
		FundHouse      string `json:"fund_house"`
		SchemeType     string `json:"scheme_type"`
		SchemeCategory string `json:"scheme_category"`
		SchemeCode     int    `json:"scheme_code"`
		SchemeName     string `json:"scheme_name"`
	} `json:"meta"`
	Data []struct {
		Date string `json:"date"`
		NAV  string `json:"nav"`
	} `json:"data"`
	Status string `json:"status"`
}

func (f *Fetcher) FetchScheme(ctx context.Context, schemeCode string) (*FundInfo, []NAVPoint, error) {
	if err := f.rl.Acquire(ctx); err != nil {
		return nil, nil, fmt.Errorf("rate limiter: %w", err)
	}

	url := fmt.Sprintf("https://api.mfapi.in/mf/%s", schemeCode)
	f.logger.Info("fetching scheme", zap.String("code", schemeCode), zap.String("url", url))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := f.client.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("http get: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	var apiResp mfAPIResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return nil, nil, fmt.Errorf("decode response: %w", err)
	}

	var points []NAVPoint
	for _, d := range apiResp.Data {
		date, err := time.Parse("02-01-2006", d.Date)
		if err != nil {
			continue
		}
		nav, err := strconv.ParseFloat(d.NAV, 64)
		if err != nil || nav <= 0 {
			continue
		}
		points = append(points, NAVPoint{
			SchemeCode: schemeCode,
			Date:       date,
			NAV:        nav,
		})
	}

	// Reverse to chronological order (oldest first)
	for i, j := 0, len(points)-1; i < j; i, j = i+1, j-1 {
		points[i], points[j] = points[j], points[i]
	}

	f.logger.Info("fetched scheme",
		zap.String("code", schemeCode),
		zap.String("name", apiResp.Meta.SchemeName),
		zap.Int("nav_points", len(points)),
	)

	var inceptionDate *time.Time
	if len(points) > 0 {
		t := points[0].Date
		inceptionDate = &t
	}

	fund := &FundInfo{
		SchemeCode:    schemeCode,
		FundName:      apiResp.Meta.SchemeName,
		AMC:           apiResp.Meta.FundHouse,
		Category:      apiResp.Meta.SchemeCategory,
		InceptionDate: inceptionDate,
	}

	return fund, points, nil
}
