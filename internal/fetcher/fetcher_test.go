package fetcher

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/varanasisaikrishna12/mutual-fund-analytics/internal/ratelimiter"
	"go.uber.org/zap"
)

func newTestFetcher(t *testing.T) *Fetcher {
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
	rl := ratelimiter.New(client, ratelimiter.Config{
		PerSecond: 2,
		PerMinute: 50,
		PerHour:   300,
	}, logger)

	return New(rl, logger)
}

// Test: fetch real scheme from mfapi.in
func TestFetchScheme(t *testing.T) {
	f := newTestFetcher(t)
	ctx := context.Background()

	fund, navPoints, err := f.FetchScheme(ctx, "119598")
	if err != nil {
		t.Fatalf("FetchScheme failed: %v", err)
	}

	if fund.SchemeCode != "119598" {
		t.Errorf("expected scheme code 119598, got %s", fund.SchemeCode)
	}
	if fund.FundName == "" {
		t.Error("fund name should not be empty")
	}
	if fund.AMC == "" {
		t.Error("AMC should not be empty")
	}

	t.Logf("✅ Fund: %s", fund.FundName)
	t.Logf("✅ AMC: %s", fund.AMC)
	t.Logf("✅ Category: %s", fund.Category)

	if len(navPoints) == 0 {
		t.Fatal("should have NAV data points")
	}
	t.Logf("✅ NAV points: %d", len(navPoints))

	// Verify chronological order
	for i := 1; i < len(navPoints); i++ {
		if navPoints[i].Date.Before(navPoints[i-1].Date) {
			t.Error("NAV points should be in chronological order")
			break
		}
	}
	t.Logf("✅ First NAV date: %s", navPoints[0].Date.Format("2006-01-02"))
	t.Logf("✅ Last NAV date: %s", navPoints[len(navPoints)-1].Date.Format("2006-01-02"))

	// Verify inception date is set
	if fund.InceptionDate == nil {
		t.Error("inception date should not be nil")
	} else {
		t.Logf("✅ Inception date: %s", fund.InceptionDate.Format("2006-01-02"))
		// Inception date should match first NAV point
		if !fund.InceptionDate.Equal(navPoints[0].Date) {
			t.Errorf("inception date %s should match first NAV point %s",
				fund.InceptionDate.Format("2006-01-02"),
				navPoints[0].Date.Format("2006-01-02"))
		}
	}

	// Verify all NAV values are positive
	for _, p := range navPoints {
		if p.NAV <= 0 {
			t.Errorf("invalid NAV value %f on %s", p.NAV, p.Date)
		}
	}
}

// Test: date parsing and chronological ordering
func TestDateParsingAndOrdering(t *testing.T) {
	f := newTestFetcher(t)
	ctx := context.Background()

	fund, navPoints, err := f.FetchScheme(ctx, "119598")
	if err != nil {
		t.Fatalf("FetchScheme failed: %v", err)
	}

	if len(navPoints) < 2 {
		t.Fatal("need at least 2 points to test ordering")
	}

	// First point should be oldest
	first := navPoints[0]
	last := navPoints[len(navPoints)-1]

	if !first.Date.Before(last.Date) {
		t.Errorf("first date %s should be before last date %s",
			first.Date.Format("2006-01-02"),
			last.Date.Format("2006-01-02"))
	}

	// Dates should be valid (not zero)
	if first.Date.Equal(time.Time{}) {
		t.Error("first date should not be zero")
	}

	// Inception date = first NAV point
	if fund.InceptionDate == nil || !fund.InceptionDate.Equal(first.Date) {
		t.Errorf("inception date should be %s", first.Date.Format("2006-01-02"))
	}

	t.Logf("✅ date parsing correct")
	t.Logf("✅ chronological order correct")
	t.Logf("✅ inception date = first NAV point: %s", first.Date.Format("2006-01-02"))
}
