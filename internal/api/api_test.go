package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/varanasisaikrishna12/mutual-fund-analytics/internal/analytics"
	redisstore "github.com/varanasisaikrishna12/mutual-fund-analytics/internal/store/redis"
	"github.com/varanasisaikrishna12/mutual-fund-analytics/internal/store/timescale"
)

const baseURL = "http://localhost:8080"

// hardcoded for tests — avoids .env path issues
const (
	testDBURL     = "postgres://mfuser:mfpassword@localhost:5432/mutualfunds"
	testRedisAddr = "localhost:6379"
	testRedisPass = ""
)

func httpGet(t *testing.T, url string) (int, map[string]interface{}, time.Duration) {
	t.Helper()
	start := time.Now()
	resp, err := http.Get(url)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("GET %s failed: %v", url, err)
	}
	defer resp.Body.Close()
	var body map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&body)
	return resp.StatusCode, body, elapsed
}

func assertFast(t *testing.T, elapsed time.Duration, endpoint string) {
	t.Helper()
	if elapsed > 200*time.Millisecond {
		t.Errorf("❌ %s too slow: %v (must be <200ms)", endpoint, elapsed)
	} else {
		t.Logf("✅ %s responded in %v", endpoint, elapsed)
	}
}

// Test 1: Health endpoint
func TestHealthEndpoint(t *testing.T) {
	status, body, elapsed := httpGet(t, baseURL+"/health")
	if status != http.StatusOK {
		t.Errorf("expected 200, got %d", status)
	}
	if body["status"] != "ok" {
		t.Errorf("expected status=ok, got %v", body["status"])
	}
	assertFast(t, elapsed, "GET /health")
}

// Test 2: GET /funds
func TestGetFunds(t *testing.T) {
	status, body, elapsed := httpGet(t, baseURL+"/funds")
	if status != http.StatusOK {
		t.Errorf("expected 200, got %d", status)
	}
	funds, ok := body["funds"].([]interface{})
	if !ok || len(funds) == 0 {
		t.Fatal("expected funds in response")
	}
	t.Logf("✅ GET /funds returned %d funds", len(funds))
	assertFast(t, elapsed, "GET /funds")
}

// Test 3: GET /funds?category= filter
func TestGetFundsWithCategoryFilter(t *testing.T) {
	_, body, _ := httpGet(t, baseURL+"/funds")
	funds, ok := body["funds"].([]interface{})
	if !ok || len(funds) == 0 {
		t.Skip("no funds available")
	}

	firstFund := funds[0].(map[string]interface{})
	category, ok := firstFund["Category"].(string)
	if !ok || category == "" {
		t.Skip("fund has no category")
	}

	status, filteredBody, elapsed := httpGet(t,
		baseURL+"/funds?category="+url.QueryEscape(category),
	)

	if status != http.StatusOK {
		t.Errorf("expected 200, got %d", status)
		return
	}

	filtered, ok := filteredBody["funds"].([]interface{})
	if !ok || len(filtered) == 0 {
		t.Errorf("expected funds for category %s", category)
		return
	}

	for _, f := range filtered {
		fund := f.(map[string]interface{})
		if fund["Category"] != category {
			t.Errorf("expected category=%s, got %v", category, fund["Category"])
		}
	}

	t.Logf("✅ GET /funds?category=%s returned %d funds", category, len(filtered))
	// DB query — 500ms threshold acceptable
	if elapsed > 500*time.Millisecond {
		t.Errorf("❌ too slow: %v", elapsed)
	} else {
		t.Logf("✅ responded in %v", elapsed)
	}
}

// Test 4: GET /funds/:code
func TestGetSingleFund(t *testing.T) {
	code := "120381"
	status, body, elapsed := httpGet(t, baseURL+"/funds/"+code)
	if status != http.StatusOK {
		t.Errorf("expected 200, got %d", status)
	}
	if body["SchemeCode"] != code {
		t.Errorf("expected SchemeCode=%s, got %v", code, body["SchemeCode"])
	}
	t.Logf("✅ GET /funds/%s: %v", code, body["FundName"])
	assertFast(t, elapsed, "GET /funds/"+code)
}

// Test 5: GET /funds/:code not found
func TestGetSingleFundNotFound(t *testing.T) {
	status, _, elapsed := httpGet(t, baseURL+"/funds/999999")
	if status != http.StatusNotFound {
		t.Errorf("expected 404, got %d", status)
	}
	t.Logf("✅ GET /funds/999999 correctly returns 404")
	assertFast(t, elapsed, "GET /funds/999999")
}

// Test 6: GET /funds/:code/analytics — all windows
func TestGetAnalyticsAllWindows(t *testing.T) {
	code := "120381"
	windows := []string{"1Y", "3Y", "5Y", "10Y"}

	for _, window := range windows {
		url := fmt.Sprintf("%s/funds/%s/analytics?window=%s", baseURL, code, window)
		status, body, elapsed := httpGet(t, url)

		if status != http.StatusOK {
			t.Errorf("window %s: expected 200, got %d", window, status)
			continue
		}

		for _, field := range []string{"fund_code", "rolling_returns", "cagr", "max_drawdown"} {
			if body[field] == nil {
				t.Errorf("window %s: missing %s", window, field)
			}
		}

		t.Logf("✅ window=%s max_drawdown=%.2f", window, body["max_drawdown"])
		assertFast(t, elapsed, fmt.Sprintf("GET /funds/%s/analytics?window=%s", code, window))
	}
}

// Test 7: GET /funds/:code/analytics — missing window
func TestGetAnalyticsMissingWindow(t *testing.T) {
	status, body, _ := httpGet(t, baseURL+"/funds/120381/analytics")
	if status != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", status)
	}
	t.Logf("✅ missing window returns 400: %v", body["error"])
}

// Test 8: GET /funds/rank
func TestGetRanking(t *testing.T) {
	_, body, _ := httpGet(t, baseURL+"/funds")
	funds, ok := body["funds"].([]interface{})
	if !ok || len(funds) == 0 {
		t.Skip("no funds available")
	}

	firstFund := funds[0].(map[string]interface{})
	category, ok := firstFund["Category"].(string)
	if !ok || category == "" {
		t.Skip("fund has no category")
	}

	rankURL := fmt.Sprintf("%s/funds/rank?category=%s&window=3Y&limit=5",
		baseURL, url.QueryEscape(category))

	status, rankBody, elapsed := httpGet(t, rankURL)
	if status != http.StatusOK {
		t.Errorf("expected 200, got %d — body: %v", status, rankBody)
		return
	}

	rankings, ok := rankBody["rankings"].([]interface{})
	if !ok || len(rankings) == 0 {
		t.Error("expected at least 1 ranking entry")
		return
	}

	// Verify fields
	first := rankings[0].(map[string]interface{})
	for _, field := range []string{"rank", "scheme_code", "cagr_median", "fund_name"} {
		if first[field] == nil {
			t.Errorf("ranking entry missing %s", field)
		}
	}

	// Verify descending order
	if len(rankings) >= 2 {
		r1 := rankings[0].(map[string]interface{})["cagr_median"].(float64)
		r2 := rankings[1].(map[string]interface{})["cagr_median"].(float64)
		if r1 < r2 {
			t.Error("rankings not sorted by CAGR descending")
		}
	}

	t.Logf("✅ rankings for category=%s window=3Y:", category)
	for i, r := range rankings {
		entry := r.(map[string]interface{})
		t.Logf("  #%d %s — CAGR: %.2f%%", i+1, entry["fund_name"], entry["cagr_median"])
	}
	assertFast(t, elapsed, "GET /funds/rank")
}

// Test 9: GET /funds/rank — missing params
func TestGetRankingMissingParams(t *testing.T) {
	status, _, _ := httpGet(t, baseURL+"/funds/rank?window=3Y")
	if status != http.StatusBadRequest {
		t.Errorf("expected 400 for missing category, got %d", status)
	}
	t.Logf("✅ missing category returns 400")

	status, _, _ = httpGet(t, baseURL+"/funds/rank?category="+url.QueryEscape("Mid Cap"))
	if status != http.StatusBadRequest {
		t.Errorf("expected 400 for missing window, got %d", status)
	}
	t.Logf("✅ missing window returns 400")
}

// Test 10: Redis cache hit
func TestRedisCacheHit(t *testing.T) {
	url := fmt.Sprintf("%s/funds/120381/analytics?window=3Y", baseURL)

	_, _, elapsed1 := httpGet(t, url)
	_, _, elapsed2 := httpGet(t, url)

	t.Logf("first:  %v", elapsed1)
	t.Logf("second: %v", elapsed2)

	assertFast(t, elapsed1, "first request")
	assertFast(t, elapsed2, "second request (Redis cache)")
}

// Test 11: DB and Redis consistency
func TestDBRedisConsistency(t *testing.T) {
	ctx := context.Background()

	tsStore, err := timescale.New(ctx, testDBURL)
	if err != nil {
		t.Fatalf("timescaledb connect failed: %v", err)
	}
	defer tsStore.Close()

	rdStore, err := redisstore.New(ctx, testRedisAddr, testRedisPass)
	if err != nil {
		t.Fatalf("redis connect failed: %v", err)
	}
	defer rdStore.Close()

	code := "120381"
	window := "3Y"

	dbResult, err := tsStore.GetAnalyticsFromDB(ctx, code, window)
	if err != nil {
		t.Fatalf("DB get failed: %v", err)
	}

	redisResult, err := rdStore.GetAnalytics(ctx, code, window)
	if err != nil {
		t.Fatalf("Redis get failed: %v", err)
	}
	if redisResult == nil {
		t.Skip("Redis cache not populated — trigger sync first")
	}

	if dbResult.CAGR.Median != redisResult.CAGR.Median {
		t.Errorf("CAGR median mismatch: DB=%.2f Redis=%.2f",
			dbResult.CAGR.Median, redisResult.CAGR.Median)
	}
	if dbResult.MaxDrawdown != redisResult.MaxDrawdown {
		t.Errorf("MaxDrawdown mismatch: DB=%.2f Redis=%.2f",
			dbResult.MaxDrawdown, redisResult.MaxDrawdown)
	}

	t.Logf("✅ DB and Redis consistent:")
	t.Logf("   CAGR median=%.2f%%", dbResult.CAGR.Median)
	t.Logf("   max drawdown=%.2f%%", dbResult.MaxDrawdown)
	t.Logf("   rolling periods=%d", dbResult.RollingPeriodsCount)
}

// Test 12: Analytics response structure validation
func TestAnalyticsResponseStructure(t *testing.T) {
	url := fmt.Sprintf("%s/funds/120381/analytics?window=3Y", baseURL)
	_, body, _ := httpGet(t, url)

	// Validate rolling_returns has all required fields
	rr, ok := body["rolling_returns"].(map[string]interface{})
	if !ok {
		t.Fatal("rolling_returns is not an object")
	}
	for _, field := range []string{"min", "max", "median", "p25", "p75"} {
		if rr[field] == nil {
			t.Errorf("rolling_returns missing field: %s", field)
		}
	}
	t.Logf("✅ rolling_returns: min=%.2f max=%.2f median=%.2f p25=%.2f p75=%.2f",
		rr["min"], rr["max"], rr["median"], rr["p25"], rr["p75"])

	// Validate cagr has min/max/median
	cagr, ok := body["cagr"].(map[string]interface{})
	if !ok {
		t.Fatal("cagr is not an object")
	}
	for _, field := range []string{"min", "max", "median"} {
		if cagr[field] == nil {
			t.Errorf("cagr missing field: %s", field)
		}
	}
	t.Logf("✅ cagr: min=%.2f max=%.2f median=%.2f",
		cagr["min"], cagr["max"], cagr["median"])

	// Validate max_drawdown is negative
	drawdown := body["max_drawdown"].(float64)
	if drawdown > 0 {
		t.Errorf("max_drawdown should be negative, got %.2f", drawdown)
	}
	t.Logf("✅ max_drawdown=%.2f (correctly negative)", drawdown)
}

// Test 13: Verify analytics values are sane
func TestAnalyticsValuesSanity(t *testing.T) {
	ctx := context.Background()

	tsStore, err := timescale.New(ctx, testDBURL)
	if err != nil {
		t.Fatalf("timescaledb connect failed: %v", err)
	}
	defer tsStore.Close()

	codes := []string{"120381", "118989", "119716"}
	windows := []string{"1Y", "3Y", "5Y", "10Y"}

	for _, code := range codes {
		for _, window := range windows {
			result, err := tsStore.GetAnalyticsFromDB(ctx, code, window)
			if err != nil {
				t.Logf("skipping %s %s: %v", code, window, err)
				continue
			}

			// Min <= Median <= Max for rolling returns
			rr := result.RollingReturns
			if rr.Min > rr.Median {
				t.Errorf("%s %s: rolling_returns min > median", code, window)
			}
			if rr.Median > rr.Max {
				t.Errorf("%s %s: rolling_returns median > max", code, window)
			}

			// P25 <= Median <= P75
			if rr.P25 > rr.Median {
				t.Errorf("%s %s: p25 > median", code, window)
			}
			if rr.Median > rr.P75 {
				t.Errorf("%s %s: median > p75", code, window)
			}

			// Max drawdown should be negative or zero
			if result.MaxDrawdown > 0 {
				t.Errorf("%s %s: max drawdown positive: %.2f", code, window, result.MaxDrawdown)
			}

			// Rolling periods should be > 0
			if result.RollingPeriodsCount == 0 {
				t.Errorf("%s %s: 0 rolling periods", code, window)
			}

			t.Logf("✅ %s %s: periods=%d cagr_median=%.2f%% drawdown=%.2f%%",
				code, window,
				result.RollingPeriodsCount,
				result.CAGR.Median,
				result.MaxDrawdown,
			)

			// Also verify unused import
			_ = analytics.WindowDays
		}
	}
}
