package analytics

import (
	"math"
	"testing"
	"time"
)

// buildNAVSeries creates a NAV series with consistent annual growth
// annualReturn: e.g. 0.15 = 15% per year
// tradingDays: number of trading days to generate
func buildNAVSeries(startNAV float64, annualReturn float64, tradingDays int) []NAVPoint {
	points := make([]NAVPoint, tradingDays)
	dailyReturn := math.Pow(1+annualReturn, 1.0/252) - 1

	nav := startNAV
	date := time.Date(2013, 1, 1, 0, 0, 0, 0, time.UTC)

	for i := 0; i < tradingDays; i++ {
		points[i] = NAVPoint{Date: date, Value: nav}
		nav = nav * (1 + dailyReturn)
		date = date.AddDate(0, 0, 1)
		for date.Weekday() == time.Saturday || date.Weekday() == time.Sunday {
			date = date.AddDate(0, 0, 1)
		}
	}
	return points
}

// Test 1: Simple rolling return calculation
// NAV doubles in 1 year → simple return = 100%
func TestSimpleRollingReturn(t *testing.T) {
	// Use many points so binary search always finds a match
	// 15% annual return over 2 years → enough for 1Y window
	points := buildNAVSeries(100.0, 0.15, 600)

	result, err := Compute("TEST001", points, "1Y")
	if err != nil {
		t.Fatalf("Compute failed: %v", err)
	}

	if result.RollingPeriodsCount == 0 {
		t.Fatal("expected rolling periods, got 0")
	}

	// With 15% annual return, median simple return for 1Y should be ~15%
	if math.Abs(result.RollingReturns.Median-15.0) > 3.0 {
		t.Errorf("expected simple return median ~15%%, got %.2f%%",
			result.RollingReturns.Median)
	}
	t.Logf("✅ simple rolling return median: %.2f%%", result.RollingReturns.Median)
}

// Test 2: CAGR accounts for time
// 1Y simple return and CAGR should be similar
// 3Y simple return and CAGR should be very different
func TestCAGRAccountsForTime(t *testing.T) {
	// 15% annual return over 5 years
	points := buildNAVSeries(100.0, 0.15, 1400)

	result1Y, err := Compute("TEST001", points, "1Y")
	if err != nil {
		t.Fatalf("1Y window failed: %v", err)
	}

	result3Y, err := Compute("TEST001", points, "3Y")
	if err != nil {
		t.Fatalf("3Y window failed: %v", err)
	}

	// For 1Y: simple return ≈ CAGR (both ~15%)
	diff1Y := math.Abs(result1Y.RollingReturns.Median - result1Y.CAGR.Median)
	if diff1Y > 2.0 {
		t.Errorf("1Y: simple return and CAGR should be close, diff=%.2f%%", diff1Y)
	}
	t.Logf("✅ 1Y: simple=%.2f%% cagr=%.2f%% diff=%.2f%%",
		result1Y.RollingReturns.Median, result1Y.CAGR.Median, diff1Y)

	// For 3Y: simple return >> CAGR
	// simple return ~52% (1.15^3 - 1), CAGR ~15%
	if result3Y.RollingReturns.Median <= result3Y.CAGR.Median {
		t.Errorf("3Y: simple return should be > CAGR, got simple=%.2f%% cagr=%.2f%%",
			result3Y.RollingReturns.Median, result3Y.CAGR.Median)
	}
	t.Logf("✅ 3Y: simple=%.2f%% cagr=%.2f%% (correctly different)",
		result3Y.RollingReturns.Median, result3Y.CAGR.Median)
}

// Test 3: Simple return vs CAGR values are correct
func TestSimpleReturnVsCAGR(t *testing.T) {
	// 5 year constant growth: 15% per year
	points := buildNAVSeries(100.0, 0.15, 1400)

	result, err := Compute("TEST001", points, "5Y")
	if err != nil {
		t.Fatalf("Compute failed: %v", err)
	}

	// 5Y at 15%/year:
	// simple return = (1.15^5 - 1) * 100 ≈ 101%
	// CAGR = ~15%
	if math.Abs(result.RollingReturns.Median-101.0) > 5.0 {
		t.Errorf("expected 5Y simple return ~101%%, got %.2f%%",
			result.RollingReturns.Median)
	}
	t.Logf("✅ 5Y simple return: %.2f%% (expected ~101%%)",
		result.RollingReturns.Median)

	if math.Abs(result.CAGR.Median-15.0) > 2.0 {
		t.Errorf("expected 5Y CAGR ~15%%, got %.2f%%",
			result.CAGR.Median)
	}
	t.Logf("✅ 5Y CAGR: %.2f%% (expected ~15%%)",
		result.CAGR.Median)
}

// Test 4: Max drawdown calculation
// NAV: 100 → 150 → 90 → 120
// Peak=150, trough=90 → drawdown = -40%
func TestMaxDrawdown(t *testing.T) {
	baseDate := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	points := []NAVPoint{
		{Date: baseDate.AddDate(0, 0, 0), Value: 100.0},
		{Date: baseDate.AddDate(0, 0, 30), Value: 150.0},
		{Date: baseDate.AddDate(0, 0, 60), Value: 90.0},
		{Date: baseDate.AddDate(0, 0, 90), Value: 120.0},
	}

	drawdown := computeMaxDrawdown(points)

	// (90-150)/150 * 100 = -40%
	if math.Abs(drawdown-(-40.0)) > 0.1 {
		t.Errorf("expected drawdown=-40%%, got %.2f%%", drawdown)
	}
	t.Logf("✅ max drawdown: %.2f%%", drawdown)
}

// Test 5: Percentile calculation
func TestPercentile(t *testing.T) {
	values := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	// p50 of 1-10 = 5.5
	p50 := percentile(values, 50)
	if math.Abs(p50-5.5) > 0.01 {
		t.Errorf("expected p50=5.5, got %.2f", p50)
	}

	// p25 of 1-10 = 3.25
	p25 := percentile(values, 25)
	if math.Abs(p25-3.25) > 0.01 {
		t.Errorf("expected p25=3.25, got %.2f", p25)
	}

	// p75 of 1-10 = 7.75
	p75 := percentile(values, 75)
	if math.Abs(p75-7.75) > 0.01 {
		t.Errorf("expected p75=7.75, got %.2f", p75)
	}

	t.Logf("✅ percentiles: p25=%.2f p50=%.2f p75=%.2f", p25, p50, p75)
}

// Test 6: Insufficient history returns error
// 3 months of data for 1Y window — target start is 9 months before
// any available data → outside 30-day tolerance → no periods → error
func TestInsufficientHistory(t *testing.T) {
	baseDate := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	points := []NAVPoint{
		{Date: baseDate, Value: 100.0},
		{Date: baseDate.AddDate(0, 1, 0), Value: 103.0},
		{Date: baseDate.AddDate(0, 2, 0), Value: 106.0},
		{Date: baseDate.AddDate(0, 3, 0), Value: 110.0},
	}

	_, err := Compute("TEST001", points, "1Y")
	if err == nil {
		t.Error("expected error for insufficient history (3 months for 1Y window)")
	} else {
		t.Logf("✅ correctly returned error: %v", err)
	}
}

// Test 7: All 4 windows compute correctly with 12+ years of data
func TestAllWindows(t *testing.T) {
	// 12 years of data at 15% annual return
	points := buildNAVSeries(100.0, 0.15, 3000)

	windows := []string{"1Y", "3Y", "5Y", "10Y"}

	for _, w := range windows {
		result, err := Compute("TEST001", points, w)
		if err != nil {
			t.Fatalf("window %s failed: %v", w, err)
		}

		if result.RollingPeriodsCount == 0 {
			t.Errorf("window %s: 0 rolling periods", w)
		}

		// Min should always be <= Max
		if result.RollingReturns.Min > result.RollingReturns.Max {
			t.Errorf("window %s: rolling return min > max", w)
		}
		if result.CAGR.Min > result.CAGR.Max {
			t.Errorf("window %s: CAGR min > max", w)
		}

		// For consistent growth, max drawdown should be 0
		if result.MaxDrawdown > 0 {
			t.Errorf("window %s: drawdown should be <=0, got %.2f", w, result.MaxDrawdown)
		}

		// CAGR median should be ~15% for all windows
		if math.Abs(result.CAGR.Median-15.0) > 2.0 {
			t.Errorf("window %s: expected CAGR median ~15%%, got %.2f%%",
				w, result.CAGR.Median)
		}

		t.Logf("✅ window %s: periods=%d simple_median=%.2f%% cagr_median=%.2f%% drawdown=%.2f%%",
			w,
			result.RollingPeriodsCount,
			result.RollingReturns.Median,
			result.CAGR.Median,
			result.MaxDrawdown,
		)
	}
}

// Test 8: Stats distribution correctness
func TestStatsDistribution(t *testing.T) {
	values := []float64{5, 10, 15, 20, 25, 30, 35, 40, 45, 50}
	stats := computeRollingStats(values)

	if stats.Min != 5 {
		t.Errorf("expected min=5, got %.2f", stats.Min)
	}
	if stats.Max != 50 {
		t.Errorf("expected max=50, got %.2f", stats.Max)
	}
	// Median of 10 values = (5th+6th)/2 = (25+30)/2 = 27.5
	if math.Abs(stats.Median-27.5) > 0.1 {
		t.Errorf("expected median=27.5, got %.2f", stats.Median)
	}

	t.Logf("✅ stats: min=%.2f max=%.2f median=%.2f p25=%.2f p75=%.2f",
		stats.Min, stats.Max, stats.Median, stats.P25, stats.P75)
}
