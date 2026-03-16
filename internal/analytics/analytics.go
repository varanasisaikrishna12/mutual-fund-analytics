package analytics

import (
	"fmt"
	"math"
	"sort"
	"time"
)

// WindowDays maps window string to calendar days
var WindowDays = map[string]int{
	"1Y":  365,
	"3Y":  1095,
	"5Y":  1825,
	"10Y": 3650,
}

// toleranceDays — how many days off the target start date we accept
// Real NAV data has weekends/holidays so exact match is rare
const toleranceDays = 30

// NAVPoint represents a single NAV data point
type NAVPoint struct {
	Date  time.Time
	Value float64
}

// RollingStats holds statistical distribution of rolling returns
type RollingStats struct {
	Min    float64 `json:"min"`
	Max    float64 `json:"max"`
	Median float64 `json:"median"`
	P25    float64 `json:"p25"`
	P75    float64 `json:"p75"`
}

// CAGRStats holds CAGR distribution (assignment asks min/max/median only)
type CAGRStats struct {
	Min    float64 `json:"min"`
	Max    float64 `json:"max"`
	Median float64 `json:"median"`
}

// Availability holds data range information
type Availability struct {
	StartDate string `json:"start_date"`
	EndDate   string `json:"end_date"`
	TotalDays int    `json:"total_days"`
	NAVPoints int    `json:"nav_data_points"`
}

// AnalyticsResult holds all computed analytics for one fund + window
type AnalyticsResult struct {
	SchemeCode          string       `json:"fund_code"`
	Window              string       `json:"window"`
	DataAvailability    Availability `json:"data_availability"`
	RollingPeriodsCount int          `json:"rolling_periods_analyzed"`
	RollingReturns      RollingStats `json:"rolling_returns"`
	MaxDrawdown         float64      `json:"max_drawdown"`
	CAGR                CAGRStats    `json:"cagr"`
	ComputedAt          time.Time    `json:"computed_at"`
}

// Compute calculates all analytics for a fund for a given window
// navPoints must be in chronological order (oldest first)
func Compute(schemeCode string, navPoints []NAVPoint, window string) (*AnalyticsResult, error) {
	days, ok := WindowDays[window]
	if !ok {
		return nil, fmt.Errorf("invalid window: %s (must be 1Y/3Y/5Y/10Y)", window)
	}

	if len(navPoints) < 2 {
		return nil, fmt.Errorf("insufficient NAV data: need at least 2 points, got %d", len(navPoints))
	}

	result := &AnalyticsResult{
		SchemeCode: schemeCode,
		Window:     window,
		ComputedAt: time.Now().UTC(),
		DataAvailability: Availability{
			StartDate: navPoints[0].Date.Format("2006-01-02"),
			EndDate:   navPoints[len(navPoints)-1].Date.Format("2006-01-02"),
			TotalDays: int(navPoints[len(navPoints)-1].Date.Sub(navPoints[0].Date).Hours() / 24),
			NAVPoints: len(navPoints),
		},
	}

	rollingReturns, rollingCAGRs, err := computeRollingPeriods(navPoints, days)
	if err != nil {
		return nil, fmt.Errorf("compute rolling periods: %w", err)
	}

	if len(rollingReturns) == 0 {
		return nil, fmt.Errorf("insufficient history for %s window: need %d days, have %d",
			window, days, result.DataAvailability.TotalDays)
	}

	result.RollingPeriodsCount = len(rollingReturns)
	result.RollingReturns = computeRollingStats(rollingReturns)
	result.CAGR = computeCAGRStats(rollingCAGRs)
	result.MaxDrawdown = computeMaxDrawdown(navPoints)

	return result, nil
}

// computeRollingPeriods computes simple returns AND CAGR
// for every possible rolling period of given calendar days
func computeRollingPeriods(navPoints []NAVPoint, days int) ([]float64, []float64, error) {
	var simpleReturns []float64
	var cagrValues []float64

	for i, endPoint := range navPoints {
		// Target: a point approximately `days` calendar days before endPoint
		targetStartDate := endPoint.Date.AddDate(0, 0, -days)

		// Find closest point to targetStartDate within tolerance
		startIdx := findClosestIndex(navPoints, targetStartDate, i)
		if startIdx == -1 {
			continue
		}

		startNAV := navPoints[startIdx].Value
		endNAV := endPoint.Value

		if startNAV <= 0 || endNAV <= 0 {
			continue
		}

		// Simple rolling return: (end - start) / start * 100
		simpleReturn := (endNAV - startNAV) / startNAV * 100

		// CAGR: annualised using actual days between the two points
		actualDays := endPoint.Date.Sub(navPoints[startIdx].Date).Hours() / 24
		actualYears := actualDays / 365.0

		if actualYears <= 0 {
			continue
		}

		cagr := (math.Pow(endNAV/startNAV, 1.0/actualYears) - 1) * 100

		// Sanity checks
		if math.IsNaN(simpleReturn) || math.IsInf(simpleReturn, 0) {
			continue
		}
		if math.IsNaN(cagr) || math.IsInf(cagr, 0) {
			continue
		}
		if cagr < -100 || cagr > 10000 {
			continue
		}

		simpleReturns = append(simpleReturns, simpleReturn)
		cagrValues = append(cagrValues, cagr)
	}

	return simpleReturns, cagrValues, nil
}

// findClosestIndex finds the index of the NAV point closest to targetDate
// Only looks before endIdx
// Returns -1 if no point within toleranceDays is found
func findClosestIndex(navPoints []NAVPoint, targetDate time.Time, endIdx int) int {
	if endIdx == 0 {
		return -1
	}

	// Binary search for the first point >= targetDate
	lo, hi := 0, endIdx-1
	firstGTE := -1

	for lo <= hi {
		mid := (lo + hi) / 2
		if !navPoints[mid].Date.Before(targetDate) {
			firstGTE = mid
			hi = mid - 1
		} else {
			lo = mid + 1
		}
	}

	// Candidates: point just before targetDate and point just on/after targetDate
	// Pick whichever is closest
	bestIdx := -1
	bestDiff := math.MaxFloat64

	// Check point just before targetDate
	if firstGTE > 0 {
		before := firstGTE - 1
		diff := math.Abs(navPoints[before].Date.Sub(targetDate).Hours() / 24)
		if diff < bestDiff {
			bestDiff = diff
			bestIdx = before
		}
	} else if firstGTE == -1 && endIdx > 0 {
		// All points are before targetDate — check the last one
		before := endIdx - 1
		diff := math.Abs(navPoints[before].Date.Sub(targetDate).Hours() / 24)
		if diff < bestDiff {
			bestDiff = diff
			bestIdx = before
		}
	}

	// Check point just on/after targetDate
	if firstGTE != -1 && firstGTE < endIdx {
		diff := math.Abs(navPoints[firstGTE].Date.Sub(targetDate).Hours() / 24)
		if diff < bestDiff {
			bestDiff = diff
			bestIdx = firstGTE
		}
	}

	// Reject if too far from target date
	if bestDiff > float64(toleranceDays) {
		return -1
	}

	return bestIdx
}

// computeMaxDrawdown finds worst peak-to-trough decline
func computeMaxDrawdown(navPoints []NAVPoint) float64 {
	if len(navPoints) < 2 {
		return 0
	}

	maxDrawdown := 0.0
	peak := navPoints[0].Value

	for _, point := range navPoints {
		if point.Value > peak {
			peak = point.Value
		}
		if peak > 0 {
			drawdown := (point.Value - peak) / peak * 100
			if drawdown < maxDrawdown {
				maxDrawdown = drawdown
			}
		}
	}

	return round(maxDrawdown, 2)
}

// computeRollingStats calculates min/max/median/p25/p75
func computeRollingStats(values []float64) RollingStats {
	if len(values) == 0 {
		return RollingStats{}
	}

	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)

	return RollingStats{
		Min:    round(sorted[0], 2),
		Max:    round(sorted[len(sorted)-1], 2),
		Median: round(percentile(sorted, 50), 2),
		P25:    round(percentile(sorted, 25), 2),
		P75:    round(percentile(sorted, 75), 2),
	}
}

// computeCAGRStats calculates min/max/median only
func computeCAGRStats(values []float64) CAGRStats {
	if len(values) == 0 {
		return CAGRStats{}
	}

	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)

	return CAGRStats{
		Min:    round(sorted[0], 2),
		Max:    round(sorted[len(sorted)-1], 2),
		Median: round(percentile(sorted, 50), 2),
	}
}

// percentile calculates p-th percentile using linear interpolation
func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if len(sorted) == 1 {
		return sorted[0]
	}

	rank := (p / 100) * float64(len(sorted)-1)
	lower := int(math.Floor(rank))
	upper := int(math.Ceil(rank))

	if lower == upper {
		return sorted[lower]
	}

	fraction := rank - float64(lower)
	return sorted[lower] + fraction*(sorted[upper]-sorted[lower])
}

// round rounds to n decimal places
func round(val float64, decimals int) float64 {
	factor := math.Pow(10, float64(decimals))
	return math.Round(val*factor) / factor
}
