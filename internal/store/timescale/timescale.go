package timescale

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/varanasisaikrishna12/mutual-fund-analytics/internal/analytics"
)

type Store struct {
	Pool *pgxpool.Pool
}

type FundRow struct {
	SchemeCode string
	FundName   string
	AMC        string
	Category   string
	CurrentNAV float64
	NAVDate    string
}

type NAVInfo struct {
	NAV  float64
	Date time.Time
}

func New(ctx context.Context, dbURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		return nil, fmt.Errorf("connect timescaledb: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping timescaledb: %w", err)
	}
	return &Store{Pool: pool}, nil
}

func (s *Store) Close() {
	s.Pool.Close()
}

func (s *Store) RunMigrations(ctx context.Context, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read migration: %w", err)
	}
	_, err = s.Pool.Exec(ctx, string(data))
	return err
}

// SeedSchemeCodes inserts scheme codes from config into tracked_schemes
func (s *Store) SeedSchemeCodes(ctx context.Context, codes []string) error {
	for _, code := range codes {
		_, err := s.Pool.Exec(ctx, `
			INSERT INTO tracked_schemes (scheme_code, is_active)
			VALUES ($1, true)
			ON CONFLICT (scheme_code) DO NOTHING
		`, code)
		if err != nil {
			return fmt.Errorf("seed scheme %s: %w", code, err)
		}
	}
	return nil
}

// LoadSchemeCodes returns all active scheme codes
func (s *Store) LoadSchemeCodes(ctx context.Context) ([]string, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT scheme_code FROM tracked_schemes
		WHERE is_active = true
		ORDER BY scheme_code
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var codes []string
	for rows.Next() {
		var code string
		if err := rows.Scan(&code); err != nil {
			continue
		}
		codes = append(codes, code)
	}
	return codes, nil
}

// AddScheme adds a new scheme to tracked_schemes
func (s *Store) AddScheme(ctx context.Context, code string) error {
	_, err := s.Pool.Exec(ctx, `
		INSERT INTO tracked_schemes (scheme_code, is_active)
		VALUES ($1, true)
		ON CONFLICT (scheme_code) DO UPDATE SET is_active = true
	`, code)
	return err
}

// DeactivateScheme marks a scheme as inactive
func (s *Store) DeactivateScheme(ctx context.Context, code string) error {
	_, err := s.Pool.Exec(ctx, `
		UPDATE tracked_schemes SET is_active = false
		WHERE scheme_code = $1
	`, code)
	return err
}

// GetNAVSeries returns full NAV history in chronological order
func (s *Store) GetNAVSeries(ctx context.Context, schemeCode string) ([]analytics.NAVPoint, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT nav_date, nav_value
		FROM nav_data
		WHERE scheme_code = $1
		ORDER BY nav_date ASC
	`, schemeCode)
	if err != nil {
		return nil, fmt.Errorf("query nav series: %w", err)
	}
	defer rows.Close()

	var points []analytics.NAVPoint
	for rows.Next() {
		var p analytics.NAVPoint
		if err := rows.Scan(&p.Date, &p.Value); err != nil {
			return nil, err
		}
		points = append(points, p)
	}
	return points, nil
}

// UpsertAnalytics stores analytics result in analytics_results table
func (s *Store) UpsertAnalytics(ctx context.Context, result *analytics.AnalyticsResult) error {
	data, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal analytics: %w", err)
	}

	_, err = s.Pool.Exec(ctx, `
		INSERT INTO analytics_results (scheme_code, win_window, result_json, computed_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (scheme_code, win_window) DO UPDATE
		SET result_json = EXCLUDED.result_json,
		    computed_at = EXCLUDED.computed_at
	`, result.SchemeCode, result.Window, data, result.ComputedAt)

	return err
}

// GetFund returns fund metadata including latest NAV — single query, fast
func (s *Store) GetFund(ctx context.Context, schemeCode string) (*FundRow, error) {
	var f FundRow
	var navDate *time.Time
	var currentNAV *float64

	err := s.Pool.QueryRow(ctx, `
		SELECT scheme_code, fund_name, amc, category,
		       current_nav, nav_date
		FROM funds WHERE scheme_code = $1
	`, schemeCode).Scan(
		&f.SchemeCode, &f.FundName, &f.AMC, &f.Category,
		&currentNAV, &navDate,
	)
	if err != nil {
		return nil, err
	}

	if currentNAV != nil {
		f.CurrentNAV = *currentNAV
	}
	if navDate != nil {
		f.NAVDate = navDate.Format("2006-01-02")
	}

	return &f, nil
}

// GetAllFunds returns all funds with optional category/amc filters
func (s *Store) GetAllFunds(ctx context.Context, category, amc string) ([]FundRow, error) {
	query := `
		SELECT scheme_code, fund_name, amc, category,
		       COALESCE(current_nav, 0), COALESCE(nav_date::text, '')
		FROM funds WHERE 1=1`
	args := []interface{}{}
	idx := 1

	if category != "" {
		query += fmt.Sprintf(" AND LOWER(category) = LOWER($%d)", idx)
		args = append(args, category)
		idx++
	}
	if amc != "" {
		query += fmt.Sprintf(" AND LOWER(amc) = LOWER($%d)", idx)
		args = append(args, amc)
	}
	query += " ORDER BY scheme_code"

	rows, err := s.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var funds []FundRow
	for rows.Next() {
		var f FundRow
		if err := rows.Scan(
			&f.SchemeCode, &f.FundName, &f.AMC, &f.Category,
			&f.CurrentNAV, &f.NAVDate,
		); err != nil {
			continue
		}
		funds = append(funds, f)
	}
	return funds, nil
}

// GetFundsBySchemes returns fund metadata for multiple scheme codes in one query
func (s *Store) GetFundsBySchemes(ctx context.Context, codes []string) (map[string]*FundRow, error) {
	if len(codes) == 0 {
		return map[string]*FundRow{}, nil
	}

	args := make([]interface{}, len(codes))
	placeholders := ""
	for i, code := range codes {
		args[i] = code
		if i > 0 {
			placeholders += ","
		}
		placeholders += fmt.Sprintf("$%d", i+1)
	}

	rows, err := s.Pool.Query(ctx, fmt.Sprintf(`
		SELECT scheme_code, fund_name, amc, category,
		       COALESCE(current_nav, 0), COALESCE(nav_date::text, '')
		FROM funds WHERE scheme_code IN (%s)
	`, placeholders), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]*FundRow)
	for rows.Next() {
		var f FundRow
		if err := rows.Scan(
			&f.SchemeCode, &f.FundName, &f.AMC, &f.Category,
			&f.CurrentNAV, &f.NAVDate,
		); err != nil {
			continue
		}
		result[f.SchemeCode] = &f
	}
	return result, nil
}

// GetAnalyticsFromDB returns analytics result from DB
func (s *Store) GetAnalyticsFromDB(ctx context.Context, schemeCode, window string) (*analytics.AnalyticsResult, error) {
	var data []byte
	err := s.Pool.QueryRow(ctx, `
		SELECT result_json FROM analytics_results
		WHERE scheme_code = $1 AND win_window = $2
	`, schemeCode, window).Scan(&data)
	if err != nil {
		return nil, err
	}

	var result analytics.AnalyticsResult
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetAllAnalyticsForCategory returns analytics for all schemes in a category in one query
func (s *Store) GetAllAnalyticsForCategory(ctx context.Context, category, window string) (map[string]*analytics.AnalyticsResult, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT a.scheme_code, a.result_json
		FROM analytics_results a
		JOIN funds f ON f.scheme_code = a.scheme_code
		WHERE LOWER(f.category) = LOWER($1)
		AND a.win_window = $2
	`, category, window)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make(map[string]*analytics.AnalyticsResult)
	for rows.Next() {
		var code string
		var data []byte
		if err := rows.Scan(&code, &data); err != nil {
			continue
		}
		var result analytics.AnalyticsResult
		if err := json.Unmarshal(data, &result); err != nil {
			continue
		}
		results[code] = &result
	}
	return results, nil
}

// GetLatestNAVBatch returns latest NAV for multiple schemes in one query
func (s *Store) GetLatestNAVBatch(ctx context.Context, codes []string) (map[string]NAVInfo, error) {
	if len(codes) == 0 {
		return map[string]NAVInfo{}, nil
	}

	args := make([]interface{}, len(codes))
	placeholders := ""
	for i, code := range codes {
		args[i] = code
		if i > 0 {
			placeholders += ","
		}
		placeholders += fmt.Sprintf("$%d", i+1)
	}

	rows, err := s.Pool.Query(ctx, fmt.Sprintf(`
		SELECT scheme_code, COALESCE(current_nav, 0), COALESCE(nav_date, NOW()::date)
		FROM funds
		WHERE scheme_code IN (%s)
	`, placeholders), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]NAVInfo)
	for rows.Next() {
		var code string
		var info NAVInfo
		if err := rows.Scan(&code, &info.NAV, &info.Date); err != nil {
			continue
		}
		result[code] = info
	}
	return result, nil
}

// GetAnalyticsComputedAt returns when analytics were last computed
func (s *Store) GetAnalyticsComputedAt(ctx context.Context, schemeCode, window string) (*time.Time, error) {
	var t time.Time
	err := s.Pool.QueryRow(ctx, `
		SELECT computed_at FROM analytics_results
		WHERE scheme_code = $1 AND win_window = $2
	`, schemeCode, window).Scan(&t)
	if err != nil {
		return nil, err
	}
	return &t, nil
}
