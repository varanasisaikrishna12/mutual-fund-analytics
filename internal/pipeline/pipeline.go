package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/varanasisaikrishna12/mutual-fund-analytics/internal/analytics"
	"github.com/varanasisaikrishna12/mutual-fund-analytics/internal/fetcher"
	redisstore "github.com/varanasisaikrishna12/mutual-fund-analytics/internal/store/redis"
	"github.com/varanasisaikrishna12/mutual-fund-analytics/internal/store/timescale"
	"go.uber.org/zap"
)

const (
	syncQueueKey      = "sync:queue"
	syncInProgressKey = "sync:in_progress"
	syncStatusKey     = "sync:status"
	syncRunIDKey      = "sync:run_id"
)

// All windows we compute analytics for
var analyticsWindows = []string{"1Y", "3Y", "5Y", "10Y"}

type SyncStatus struct {
	RunID          string     `json:"run_id"`
	Status         string     `json:"status"`
	Phase          string     `json:"phase"`
	QueueRemaining int        `json:"queue_remaining"`
	CurrentScheme  string     `json:"current_scheme"`
	StartedAt      time.Time  `json:"started_at"`
	CompletedAt    *time.Time `json:"completed_at,omitempty"`
	LastError      string     `json:"last_error,omitempty"`
}

type Pipeline struct {
	fetcher     *fetcher.Fetcher
	tsStore     *timescale.Store
	rdStore     *redisstore.Store
	schemeCodes []string
	logger      *zap.Logger
}

func New(
	f *fetcher.Fetcher,
	ts *timescale.Store,
	rd *redisstore.Store,
	schemeCodes []string,
	logger *zap.Logger,
) *Pipeline {
	return &Pipeline{
		fetcher:     f,
		tsStore:     ts,
		rdStore:     rd,
		schemeCodes: schemeCodes,
		logger:      logger,
	}
}

// Start checks for interrupted runs on startup and resumes if needed
func (p *Pipeline) Start(ctx context.Context) error {
	p.logger.Info("pipeline starting, checking for interrupted runs")

	inProgress, err := p.rdStore.Client.Get(ctx, syncInProgressKey).Result()
	if err == nil && inProgress != "" {
		p.logger.Warn("found interrupted sync, re-queuing scheme",
			zap.String("scheme", inProgress),
		)
		p.rdStore.Client.LPush(ctx, syncQueueKey, inProgress)
		p.rdStore.Client.Del(ctx, syncInProgressKey)
	}

	incomplete, err := p.getIncompleteSchemes(ctx)
	if err != nil {
		return fmt.Errorf("get incomplete schemes: %w", err)
	}

	if len(incomplete) > 0 {
		p.logger.Info("resuming incomplete backfill",
			zap.Strings("schemes", incomplete),
		)
		p.Trigger(ctx)
	}

	return nil
}

// Trigger starts a new sync run
func (p *Pipeline) Trigger(ctx context.Context) (string, error) {
	status, _ := p.GetStatus(ctx)
	if status != nil && status.Status == "running" {
		return "", fmt.Errorf("sync already in progress")
	}

	runID := fmt.Sprintf("run_%d", time.Now().UnixNano())

	for _, code := range p.schemeCodes {
		p.rdStore.Client.RPush(ctx, syncQueueKey, code)
	}

	p.setStatus(ctx, &SyncStatus{
		RunID:     runID,
		Status:    "running",
		Phase:     "fetching",
		StartedAt: time.Now().UTC(),
	})

	p.rdStore.Client.Set(ctx, syncRunIDKey, runID, 0)

	go p.run(ctx, runID)

	p.logger.Info("sync triggered", zap.String("run_id", runID))
	return runID, nil
}

// run is the main pipeline loop — sequential processing
func (p *Pipeline) run(ctx context.Context, runID string) {
	p.logger.Info("pipeline run started", zap.String("run_id", runID))

	// Channel to pass scheme codes to analytics worker
	analyticsQueue := make(chan string, len(p.schemeCodes))

	// Analytics worker runs concurrently with fetching
	var analyticsWg sync.WaitGroup
	analyticsWg.Add(1)
	go func() {
		defer analyticsWg.Done()
		p.analyticsWorker(ctx, analyticsQueue)
	}()

	// Collect all scheme codes from Redis queue
	var schemes []string
	for {
		result, err := p.rdStore.Client.LPop(ctx, syncQueueKey).Result()
		if err != nil {
			break
		}
		schemes = append(schemes, result)
	}

	p.logger.Info("schemes to process", zap.Int("count", len(schemes)))

	processed := 0
	failed := 0

	// Sequential processing — rate limiter is the bottleneck anyway
	for _, schemeCode := range schemes {
		p.setStatus(ctx, &SyncStatus{
			RunID:         runID,
			Status:        "running",
			Phase:         "fetching",
			CurrentScheme: schemeCode,
			StartedAt:     time.Now().UTC(),
		})

		p.logger.Info("processing scheme",
			zap.String("scheme", schemeCode),
			zap.Int("queue_remaining", len(schemes)-processed-1),
		)

		if err := p.processScheme(ctx, schemeCode); err != nil {
			p.logger.Error("failed to process scheme",
				zap.String("scheme", schemeCode),
				zap.Error(err),
			)
			p.updateSyncStateError(ctx, schemeCode, err)
			failed++
			continue
		}

		// Signal analytics worker
		analyticsQueue <- schemeCode
		processed++
	}

	// Close analytics queue and wait for it to finish
	close(analyticsQueue)
	analyticsWg.Wait()

	p.logger.Info("pipeline fetch phase complete",
		zap.Int("processed", processed),
		zap.Int("failed", failed),
	)
}

// analyticsWorker receives scheme codes and computes analytics
// Runs concurrently while pipeline fetches next scheme
func (p *Pipeline) analyticsWorker(ctx context.Context, queue <-chan string) {
	for schemeCode := range queue {
		p.logger.Info("computing analytics", zap.String("scheme", schemeCode))

		if err := p.computeAndStoreAnalytics(ctx, schemeCode); err != nil {
			p.logger.Error("analytics failed",
				zap.String("scheme", schemeCode),
				zap.Error(err),
			)
			continue
		}

		p.logger.Info("analytics complete", zap.String("scheme", schemeCode))
	}

	now := time.Now().UTC()
	p.setStatus(ctx, &SyncStatus{
		Status:      "completed",
		Phase:       "done",
		CompletedAt: &now,
	})
	p.logger.Info("pipeline completed")
}

// computeAndStoreAnalytics runs the full analytics pipeline for one scheme:
// 1. Pull NAV history from TimescaleDB
// 2. Compute analytics for all 4 windows
// 3. Store in TimescaleDB
// 4. Push to Redis
// 5. Update ranking sorted sets
func (p *Pipeline) computeAndStoreAnalytics(ctx context.Context, schemeCode string) error {
	// 1. Pull full NAV history from TimescaleDB
	navPoints, err := p.tsStore.GetNAVSeries(ctx, schemeCode)
	if err != nil {
		return fmt.Errorf("get nav series: %w", err)
	}
	if len(navPoints) < 2 {
		return fmt.Errorf("insufficient NAV data for %s: %d points", schemeCode, len(navPoints))
	}

	p.logger.Debug("nav series loaded",
		zap.String("scheme", schemeCode),
		zap.Int("points", len(navPoints)),
	)

	// Get fund category for ranking
	fund, err := p.tsStore.GetFund(ctx, schemeCode)
	if err != nil {
		p.logger.Warn("could not get fund category, skipping ranking",
			zap.String("scheme", schemeCode),
			zap.Error(err),
		)
	}

	// 2. Compute analytics for all 4 windows
	for _, window := range analyticsWindows {
		result, err := analytics.Compute(schemeCode, navPoints, window)
		if err != nil {
			// Insufficient history for this window — not an error, just skip
			p.logger.Debug("skipping window (insufficient history)",
				zap.String("scheme", schemeCode),
				zap.String("window", window),
				zap.Error(err),
			)
			continue
		}

		// 3. Store in TimescaleDB
		if err := p.tsStore.UpsertAnalytics(ctx, result); err != nil {
			p.logger.Error("failed to upsert analytics to db",
				zap.String("scheme", schemeCode),
				zap.String("window", window),
				zap.Error(err),
			)
			continue
		}

		// 4. Push to Redis (25hr TTL)
		if err := p.rdStore.SetAnalytics(ctx, result); err != nil {
			p.logger.Error("failed to cache analytics in redis",
				zap.String("scheme", schemeCode),
				zap.String("window", window),
				zap.Error(err),
			)
			// Non-fatal — DB has the data, Redis is just cache
		}

		// 5. Update ranking sorted set (only if we have fund category)
		if fund != nil && fund.Category != "" {
			if err := p.rdStore.UpdateRanking(ctx,
				fund.Category,
				window,
				schemeCode,
				result.CAGR.Median,
			); err != nil {
				p.logger.Error("failed to update ranking",
					zap.String("scheme", schemeCode),
					zap.String("window", window),
					zap.Error(err),
				)
				// Non-fatal
			}
		}

		p.logger.Debug("analytics stored",
			zap.String("scheme", schemeCode),
			zap.String("window", window),
			zap.Int("periods", result.RollingPeriodsCount),
			zap.Float64("cagr_median", result.CAGR.Median),
		)
	}

	return nil
}

// processScheme fetches NAV data for one scheme and stores it
func (p *Pipeline) processScheme(ctx context.Context, schemeCode string) error {
	lastDate, err := p.getLastNAVDate(ctx, schemeCode)
	if err != nil {
		p.logger.Warn("no existing data, doing full backfill",
			zap.String("scheme", schemeCode),
		)
	}

	fund, navPoints, err := p.fetcher.FetchScheme(ctx, schemeCode)
	if err != nil {
		return fmt.Errorf("fetch scheme: %w", err)
	}

	if err := p.upsertFund(ctx, fund); err != nil {
		return fmt.Errorf("upsert fund: %w", err)
	}

	newPoints := navPoints
	if lastDate != nil {
		newPoints = filterNewPoints(navPoints, *lastDate)
		p.logger.Info("incremental sync",
			zap.String("scheme", schemeCode),
			zap.String("last_date", lastDate.Format("2006-01-02")),
			zap.Int("new_points", len(newPoints)),
		)
	}

	if len(newPoints) == 0 {
		p.logger.Info("no new NAV data", zap.String("scheme", schemeCode))
		return nil
	}

	if err := p.batchInsertNAV(ctx, newPoints); err != nil {
		return fmt.Errorf("insert nav: %w", err)
	}

	latestDate := newPoints[len(newPoints)-1].Date
	if err := p.updateSyncState(ctx, schemeCode, latestDate); err != nil {
		return fmt.Errorf("update sync state: %w", err)
	}

	p.logger.Info("scheme processed",
		zap.String("scheme", schemeCode),
		zap.Int("points_inserted", len(newPoints)),
		zap.String("latest_date", latestDate.Format("2006-01-02")),
	)

	return nil
}

// GetStatus returns current sync status
func (p *Pipeline) GetStatus(ctx context.Context) (*SyncStatus, error) {
	data, err := p.rdStore.Client.Get(ctx, syncStatusKey).Result()
	if err != nil {
		return &SyncStatus{Status: "idle"}, nil
	}

	var status SyncStatus
	if err := json.Unmarshal([]byte(data), &status); err != nil {
		return nil, fmt.Errorf("unmarshal status: %w", err)
	}
	return &status, nil
}

func (p *Pipeline) setStatus(ctx context.Context, status *SyncStatus) {
	data, _ := json.Marshal(status)
	p.rdStore.Client.Set(ctx, syncStatusKey, data, 0)
}

// --- DB helpers ---

func (p *Pipeline) upsertFund(ctx context.Context, fund *fetcher.FundInfo) error {
	_, err := p.tsStore.Pool.Exec(ctx, `
		INSERT INTO funds (scheme_code, fund_name, amc, category, inception_date, updated_at)
		VALUES ($1, $2, $3, $4, $5, NOW())
		ON CONFLICT (scheme_code) DO UPDATE
		SET fund_name      = EXCLUDED.fund_name,
		    amc            = EXCLUDED.amc,
		    category       = EXCLUDED.category,
		    inception_date = COALESCE(funds.inception_date, EXCLUDED.inception_date),
		    updated_at     = NOW()
	`, fund.SchemeCode, fund.FundName, fund.AMC, fund.Category, fund.InceptionDate)
	return err
}

func (p *Pipeline) batchInsertNAV(ctx context.Context, points []fetcher.NAVPoint) error {
	rows := make([][]interface{}, len(points))
	for i, pt := range points {
		rows[i] = []interface{}{pt.SchemeCode, pt.Date, pt.NAV}
	}

	_, err := p.tsStore.Pool.CopyFrom(
		ctx,
		pgx.Identifier{"nav_data"},
		[]string{"scheme_code", "nav_date", "nav_value"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return p.upsertNAVBatch(ctx, points)
	}
	return nil
}

func (p *Pipeline) upsertNAVBatch(ctx context.Context, points []fetcher.NAVPoint) error {
	for _, point := range points {
		_, err := p.tsStore.Pool.Exec(ctx, `
			INSERT INTO nav_data (scheme_code, nav_date, nav_value)
			VALUES ($1, $2, $3)
			ON CONFLICT (scheme_code, nav_date) DO UPDATE
			SET nav_value = EXCLUDED.nav_value
		`, point.SchemeCode, point.Date, point.NAV)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Pipeline) getLastNAVDate(ctx context.Context, schemeCode string) (*time.Time, error) {
	var lastDate time.Time
	err := p.tsStore.Pool.QueryRow(ctx, `
		SELECT last_nav_date FROM sync_state
		WHERE scheme_code = $1
	`, schemeCode).Scan(&lastDate)
	if err != nil {
		return nil, err
	}
	return &lastDate, nil
}

func (p *Pipeline) updateSyncState(ctx context.Context, schemeCode string, lastDate time.Time) error {
	_, err := p.tsStore.Pool.Exec(ctx, `
		INSERT INTO sync_state (scheme_code, backfill_status, last_nav_date, last_synced_at)
		VALUES ($1, 'complete', $2, NOW())
		ON CONFLICT (scheme_code) DO UPDATE
		SET backfill_status = 'complete',
		    last_nav_date   = EXCLUDED.last_nav_date,
		    last_synced_at  = NOW(),
		    error_count     = 0,
		    last_error      = NULL,
		    updated_at      = NOW()
	`, schemeCode, lastDate)
	return err
}

func (p *Pipeline) updateSyncStateError(ctx context.Context, schemeCode string, syncErr error) {
	p.tsStore.Pool.Exec(ctx, `
		INSERT INTO sync_state (scheme_code, backfill_status, error_count, last_error)
		VALUES ($1, 'error', 1, $2)
		ON CONFLICT (scheme_code) DO UPDATE
		SET backfill_status = 'error',
		    error_count     = sync_state.error_count + 1,
		    last_error      = EXCLUDED.last_error,
		    updated_at      = NOW()
	`, schemeCode, syncErr.Error())
}

func (p *Pipeline) getIncompleteSchemes(ctx context.Context) ([]string, error) {
	rows, err := p.tsStore.Pool.Query(ctx, `
		SELECT scheme_code FROM sync_state
		WHERE backfill_status IN ('pending', 'in_progress', 'error')
		AND error_count < 3
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

func filterNewPoints(points []fetcher.NAVPoint, lastDate time.Time) []fetcher.NAVPoint {
	var filtered []fetcher.NAVPoint
	for _, p := range points {
		if p.Date.After(lastDate) {
			filtered = append(filtered, p)
		}
	}
	return filtered
}

// InitSyncState inserts pending sync state for all schemes
func (p *Pipeline) InitSyncState(ctx context.Context) error {
	for _, code := range p.schemeCodes {
		_, err := p.tsStore.Pool.Exec(ctx, `
			INSERT INTO sync_state (scheme_code, backfill_status)
			VALUES ($1, 'pending')
			ON CONFLICT (scheme_code) DO NOTHING
		`, code)
		if err != nil {
			return fmt.Errorf("init sync state for %s: %w", code, err)
		}
	}
	return nil
}

// ProcessSingleScheme fetches NAV and computes analytics for one scheme
func (p *Pipeline) ProcessSingleScheme(ctx context.Context, schemeCode string) error {
	p.logger.Info("processing single scheme", zap.String("scheme", schemeCode))

	_, err := p.tsStore.Pool.Exec(ctx, `
		INSERT INTO sync_state (scheme_code, backfill_status)
		VALUES ($1, 'pending')
		ON CONFLICT (scheme_code) DO NOTHING
	`, schemeCode)
	if err != nil {
		return fmt.Errorf("init sync state: %w", err)
	}

	if err := p.processScheme(ctx, schemeCode); err != nil {
		return fmt.Errorf("process scheme: %w", err)
	}

	if err := p.computeAndStoreAnalytics(ctx, schemeCode); err != nil {
		return fmt.Errorf("compute analytics: %w", err)
	}

	p.logger.Info("single scheme processing complete",
		zap.String("scheme", schemeCode),
	)
	return nil
}
