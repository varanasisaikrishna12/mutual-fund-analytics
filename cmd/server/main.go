package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/varanasisaikrishna12/mutual-fund-analytics/internal/config"
	"github.com/varanasisaikrishna12/mutual-fund-analytics/internal/fetcher"
	"github.com/varanasisaikrishna12/mutual-fund-analytics/internal/pipeline"
	"github.com/varanasisaikrishna12/mutual-fund-analytics/internal/ratelimiter"
	redisstore "github.com/varanasisaikrishna12/mutual-fund-analytics/internal/store/redis"
	"github.com/varanasisaikrishna12/mutual-fund-analytics/internal/store/timescale"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.TimeKey = "time"
	encoderCfg.EncodeTime = zapcore.TimeEncoderOfLayout("02-01-2006 15:04:05")

	loggerCfg := zap.NewProductionConfig()
	loggerCfg.EncoderConfig = encoderCfg

	logger, err := loggerCfg.Build()
	if err != nil {
		log.Fatalf("failed to init logger: %v", err)
	}
	defer logger.Sync()

	cfg, err := config.Load()
	if err != nil {
		logger.Fatal("failed to load config", zap.Error(err))
	}
	logger.Info("config loaded",
		zap.String("port", cfg.ServerPort),
		zap.Int("scheme_codes", len(cfg.SchemeCodes)),
	)

	ctx := context.Background()

	tsStore, err := timescale.New(ctx, cfg.DBUrl)
	if err != nil {
		logger.Fatal("failed to connect timescaledb", zap.Error(err))
	}
	defer tsStore.Close()
	logger.Info("timescaledb connected")

	if err := tsStore.RunMigrations(ctx, "migrations/001_init.sql"); err != nil {
		logger.Fatal("migrations failed", zap.Error(err))
	}
	logger.Info("migrations applied")

	rdStore, err := redisstore.New(ctx, cfg.RedisAddr, cfg.RedisPassword)
	if err != nil {
		logger.Fatal("failed to connect redis", zap.Error(err))
	}
	defer rdStore.Close()
	logger.Info("redis connected")

	rl := ratelimiter.New(rdStore.Client, ratelimiter.Config{
		PerSecond: cfg.RateLimitPerSecond,
		PerMinute: cfg.RateLimitPerMinute,
		PerHour:   cfg.RateLimitPerHour,
	}, logger)

	f := fetcher.New(rl, logger)

	if err := tsStore.SeedSchemeCodes(ctx, cfg.SchemeCodes); err != nil {
		logger.Fatal("failed to seed scheme codes", zap.Error(err))
	}
	logger.Info("scheme codes seeded", zap.Int("count", len(cfg.SchemeCodes)))

	schemeCodes, err := tsStore.LoadSchemeCodes(ctx)
	if err != nil {
		logger.Fatal("failed to load scheme codes", zap.Error(err))
	}
	logger.Info("schemes loaded from db", zap.Int("count", len(schemeCodes)))

	pl := pipeline.New(f, tsStore, rdStore, schemeCodes, logger)

	if err := pl.InitSyncState(ctx); err != nil {
		logger.Fatal("failed to init sync state", zap.Error(err))
	}

	if err := pl.Start(ctx); err != nil {
		logger.Warn("pipeline start error", zap.Error(err))
	}

	router := gin.Default()

	// ─────────────────────────────────────────
	// HEALTH
	// ─────────────────────────────────────────
	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status":    "ok",
			"timestamp": time.Now().UTC(),
		})
	})

	// ─────────────────────────────────────────
	// SYNC
	// ─────────────────────────────────────────
	router.POST("/sync/trigger", func(c *gin.Context) {
		runID, err := pl.Trigger(ctx)
		if err != nil {
			c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusAccepted, gin.H{
			"run_id":  runID,
			"status":  "started",
			"message": "sync triggered successfully",
		})
	})

	router.GET("/sync/status", func(c *gin.Context) {
		status, err := pl.GetStatus(ctx)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, status)
	})

	// ─────────────────────────────────────────
	// SCHEMES
	// ─────────────────────────────────────────
	router.POST("/schemes/add", func(c *gin.Context) {
		var body struct {
			SchemeCode string `json:"scheme_code" binding:"required"`
		}
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "scheme_code is required"})
			return
		}

		if err := tsStore.AddScheme(ctx, body.SchemeCode); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		go func() {
			logger.Info("fetching new scheme", zap.String("code", body.SchemeCode))
			if err := pl.ProcessSingleScheme(ctx, body.SchemeCode); err != nil {
				logger.Error("failed to process new scheme",
					zap.String("code", body.SchemeCode),
					zap.Error(err),
				)
			}
		}()

		c.JSON(http.StatusAccepted, gin.H{
			"message":     "scheme added, fetching NAV and computing analytics in background",
			"scheme_code": body.SchemeCode,
			"status":      "processing",
		})
	})

	// ─────────────────────────────────────────
	// FUNDS
	// ─────────────────────────────────────────

	// GET /funds?category=&amc=
	router.GET("/funds", func(c *gin.Context) {
		category := c.Query("category")
		amc := c.Query("amc")

		funds, err := tsStore.GetAllFunds(ctx, category, amc)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"funds": funds,
			"count": len(funds),
		})
	})

	// GET /funds/:code — single fund details + latest NAV from funds table
	router.GET("/funds/:code", func(c *gin.Context) {
		code := c.Param("code")

		fund, err := tsStore.GetFund(ctx, code)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("fund %s not found", code)})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"scheme_code":  fund.SchemeCode,
			"fund_name":    fund.FundName,
			"amc":          fund.AMC,
			"category":     fund.Category,
			"current_nav":  fund.CurrentNAV,
			"last_updated": fund.NAVDate,
		})
	})

	// GET /funds/:code/analytics?window=
	router.GET("/funds/:code/analytics", func(c *gin.Context) {
		code := c.Param("code")
		window := c.Query("window")

		if window == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "window is required (1Y/3Y/5Y/10Y)"})
			return
		}

		// 1. Try Redis first
		result, err := rdStore.GetAnalytics(ctx, code, window)
		if err != nil {
			logger.Error("redis get analytics failed", zap.Error(err))
		}

		// 2. Cache miss → fallback to TimescaleDB
		if result == nil {
			logger.Info("cache miss, reading from db",
				zap.String("scheme", code),
				zap.String("window", window),
			)
			result, err = tsStore.GetAnalyticsFromDB(ctx, code, window)
			if err != nil {
				c.JSON(http.StatusNotFound, gin.H{
					"error": fmt.Sprintf("analytics not found for %s window %s — run /sync/trigger first", code, window),
				})
				return
			}
			// Warm Redis cache
			if err := rdStore.SetAnalytics(ctx, result); err != nil {
				logger.Warn("failed to warm redis cache", zap.Error(err))
			}
		}

		// Enrich with fund metadata
		fund, _ := tsStore.GetFund(ctx, code)

		resp := gin.H{
			"fund_code":                result.SchemeCode,
			"window":                   result.Window,
			"data_availability":        result.DataAvailability,
			"rolling_periods_analyzed": result.RollingPeriodsCount,
			"rolling_returns":          result.RollingReturns,
			"max_drawdown":             result.MaxDrawdown,
			"cagr":                     result.CAGR,
			"computed_at":              result.ComputedAt.Format(time.RFC3339),
		}

		if fund != nil {
			resp["fund_name"] = fund.FundName
			resp["category"] = fund.Category
			resp["amc"] = fund.AMC
		}

		c.JSON(http.StatusOK, resp)
	})

	// GET /funds/rank?category=&window=&sort_by=&limit=
	router.GET("/funds/rank", func(c *gin.Context) {
		category := c.Query("category")
		window := c.Query("window")
		sortBy := c.DefaultQuery("sort_by", "median_return")
		limitStr := c.DefaultQuery("limit", "5")

		if category == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "category is required"})
			return
		}
		if window == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "window is required (1Y/3Y/5Y/10Y)"})
			return
		}
		if sortBy != "median_return" && sortBy != "max_drawdown" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "sort_by must be median_return or max_drawdown"})
			return
		}

		limit := 5
		fmt.Sscanf(limitStr, "%d", &limit)
		if limit <= 0 || limit > 20 {
			limit = 5
		}

		// 1. Try Redis — pre-computed at sync time
		ranking, err := rdStore.GetRanking(ctx, category, window, sortBy)
		if err != nil {
			logger.Error("redis get ranking failed", zap.Error(err))
		}

		// 2. Cache miss — compute from DB and warm Redis
		if ranking == nil {
			logger.Info("ranking cache miss, computing from db",
				zap.String("category", category),
				zap.String("window", window),
				zap.String("sort_by", sortBy),
			)

			funds, err := tsStore.GetAllFunds(ctx, category, "")
			if err != nil || len(funds) == 0 {
				c.JSON(http.StatusNotFound, gin.H{
					"error": fmt.Sprintf("no funds found for category=%s", category),
				})
				return
			}

			analyticsMap, _ := tsStore.GetAllAnalyticsForCategory(ctx, category, window)

			var entries []redisstore.RankEntry
			for _, fund := range funds {
				result, ok := analyticsMap[fund.SchemeCode]
				if !ok {
					continue
				}
				entries = append(entries, redisstore.RankEntry{
					FundCode:     fund.SchemeCode,
					FundName:     fund.FundName,
					AMC:          fund.AMC,
					MedianReturn: result.RollingReturns.Median,
					MaxDrawdown:  result.MaxDrawdown,
					CurrentNAV:   fund.CurrentNAV,
					LastUpdated:  fund.NAVDate,
				})
			}

			if sortBy == "median_return" {
				sort.Slice(entries, func(i, j int) bool {
					return entries[i].MedianReturn > entries[j].MedianReturn
				})
			} else {
				sort.Slice(entries, func(i, j int) bool {
					return entries[i].MaxDrawdown > entries[j].MaxDrawdown
				})
			}

			for i := range entries {
				entries[i].Rank = i + 1
			}

			ranking = &redisstore.RankingResult{
				Category:   category,
				Window:     window,
				SortBy:     sortBy,
				TotalFunds: len(entries),
				ComputedAt: time.Now().UTC(),
				Funds:      entries,
			}

			if err := rdStore.SetRanking(ctx, ranking); err != nil {
				logger.Warn("failed to cache ranking", zap.Error(err))
			}
		}

		// Apply limit
		funds := ranking.Funds
		if limit > len(funds) {
			limit = len(funds)
		}
		funds = funds[:limit]

		c.JSON(http.StatusOK, gin.H{
			"category":    ranking.Category,
			"window":      ranking.Window,
			"sorted_by":   ranking.SortBy,
			"total_funds": ranking.TotalFunds,
			"showing":     len(funds),
			"computed_at": ranking.ComputedAt,
			"funds":       funds,
		})
	})

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%s", cfg.ServerPort),
		Handler: router,
	}

	go func() {
		logger.Info("server starting", zap.String("port", cfg.ServerPort))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("server error", zap.Error(err))
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("shutting down...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Fatal("shutdown failed", zap.Error(err))
	}
	logger.Info("server stopped cleanly")
}
