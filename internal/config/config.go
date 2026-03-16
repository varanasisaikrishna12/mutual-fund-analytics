package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

type Config struct {
	ServerPort string

	DBHost     string
	DBPort     string
	DBUser     string
	DBPassword string
	DBName     string
	DBUrl      string

	RedisHost     string
	RedisPort     string
	RedisPassword string
	RedisAddr     string

	RateLimitPerSecond int
	RateLimitPerMinute int
	RateLimitPerHour   int

	SchemeCodes []string
}

func Load() (*Config, error) {
	_ = godotenv.Load()

	cfg := &Config{
		ServerPort:    getEnv("SERVER_PORT", "8080"),
		DBHost:        getEnv("DB_HOST", "localhost"),
		DBPort:        getEnv("DB_PORT", "5432"),
		DBUser:        getEnv("DB_USER", "mfuser"),
		DBPassword:    getEnv("DB_PASSWORD", "mfpassword"),
		DBName:        getEnv("DB_NAME", "mutualfunds"),
		RedisHost:     getEnv("REDIS_HOST", "localhost"),
		RedisPort:     getEnv("REDIS_PORT", "6379"),
		RedisPassword: getEnv("REDIS_PASSWORD", ""),
	}

	cfg.DBUrl = fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=disable",
		cfg.DBUser, cfg.DBPassword, cfg.DBHost, cfg.DBPort, cfg.DBName,
	)
	cfg.RedisAddr = fmt.Sprintf("%s:%s", cfg.RedisHost, cfg.RedisPort)

	var err error
	cfg.RateLimitPerSecond, err = strconv.Atoi(getEnv("RATE_LIMIT_PER_SECOND", "2"))
	if err != nil {
		return nil, fmt.Errorf("invalid RATE_LIMIT_PER_SECOND: %w", err)
	}
	cfg.RateLimitPerMinute, err = strconv.Atoi(getEnv("RATE_LIMIT_PER_MINUTE", "50"))
	if err != nil {
		return nil, fmt.Errorf("invalid RATE_LIMIT_PER_MINUTE: %w", err)
	}
	cfg.RateLimitPerHour, err = strconv.Atoi(getEnv("RATE_LIMIT_PER_HOUR", "300"))
	if err != nil {
		return nil, fmt.Errorf("invalid RATE_LIMIT_PER_HOUR: %w", err)
	}

	rawCodes := getEnv("SCHEME_CODES", "")
	if rawCodes == "" {
		return nil, fmt.Errorf("SCHEME_CODES is required in .env")
	}
	for _, code := range strings.Split(rawCodes, ",") {
		if code = strings.TrimSpace(code); code != "" {
			cfg.SchemeCodes = append(cfg.SchemeCodes, code)
		}
	}

	return cfg, nil
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
