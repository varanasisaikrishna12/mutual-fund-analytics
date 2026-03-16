# test.ps1 - Run all tests for Mutual Fund Analytics
# Usage: .\scripts\test.ps1
#        .\scripts\test.ps1 -Suite ratelimiter
#        .\scripts\test.ps1 -Suite all -ApiServer

param(
    [string]$Suite = "all",
    [switch]$ApiServer
)

$ErrorActionPreference = "Continue"
$passed = 0
$failed = 0

function Run-Suite {
    param([string]$Name, [string]$Path)

    Write-Host ""
    Write-Host "-------------------------------------" -ForegroundColor DarkGray
    Write-Host " Running: $Name" -ForegroundColor Cyan
    Write-Host "-------------------------------------" -ForegroundColor DarkGray

    go clean -testcache
    go test $Path -v -timeout 120s 2>&1

    if ($LASTEXITCODE -eq 0) {
        Write-Host " PASSED: $Name" -ForegroundColor Green
        $script:passed++
    } else {
        Write-Host " FAILED: $Name" -ForegroundColor Red
        $script:failed++
    }
}

Write-Host ""
Write-Host "=== Mutual Fund Analytics - Test Runner ===" -ForegroundColor Cyan
Write-Host ""

$redisCheck = docker inspect mf_redis --format="{{.State.Health.Status}}" 2>&1
if ($redisCheck -ne "healthy") {
    Write-Host "WARNING: Redis container not healthy. Starting containers..." -ForegroundColor Yellow
    docker compose up -d 2>&1 | Out-Null
    Start-Sleep -Seconds 5
}

switch ($Suite) {
    "ratelimiter" {
        Run-Suite "Rate Limiter" "./internal/ratelimiter/..."
    }
    "analytics" {
        Run-Suite "Analytics Engine" "./internal/analytics/..."
    }
    "fetcher" {
        Run-Suite "Fetcher" "./internal/fetcher/..."
    }
    "api" {
        if (-not $ApiServer) {
            Write-Host "NOTE: API tests require the server to be running." -ForegroundColor Yellow
            Write-Host "      Start server: go run cmd/server/main.go" -ForegroundColor Yellow
            Write-Host "      Then run: .\scripts\test.ps1 -Suite api -ApiServer" -ForegroundColor Yellow
            exit 0
        }
        Run-Suite "API" "./internal/api/..."
    }
    "all" {
        Run-Suite "Rate Limiter"     "./internal/ratelimiter/..."
        Run-Suite "Analytics Engine" "./internal/analytics/..."
        Run-Suite "Fetcher"          "./internal/fetcher/..."

        if ($ApiServer) {
            Run-Suite "API" "./internal/api/..."
        } else {
            Write-Host ""
            Write-Host "NOTE: Skipping API tests (requires running server)." -ForegroundColor Yellow
            Write-Host "      To include: .\scripts\test.ps1 -Suite all -ApiServer" -ForegroundColor Yellow
        }
    }
    default {
        Write-Host "Unknown suite: $Suite" -ForegroundColor Red
        Write-Host "Valid: all | ratelimiter | analytics | fetcher | api" -ForegroundColor Yellow
        exit 1
    }
}

Write-Host ""
Write-Host "=====================================" -ForegroundColor DarkGray
Write-Host " Test Summary" -ForegroundColor Cyan
Write-Host "=====================================" -ForegroundColor DarkGray
Write-Host " Passed: $passed" -ForegroundColor Green

$failColor = "Green"
if ($failed -gt 0) { $failColor = "Red" }
Write-Host " Failed: $failed" -ForegroundColor $failColor
Write-Host ""

if ($failed -gt 0) { exit 1 } else { exit 0 }