# ─────────────────────────────────────────────────────────────
# start.ps1 — Start Mutual Fund Analytics Service
# Usage: .\scripts\start.ps1
# ─────────────────────────────────────────────────────────────

Write-Host ""
Write-Host "=== Mutual Fund Analytics ===" -ForegroundColor Cyan
Write-Host ""

# Check Docker is running
Write-Host "[1/4] Checking Docker..." -ForegroundColor Yellow
$dockerStatus = docker info 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Docker is not running. Start Docker Desktop first." -ForegroundColor Red
    exit 1
}
Write-Host "      Docker OK" -ForegroundColor Green

# Start containers
Write-Host "[2/4] Starting containers (TimescaleDB + Redis)..." -ForegroundColor Yellow
docker compose up -d 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: docker compose up failed" -ForegroundColor Red
    exit 1
}

# Wait for containers to be healthy
Write-Host "      Waiting for containers to be healthy..." -ForegroundColor Yellow
$maxWait = 30
$waited = 0
while ($waited -lt $maxWait) {
    $dbStatus = docker inspect mf_timescaledb --format='{{.State.Health.Status}}' 2>&1
    $rdStatus = docker inspect mf_redis --format='{{.State.Health.Status}}' 2>&1
    if ($dbStatus -eq "healthy" -and $rdStatus -eq "healthy") {
        break
    }
    Start-Sleep -Seconds 2
    $waited += 2
}

if ($waited -ge $maxWait) {
    Write-Host "ERROR: Containers did not become healthy in time" -ForegroundColor Red
    Write-Host "Run: docker compose ps" -ForegroundColor Yellow
    exit 1
}
Write-Host "      Containers healthy" -ForegroundColor Green

# Build
Write-Host "[3/4] Building..." -ForegroundColor Yellow
go build ./... 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Build failed" -ForegroundColor Red
    exit 1
}
Write-Host "      Build OK" -ForegroundColor Green

# Start server
Write-Host "[4/4] Starting server on :8080..." -ForegroundColor Yellow
Write-Host ""
Write-Host "      Server will auto-sync all 10 schemes on first start." -ForegroundColor Gray
Write-Host "      Watch for 'all rankings pre-computed' in logs." -ForegroundColor Gray
Write-Host ""
Write-Host "  API Endpoints:" -ForegroundColor Cyan
Write-Host "  GET  http://localhost:8080/health"
Write-Host "  GET  http://localhost:8080/funds"
Write-Host "  GET  http://localhost:8080/funds/:code"
Write-Host "  GET  http://localhost:8080/funds/:code/analytics?window=3Y"
Write-Host "  GET  http://localhost:8080/funds/rank?category=Equity Scheme - Mid Cap Fund&window=3Y&sort_by=median_return"
Write-Host "  POST http://localhost:8080/sync/trigger"
Write-Host "  GET  http://localhost:8080/sync/status"
Write-Host ""

go run cmd/server/main.go