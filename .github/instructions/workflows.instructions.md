---
applyTo: ".github/workflows/*.yml"
---

# GitHub Actions Workflow Instructions

## Project CI/CD Overview
This project has two workflows:
- **ci.yml** — Runs on push/PR to `main`. Builds, runs unit tests, starts Kafka via docker-compose, runs integration tests, uploads test artifacts.
- **publish.yml** — Runs on `v*` tags. Builds, tests, packs NuGet packages, and pushes to nuget.org.

## Multi-targeting
This project targets both `net8.0` and `net10.0`. Both SDK versions must be installed in workflows via `actions/setup-dotnet@v4`.

## Key Rules
- Integration tests must run one TFM at a time (`--framework net10.0`) to avoid Kafka topic contention between parallel test processes
- Always run `docker compose down -v` in an `if: always()` step to clean up Kafka infrastructure
- NuGet package version is derived from the git tag (`v1.2.3` → `1.2.3`)
- Use `/p:ContinuousIntegrationBuild=true` for deterministic builds
- Secrets: `NUGET_API_KEY` is required for publish workflow

## Conventions
- Use `actions/checkout@v4`, `actions/setup-dotnet@v4`, `actions/upload-artifact@v4`
- Set `DOTNET_NOLOGO: true` and `DOTNET_CLI_TELEMETRY_OPTOUT: true` as env vars
- Test results are uploaded as `.trx` artifacts
