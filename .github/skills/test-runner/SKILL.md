---
name: test-runner
description: Use when asked to run unit tests, integration tests, set up the Kafka test environment, or troubleshoot test failures.
---

# Running KafkaWorker Tests

## Run All Tests

To run both unit and integration tests (docker must be running):
```powershell
dotnet test KafkaWorker.Tests -c Release
docker compose up -d --wait
dotnet test KafkaWorker.IntegrationTests -c Release -s tests.runsettings -f net10.0
dotnet test KafkaWorker.IntegrationTests -c Release -s tests.runsettings -f net8.0
```

## Unit Tests

Unit tests have no external dependencies and can be run directly:
```powershell
dotnet test KafkaWorker.Tests -c Release
```

## Integration Tests

Integration tests require a live Kafka cluster with Schema Registry. Follow these steps:

### 1. Start Infrastructure
```powershell
docker compose up -d --wait
```
This starts: Zookeeper, Kafka broker (localhost:9092), Schema Registry (localhost:8082), Kafka UI (localhost:888), and an init-kafka container that creates `my-topic` and `my-topic-dlq`.

### 2. Verify Services Are Healthy
```powershell
docker compose ps
```
Ensure `broker`, `schema-registry`, and `zookeeper` show as healthy/running.

If Schema Registry is not ready:
```powershell
curl http://localhost:8082/subjects
```
Should return `[]` when healthy.

### 3. Run Integration Tests

**Important:** This project multi-targets `net8.0` and `net10.0`. You **must** run each TFM separately. Running without `-f` launches both TFMs in parallel, which causes hangs due to Kafka topic and consumer group contention between the two test processes.

```powershell
dotnet test KafkaWorker.IntegrationTests -c Release -s tests.runsettings -f net10.0
dotnet test KafkaWorker.IntegrationTests -c Release -s tests.runsettings -f net8.0
```
The `tests.runsettings` file configures detailed console logging.

To run a specific test:
```powershell
dotnet test KafkaWorker.IntegrationTests -c Release --filter "FullyQualifiedName~JsonConsumerTests" -f net10.0
```

### 4. Tear Down Infrastructure
```powershell
docker compose down -v
```

## Running Tests for a Specific Target Framework
```powershell
dotnet test KafkaWorker.Tests --no-build -c Release -f net10.0
dotnet test KafkaWorker.Tests --no-build -c Release -f net8.0
```

## Troubleshooting

**Tests hang or deadlock when running both TFMs:**
- This happens when `dotnet test` runs `net8.0` and `net10.0` in parallel — both test processes compete for the same Kafka topics and consumer groups.
- **Fix:** Always pass `-f net10.0` or `-f net8.0` to run one TFM at a time.

**Tests timeout waiting for messages:**
- Ensure topics exist: `docker compose exec broker kafka-topics --bootstrap-server broker:9092 --list`
- Restart the init-kafka container: `docker compose restart init-kafka`

**Schema Registry connection refused:**
- Schema Registry maps port 8081 internally to 8082 externally
- Integration tests use `localhost:8082` for Schema Registry

**Port conflicts:**
- Kafka: 9092, Schema Registry: 8082, Kafka UI: 888, Zookeeper: 2181
- Check for other services using these ports
