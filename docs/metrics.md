---
layout: default
title: Metrics
nav_order: 6
---

# Metrics
{: .no_toc }

The library emits [OpenTelemetry-compatible metrics](https://learn.microsoft.com/en-us/dotnet/core/diagnostics/metrics) via `System.Diagnostics.Metrics`. When no listener is attached, metrics are zero-cost no-ops.
{: .fs-6 .fw-300 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Available Metrics

| Instrument | Type | Tags | Description |
|------------|------|------|-------------|
| `kafkaworker.messages.processed` | Counter | `topic`, `status` | Messages processed by the main consumer |
| `kafkaworker.messages.processing_duration` | Histogram (ms) | `topic` | Processing duration per message |
| `kafkaworker.messages.dlq_published` | Counter | `topic`, `dlq_topic`, `reason` | Messages published to the dead letter queue |
| `kafkaworker.dlq.messages_reprocessed` | Counter | `dlq_topic` | Messages successfully reprocessed in place from the DLQ |
| `kafkaworker.dlq.messages_skipped` | Counter | `dlq_topic`, `reason` | Messages skipped during DLQ reprocessing |

### Tag Values

**`status`** (on `messages.processed`):
- `success` — Message processed successfully
- `invalid` — Message rejected via `InvalidMessageException`
- `failed` — Message failed after all retries
- `deserialization_failed` — Message could not be deserialized; skipped and committed past (never reaches the handler or the DLQ)

**`reason`** (on `messages.dlq_published`):
- `processing_failed` — Message failed after all retries in the main consumer
- `invalid` — Message rejected via `InvalidMessageException` in the main consumer
- `reprocess_failed` — Message failed in-place DLQ reprocessing and was re-enqueued to the DLQ

**`reason`** (on `dlq.messages_skipped`):
- `invalid` — Message marked as invalid (will never succeed)
- `max_attempts` — Message exceeded the maximum reprocess attempts
- `deserialization_failed` — DLQ record could not be deserialized; skipped and committed past

---

## Subscribing to Metrics

Add the `KafkaWorker` meter to your OpenTelemetry configuration:

```csharp
builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics =>
    {
        metrics.AddMeter("KafkaWorker");
        metrics.AddPrometheusExporter(); // or any exporter
    });
```

The meter name is `"KafkaWorker"` — this is the only name you need to subscribe to.

---

## Compatible Listeners

Metrics work with any `System.Diagnostics.Metrics`-compatible listener:

- **OpenTelemetry** — Export to Prometheus, Jaeger, OTLP, etc.
- **Azure Monitor** — Via the Azure Monitor OpenTelemetry exporter
- **dotnet-counters** — CLI tool for local debugging
- **Prometheus** — Direct export via `AddPrometheusExporter()`

### Local Debugging with dotnet-counters

```bash
dotnet counters monitor --process-id <PID> --counters KafkaWorker
```

This shows real-time metric values without any exporter configuration.
