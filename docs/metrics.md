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
| `kafkaworker.messages.dlq_published` | Counter | `topic`, `dlq_topic` | Messages published to the dead letter queue |
| `kafkaworker.dlq.messages_reprocessed` | Counter | `topic`, `dlq_topic` | Messages reprocessed from the DLQ back to the original topic |
| `kafkaworker.dlq.messages_skipped` | Counter | `topic`, `reason` | Messages skipped during DLQ reprocessing |

### Tag Values

**`status`** (on `messages.processed`):
- `success` — Message processed successfully
- `invalid` — Message rejected via `InvalidMessageException`
- `failed` — Message failed after all retries

**`reason`** (on `dlq.messages_skipped`):
- `invalid` — Message marked as invalid (will never succeed)
- `max_attempts` — Message exceeded the maximum reprocess attempts
- `missing_topic` — Original topic header is missing from the DLQ message

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
