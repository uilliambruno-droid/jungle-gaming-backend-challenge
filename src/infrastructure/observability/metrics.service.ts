import { Injectable } from "@nestjs/common";

@Injectable()
export class MetricsService {
  private readonly counters = new Map<string, number>([
    ["transactions_processed_total", 0],
    ["transactions_rejected_total", 0],
    ["transactions_pending_reference_total", 0],
    ["transactions_failed_total", 0],
    ["idempotent_replays_total", 0],
    ["inbox_duplicates_total", 0],
    ["inbox_payload_conflicts_total", 0],
    ["sqs_processed_total", 0],
    ["sqs_conflicts_total", 0],
    ["sqs_dlq_candidates_total", 0],
    ["sqs_dlq_messages", 0],
    ["sqs_retries_total", 0],
    ["sqs_receive_errors_total", 0],
    ["lock_conflicts_total", 0],
    ["pending_reference_retries_total", 0],
    ["pending_reference_processed_total", 0],
    ["pending_reference_expired_total", 0],
    ["outbox_publish_attempts_total", 0],
    ["outbox_publish_latency_ms_count", 0],
    ["outbox_publish_latency_ms_sum", 0],
    ["outbox_publish_latency_ms_max", 0],
    ["outbox_retries_total", 0],
    ["outbox_published_total", 0],
    ["outbox_worker_errors_total", 0],
    ["outbox_pending_total", 0],
    ["outbox_lag_seconds", 0],
    ["reconciliation_mismatches_total", 0],
    ["wager_submission_latency_ms_count", 0],
    ["wager_submission_latency_ms_sum", 0],
    ["wager_submission_latency_ms_max", 0],
    ["pending_reference_processing_latency_ms_count", 0],
    ["pending_reference_processing_latency_ms_sum", 0],
    ["pending_reference_processing_latency_ms_max", 0],
  ]);

  increment(name: string, value = 1): void {
    this.counters.set(name, (this.counters.get(name) ?? 0) + value);
  }

  set(name: string, value: number): void {
    this.counters.set(name, value);
  }

  observe(name: string, value: number): void {
    this.increment(`${name}_count`);
    this.increment(`${name}_sum`, value);
    this.set(`${name}_max`, Math.max(this.counters.get(`${name}_max`) ?? 0, value));
  }

  snapshot(): Record<string, number> {
    return Object.fromEntries(
      [...this.counters.entries()].sort(([a], [b]) => a.localeCompare(b)),
    );
  }
}
