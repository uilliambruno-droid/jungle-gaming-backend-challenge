import { Injectable } from "@nestjs/common";

@Injectable()
export class MetricsService {
  private readonly counters = new Map<string, number>();

  increment(name: string, value = 1): void { this.counters.set(name, (this.counters.get(name) ?? 0) + value); }
  set(name: string, value: number): void { this.counters.set(name, value); }
  snapshot(): Record<string, number> { return Object.fromEntries([...this.counters.entries()].sort(([a], [b]) => a.localeCompare(b))); }
}
