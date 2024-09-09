package redis

import (
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	PoolHitsTotal        = "pool_hits_total"
	PoolMissesTotal      = "pool_misses_total"
	PoolTimeoutTotal     = "pool_timeout_total"
	PoolConnTotalCurrent = "pool_conn_total_current"
	PoolConnIdleCurrent  = "pool_conn_idle_current"
	PoolConnStaleTotal   = "pool_conn_stale_total"

	meterRequestTotal               = "request_total"
	meterRequestLatencyMicroseconds = "latency_microseconds"
	meterRequestDurationSeconds     = "request_duration_seconds"
)

type Statser interface {
	PoolStats() *redis.PoolStats
}

func (b *Broker) statsMeter() {
	go func() {
		ticker := time.NewTicker(DefaultMeterStatsInterval)
		defer ticker.Stop()

		for {
			select {
			case <-b.done:
				return
			case <-ticker.C:
				if b.cli == nil {
					return
				}
				stats := b.cli.PoolStats()
				b.opts.Meter.Counter(PoolHitsTotal).Set(uint64(stats.Hits))
				b.opts.Meter.Counter(PoolMissesTotal).Set(uint64(stats.Misses))
				b.opts.Meter.Counter(PoolTimeoutTotal).Set(uint64(stats.Timeouts))
				b.opts.Meter.Counter(PoolConnTotalCurrent).Set(uint64(stats.TotalConns))
				b.opts.Meter.Counter(PoolConnIdleCurrent).Set(uint64(stats.IdleConns))
				b.opts.Meter.Counter(PoolConnStaleTotal).Set(uint64(stats.StaleConns))
			}
		}
	}()
}
